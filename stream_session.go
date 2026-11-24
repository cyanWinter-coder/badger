/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/dgraph-io/ristretto/v2/z"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/trie"
	"github.com/dgraph-io/badger/v4/y"
)

// defaultStreamRetainedRecords bounds how many published records the stream
// session manager keeps for replay. Records beyond the bound are dropped,
// oldest first; a session whose cursor falls behind the retained floor gets
// ErrStreamCursorExpired on its next attach.
const defaultStreamRetainedRecords = 1 << 14

// StreamCursor is the durable position of a resumable stream session.
type StreamCursor struct {
	// Version is the highest write version the consumer has acknowledged.
	Version uint64
	// Generation is the last attached generation of the session. It is
	// bumped and persisted on every attach so that acks issued against an
	// older generation can never move the cursor of a newer one.
	Generation uint64
}

// StreamCursorStore persists stream session cursors. Implementations must be
// safe for concurrent use.
type StreamCursorStore interface {
	// LoadCursor returns the stored cursor for sessionID, or the zero
	// StreamCursor if the session has never been seen.
	LoadCursor(sessionID string) (StreamCursor, error)
	// SaveCursor durably stores the cursor for sessionID.
	SaveCursor(sessionID string, cursor StreamCursor) error
	// DeleteCursor removes any stored cursor for sessionID.
	DeleteCursor(sessionID string) error
}

// MemoryStreamCursorStore keeps cursors in memory. Cursors survive session
// reconnects but not process restarts.
type MemoryStreamCursorStore struct {
	mu      sync.Mutex
	cursors map[string]StreamCursor
}

// NewMemoryStreamCursorStore returns an empty in-memory cursor store.
func NewMemoryStreamCursorStore() *MemoryStreamCursorStore {
	return &MemoryStreamCursorStore{cursors: make(map[string]StreamCursor)}
}

// LoadCursor implements StreamCursorStore.
func (s *MemoryStreamCursorStore) LoadCursor(sessionID string) (StreamCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursors[sessionID], nil
}

// SaveCursor implements StreamCursorStore.
func (s *MemoryStreamCursorStore) SaveCursor(sessionID string, cursor StreamCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[sessionID] = cursor
	return nil
}

// DeleteCursor implements StreamCursorStore.
func (s *MemoryStreamCursorStore) DeleteCursor(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cursors, sessionID)
	return nil
}

// FileStreamCursorStore persists cursors in a JSON file. Writes go to a
// temporary file which is fsynced and atomically renamed over the target, so
// a crash cannot leave a partially written cursor file behind.
type FileStreamCursorStore struct {
	path    string
	mu      sync.Mutex
	loaded  bool
	cursors map[string]StreamCursor
}

// NewFileStreamCursorStore returns a cursor store backed by the file at path.
// The file is created lazily on the first save.
func NewFileStreamCursorStore(path string) *FileStreamCursorStore {
	return &FileStreamCursorStore{path: path, cursors: make(map[string]StreamCursor)}
}

func (s *FileStreamCursorStore) loadLocked() error {
	if s.loaded {
		return nil
	}
	s.loaded = true
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return y.Wrapf(err, "while reading stream cursor file %s", s.path)
	}
	if err := json.Unmarshal(data, &s.cursors); err != nil {
		return y.Wrapf(err, "while parsing stream cursor file %s", s.path)
	}
	return nil
}

func (s *FileStreamCursorStore) persistLocked() error {
	data, err := json.Marshal(s.cursors)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return y.Wrapf(err, "while creating stream cursor file %s", tmp)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	// Persist the rename itself.
	return syncDir(filepath.Dir(s.path))
}

// LoadCursor implements StreamCursorStore.
func (s *FileStreamCursorStore) LoadCursor(sessionID string) (StreamCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return StreamCursor{}, err
	}
	return s.cursors[sessionID], nil
}

// SaveCursor implements StreamCursorStore.
func (s *FileStreamCursorStore) SaveCursor(sessionID string, cursor StreamCursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	s.cursors[sessionID] = cursor
	return s.persistLocked()
}

// DeleteCursor implements StreamCursorStore.
func (s *FileStreamCursorStore) DeleteCursor(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	delete(s.cursors, sessionID)
	return s.persistLocked()
}

// streamRecord is a single published change kept for replay.
type streamRecord struct {
	version uint64
	kv      *pb.KV
}

// streamSessionState is the dispatcher-owned state of one attached session
// generation. All fields are guarded by streamSessionManager.mu; cond is
// bound to the same mutex.
type streamSessionState struct {
	id        string
	gen       uint64
	cursor    uint64 // acked watermark, persisted on every advance
	delivered uint64 // highest write version ever queued for this generation
	matches   *trie.Trie
	queue     []*pb.KVList
	cond      *sync.Cond
	closed    bool
}

// streamSessionManager multiplexes the DB change stream across any number of
// named, resumable sessions. All sessions share a single publisher
// subscription (the "shared reader"): detaching one session never disturbs
// the delivery to the others, and the shared reader keeps running while
// sessions reconnect so that disconnect gaps remain replayable.
type streamSessionManager struct {
	db    *DB
	store StreamCursorStore

	// maxRetained bounds len(retained). Not guarded by mu; set before the
	// first session attaches (tests may shrink it).
	maxRetained int

	mu       sync.Mutex
	closed   bool
	sessions map[string]*streamSessionState
	retained []streamRecord // stable-sorted by write version

	// The shared publisher subscription. Started lazily on the first
	// attach, runs until the manager is closed.
	subActive bool
	sub       subscriber
	subStop   chan struct{}
	subDone   chan struct{}
}

func newStreamSessionManager(db *DB) *streamSessionManager {
	store := db.opt.StreamCursorStore
	if store == nil {
		store = NewMemoryStreamCursorStore()
	}
	return &streamSessionManager{
		db:          db,
		store:       store,
		maxRetained: defaultStreamRetainedRecords,
		sessions:    make(map[string]*streamSessionState),
	}
}

// attach opens a new generation of the named session. The persisted cursor
// decides where replay starts; the persisted generation is bumped and saved
// before the session becomes visible, so a crash can never resurrect a
// generation that stale acks might still reference.
func (m *streamSessionManager) attach(id string, matches []pb.Match) (*StreamSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrDBClosed
	}
	if _, ok := m.sessions[id]; ok {
		return nil, ErrStreamSessionActive
	}
	cur, err := m.store.LoadCursor(id)
	if err != nil {
		return nil, y.Wrapf(err, "while loading stream cursor for session %q", id)
	}
	gen := cur.Generation + 1
	if err := m.store.SaveCursor(id, StreamCursor{Version: cur.Version, Generation: gen}); err != nil {
		return nil, y.Wrapf(err, "while persisting stream cursor for session %q", id)
	}

	if len(m.retained) > 0 && cur.Version > 0 && cur.Version < m.retained[0].version {
		// Records in (cursor, retained floor] were evicted before anyone
		// acked them; replaying would silently skip changes.
		return nil, ErrStreamCursorExpired
	}

	tr := trie.NewTrie()
	for _, match := range matches {
		if err := tr.AddMatch(match, 1); err != nil {
			return nil, err
		}
	}
	st := &streamSessionState{
		id:      id,
		gen:     gen,
		cursor:  cur.Version,
		matches: tr,
	}
	st.cond = sync.NewCond(&m.mu)

	// Replay the retained, still unacknowledged range first. Records at or
	// below the persisted cursor were already acknowledged and are never
	// delivered again.
	var replay []*pb.KV
	delivered := cur.Version
	for _, rec := range m.retained {
		if rec.version > delivered {
			delivered = rec.version
		}
		if rec.version <= cur.Version {
			continue
		}
		if len(tr.Get(rec.kv.Key)) > 0 {
			replay = append(replay, rec.kv)
		}
	}
	if len(replay) > 0 {
		st.queue = append(st.queue, &pb.KVList{Kv: replay})
	}
	st.delivered = delivered

	if err := m.ensureSharedReaderLocked(); err != nil {
		return nil, err
	}
	m.sessions[id] = st
	return &StreamSession{mgr: m, id: id, gen: gen, st: st}, nil
}

// ack advances the session cursor to version, but only if gen is the
// session's current generation: acks from older generations are rejected and
// acks at or below the current cursor are treated as duplicates and ignored.
func (m *streamSessionManager) ack(id string, gen uint64, version uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.sessions[id]
	if !ok || st.gen != gen {
		return ErrStreamStaleAck
	}
	if version <= st.cursor {
		// Duplicate or late ack; the cursor is already past it.
		return nil
	}
	if err := m.store.SaveCursor(id, StreamCursor{Version: version, Generation: gen}); err != nil {
		return y.Wrapf(err, "while persisting stream cursor for session %q", id)
	}
	st.cursor = version
	return nil
}

// detach removes exactly this session generation. The shared reader and all
// other sessions are left running.
func (m *streamSessionManager) detach(id string, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.sessions[id]
	if !ok || st.gen != gen {
		return
	}
	st.closed = true
	st.cond.Broadcast()
	delete(m.sessions, id)
}

// dispatch stable-sorts the batch by write version, appends it to the
// retained log and queues the matching, not-yet-delivered records for every
// attached session. It never blocks on session consumption.
func (m *streamSessionManager) dispatch(batch *pb.KVList) {
	if len(batch.GetKv()) == 0 {
		return
	}
	sort.SliceStable(batch.Kv, func(i, j int) bool {
		return batch.Kv[i].Version < batch.Kv[j].Version
	})
	lastVersion := batch.Kv[len(batch.Kv)-1].Version

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, kv := range batch.Kv {
		m.retained = append(m.retained, streamRecord{version: kv.Version, kv: kv})
	}
	if overflow := len(m.retained) - m.maxRetained; overflow > 0 {
		// Copy into a fresh slice so the evicted prefix's backing array can
		// be reclaimed.
		m.retained = append([]streamRecord(nil), m.retained[overflow:]...)
	}
	for _, st := range m.sessions {
		if st.delivered >= lastVersion {
			continue
		}
		var out []*pb.KV
		for _, kv := range batch.Kv {
			if kv.Version <= st.delivered {
				continue
			}
			if len(st.matches.Get(kv.Key)) == 0 {
				continue
			}
			out = append(out, kv)
		}
		st.delivered = lastVersion
		if len(out) > 0 {
			st.queue = append(st.queue, &pb.KVList{Kv: out})
			st.cond.Signal()
		}
	}
}

// ensureSharedReaderLocked starts the single publisher subscription shared by
// all stream sessions, if it isn't running yet. Caller holds m.mu.
func (m *streamSessionManager) ensureSharedReaderLocked() error {
	if m.subActive {
		return nil
	}
	c := z.NewCloser(1)
	// An empty prefix matches every key; per-session match filtering happens
	// in dispatch, so sessions can attach and detach freely without ever
	// touching the shared subscription.
	sub, err := m.db.pub.newSubscriber(c, []pb.Match{{Prefix: []byte{}}})
	if err != nil {
		c.Done()
		return y.Wrapf(err, "while creating the shared stream subscriber")
	}
	m.sub = sub
	m.subStop = make(chan struct{})
	m.subDone = make(chan struct{})
	m.subActive = true
	go m.consumeShared(sub, c, m.subStop, m.subDone)
	return nil
}

// consumeShared is the shared reader loop. It mirrors DB.Subscribe's drain
// behavior so a blocked publisher send can always complete.
func (m *streamSessionManager) consumeShared(sub subscriber, c *z.Closer, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	defer c.Done()
	slurp := func(batch *pb.KVList) *pb.KVList {
		for {
			select {
			case kvs := <-sub.sendCh:
				batch.Kv = append(batch.Kv, kvs.Kv...)
			default:
				return batch
			}
		}
	}
	for {
		select {
		case <-stop:
			return
		case <-c.HasBeenClosed():
			// The DB is closing underneath us; deliver whatever is left.
			m.dispatch(slurp(new(pb.KVList)))
			return
		case batch := <-sub.sendCh:
			m.dispatch(slurp(batch))
		}
	}
}

// close detaches every session and stops the shared reader. Pending
// unacknowledged records stay replayable: cursors only move via Ack.
func (m *streamSessionManager) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for id, st := range m.sessions {
		st.closed = true
		st.cond.Broadcast()
		delete(m.sessions, id)
	}
	if !m.subActive {
		m.mu.Unlock()
		return
	}
	m.subActive = false
	sub := m.sub
	subStop := m.subStop
	subDone := m.subDone
	m.mu.Unlock()

	// Mirror DB.Subscribe's teardown: mark inactive so the publisher stops
	// sending, stop the consumer, drain anything in flight so a publisher
	// send blocked on the full channel can complete, then remove only our
	// own subscription. consumeShared releases the subscriber closer.
	sub.active.Store(0)
	close(subStop)
	<-subDone
	for {
		select {
		case <-sub.sendCh:
		default:
			m.db.pub.deleteSubscriber(sub.id)
			return
		}
	}
}

// StreamSession is one generation of a named, resumable change-stream
// subscription. Create it with DB.NewStreamSession, consume it with Serve,
// and acknowledge processed records with Ack. A session is single-use:
// once Serve returns (or Close is called), reconnect by opening a new
// session with the same session ID.
type StreamSession struct {
	mgr *streamSessionManager
	id  string
	gen uint64
	st  *streamSessionState

	closeOnce sync.Once
}

// Generation returns the generation of this session attachment. Every
// reconnect of the same session ID produces a new, higher generation.
func (s *StreamSession) Generation() uint64 {
	return s.gen
}

// Cursor returns the highest acknowledged write version of this session.
func (s *StreamSession) Cursor() uint64 {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	return s.st.cursor
}

// Ack records that every record up to and including version has been
// durably processed. The ack is bound to this session's generation: it is
// rejected with ErrStreamStaleAck if a newer generation has since attached,
// and it is ignored (returning nil) if the cursor has already moved past
// version, so duplicate and late acks can never move the cursor backwards
// or resurrect already-acked records for redelivery.
func (s *StreamSession) Ack(version uint64) error {
	return s.mgr.ack(s.id, s.gen, version)
}

// Close detaches this session generation. It only removes this session: the
// shared reader and any other consumers keep running, and the session can be
// resumed later with DB.NewStreamSession. Close is idempotent.
func (s *StreamSession) Close() {
	s.closeOnce.Do(func() {
		s.mgr.detach(s.id, s.gen)
	})
}

// Serve delivers records to cb, replaying the retained unacknowledged range
// first and then streaming live changes in stable write-version order. It
// blocks until ctx is done, cb returns an error, or the session/DB is
// closed, and it always detaches the session before returning.
func (s *StreamSession) Serve(ctx context.Context, cb func(kv *KVList) error) error {
	if cb == nil {
		return ErrNilCallback
	}
	s.mgr.mu.Lock()
	if s.st.closed {
		s.mgr.mu.Unlock()
		return ErrStreamSessionClosed
	}
	s.mgr.mu.Unlock()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.Close()
		case <-stop:
		}
	}()
	// Disconnecting detaches this generation so the session ID can be
	// reopened; unacknowledged records will be replayed to the next one.
	defer s.Close()

	for {
		s.mgr.mu.Lock()
		for len(s.st.queue) == 0 && !s.st.closed {
			s.st.cond.Wait()
		}
		if s.st.closed {
			s.mgr.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}
		batch := s.st.queue[0]
		s.st.queue = s.st.queue[1:]
		s.mgr.mu.Unlock()

		if err := cb(batch); err != nil {
			return err
		}
	}
}

// NewStreamSession opens (or resumes) a named, resumable change-stream
// session over the given key matches.
//
// Delivery starts from the session's persisted cursor: retained records
// newer than the last acknowledged version are replayed first, followed by
// live changes, all in stable write-version order. Records at or below the
// cursor are never delivered again. Use StreamSession.Ack to advance the
// cursor; acks are bound to the session generation, so acks issued by a
// previous, disconnected generation cannot advance the new cursor.
//
// Multiple sessions share a single underlying change-stream reader; closing
// one session never interrupts the others. Only one generation of a session
// ID may be active at a time: reopening an ID that is still attached fails
// with ErrStreamSessionActive.
func (db *DB) NewStreamSession(sessionID string, matches []pb.Match) (*StreamSession, error) {
	if sessionID == "" {
		return nil, y.Wrapf(ErrInvalidRequest, "stream session ID cannot be empty")
	}
	if db.IsClosed() {
		return nil, ErrDBClosed
	}
	return db.streamMgr.attach(sessionID, matches)
}
