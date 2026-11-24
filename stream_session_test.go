/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/trie"
)

// streamCollector collects every KV delivered to a stream session callback.
type streamCollector struct {
	mu  sync.Mutex
	kvs []*pb.KV
}

func (c *streamCollector) cb(kvs *KVList) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kvs = append(c.kvs, kvs.GetKv()...)
	return nil
}

func (c *streamCollector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.kvs)
}

func (c *streamCollector) snapshot() []*pb.KV {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*pb.KV(nil), c.kvs...)
}

func writeStreamKeys(t *testing.T, db *DB, prefix string, start, end int) {
	t.Helper()
	for i := start; i < end; i++ {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry(
				[]byte(fmt.Sprintf("%s%d", prefix, i)),
				[]byte(fmt.Sprintf("value%d", i))))
		}))
	}
}

// retainedLen reports how many records the manager has retained for replay.
func retainedLen(db *DB) int {
	db.streamMgr.mu.Lock()
	defer db.streamMgr.mu.Unlock()
	return len(db.streamMgr.retained)
}

func waitForRetained(t *testing.T, db *DB, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return retainedLen(db) >= n
	}, 10*time.Second, time.Millisecond, "timed out waiting for %d retained records", n)
}

func waitForDelivered(t *testing.T, c *streamCollector, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return c.len() >= n
	}, 10*time.Second, time.Millisecond, "timed out waiting for %d delivered records", n)
}

// Reconnecting a session must replay exactly the unacknowledged range:
// acknowledged records are not delivered again, and records written while
// the session was disconnected are replayed before live ones.
func TestStreamSessionReplayAfterReconnect(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		matches := []pb.Match{{Prefix: []byte("ke")}}

		sess1, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)
		require.Equal(t, uint64(1), sess1.Generation())
		require.Equal(t, uint64(0), sess1.Cursor())

		col1 := &streamCollector{}
		ctx1, cancel1 := context.WithCancel(context.Background())
		serve1 := make(chan error, 1)
		go func() { serve1 <- sess1.Serve(ctx1, col1.cb) }()

		writeStreamKeys(t, db, "key", 0, 3)
		waitForDelivered(t, col1, 3)

		got1 := col1.snapshot()
		require.Len(t, got1, 3)
		// The active stream is delivered in stable write-version order.
		for i := 1; i < len(got1); i++ {
			require.Greater(t, got1[i].Version, got1[i-1].Version)
		}
		// Acknowledge the first two records only.
		require.NoError(t, sess1.Ack(got1[1].Version))
		require.Equal(t, got1[1].Version, sess1.Cursor())

		// Disconnect, then write more while nobody is listening for s1.
		cancel1()
		require.ErrorIs(t, <-serve1, context.Canceled)
		writeStreamKeys(t, db, "key", 3, 5)
		waitForRetained(t, db, 5)

		// Reconnect: a new generation resumes from the persisted cursor.
		sess2, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)
		require.Equal(t, uint64(2), sess2.Generation())
		require.Equal(t, got1[1].Version, sess2.Cursor())

		col2 := &streamCollector{}
		ctx2, cancel2 := context.WithCancel(context.Background())
		serve2 := make(chan error, 1)
		go func() { serve2 <- sess2.Serve(ctx2, col2.cb) }()

		// The replay covers exactly the unacked range: key2..key4.
		waitForDelivered(t, col2, 3)
		got2 := col2.snapshot()
		require.Len(t, got2, 3)
		require.Equal(t, "key2", string(got2[0].Key))
		require.Equal(t, "key3", string(got2[1].Key))
		require.Equal(t, "key4", string(got2[2].Key))
		for _, kv := range got2 {
			require.Greater(t, kv.Version, got1[1].Version,
				"acknowledged record was redelivered")
		}

		// Live writes keep flowing after the replay.
		writeStreamKeys(t, db, "key", 5, 6)
		waitForDelivered(t, col2, 4)
		require.Equal(t, "key5", string(col2.snapshot()[3].Key))

		cancel2()
		require.ErrorIs(t, <-serve2, context.Canceled)
	})
}

// Acks are bound to the session generation: an ack from a previous
// generation (a late ack arriving after reconnect) must not advance the new
// generation's cursor, and duplicate acks must be no-ops.
func TestStreamSessionStaleAndLateAcks(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		matches := []pb.Match{{Prefix: []byte("ke")}}

		sess1, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)

		col1 := &streamCollector{}
		ctx1, cancel1 := context.WithCancel(context.Background())
		serve1 := make(chan error, 1)
		go func() { serve1 <- sess1.Serve(ctx1, col1.cb) }()

		writeStreamKeys(t, db, "key", 0, 2)
		waitForDelivered(t, col1, 2)
		v0 := col1.snapshot()[0].Version
		require.NoError(t, sess1.Ack(v0))
		require.Equal(t, v0, sess1.Cursor())

		// Disconnect and reconnect: generation 2 becomes active.
		cancel1()
		require.ErrorIs(t, <-serve1, context.Canceled)
		sess2, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)
		require.Equal(t, uint64(2), sess2.Generation())
		require.Equal(t, v0, sess2.Cursor())

		// A late ack from the old generation is rejected and must not move
		// the new generation's cursor.
		require.ErrorIs(t, sess1.Ack(v0+1000), ErrStreamStaleAck)
		require.Equal(t, v0, sess2.Cursor())

		// A duplicate ack of an already-acked version is a no-op.
		require.NoError(t, sess2.Ack(v0))
		require.Equal(t, v0, sess2.Cursor())

		// The new generation can still advance the cursor.
		writeStreamKeys(t, db, "key", 2, 3)
		col2 := &streamCollector{}
		ctx2, cancel2 := context.WithCancel(context.Background())
		serve2 := make(chan error, 1)
		go func() { serve2 <- sess2.Serve(ctx2, col2.cb) }()
		waitForDelivered(t, col2, 2) // replay of key1 plus live key2
		vNew := col2.snapshot()[1].Version
		require.NoError(t, sess2.Ack(vNew))
		require.Equal(t, vNew, sess2.Cursor())

		// A late duplicate ack (already below the cursor) stays a no-op even
		// on the active generation.
		require.NoError(t, sess2.Ack(v0))
		require.Equal(t, vNew, sess2.Cursor())

		cancel2()
		require.ErrorIs(t, <-serve2, context.Canceled)
	})
}

// Closing one consumer must not disturb the others: the shared reader keeps
// streaming to the remaining sessions.
func TestStreamSessionUnsubscribeKeepsSharedReader(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		matches := []pb.Match{{Prefix: []byte("ke")}}

		sessA, err := db.NewStreamSession("consumer-a", matches)
		require.NoError(t, err)
		sessB, err := db.NewStreamSession("consumer-b", matches)
		require.NoError(t, err)

		colA, colB := &streamCollector{}, &streamCollector{}
		ctxA, cancelA := context.WithCancel(context.Background())
		ctxB, cancelB := context.WithCancel(context.Background())
		serveA := make(chan error, 1)
		serveB := make(chan error, 1)
		go func() { serveA <- sessA.Serve(ctxA, colA.cb) }()
		go func() { serveB <- sessB.Serve(ctxB, colB.cb) }()

		writeStreamKeys(t, db, "key", 0, 1)
		waitForDelivered(t, colA, 1)
		waitForDelivered(t, colB, 1)

		// Unsubscribe A only; B must keep receiving updates.
		cancelA()
		require.ErrorIs(t, <-serveA, context.Canceled)

		writeStreamKeys(t, db, "key", 1, 3)
		waitForDelivered(t, colB, 3)
		gotB := colB.snapshot()
		require.Equal(t, "key1", string(gotB[1].Key))
		require.Equal(t, "key2", string(gotB[2].Key))
		require.Equal(t, 1, colA.len(), "detached session kept receiving updates")

		cancelB()
		require.ErrorIs(t, <-serveB, context.Canceled)
	})
}

// Only one generation of a session ID may be active at a time; after the
// active one disconnects, a duplicate reconnect resumes from the cursor.
func TestStreamSessionDuplicateReconnect(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		matches := []pb.Match{{Prefix: []byte("ke")}}

		sess1, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)

		// A duplicate attach while the first generation is active fails.
		_, err = db.NewStreamSession("s1", matches)
		require.ErrorIs(t, err, ErrStreamSessionActive)

		// After closing, the same ID can be reopened with a new generation.
		sess1.Close()
		sess2, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)
		require.Equal(t, sess1.Generation()+1, sess2.Generation())

		// A closed session cannot be served again.
		require.ErrorIs(t, sess1.Serve(context.Background(), func(*KVList) error {
			return nil
		}), ErrStreamSessionClosed)
		sess2.Close()
	})
}

// The cursor survives restarts of the manager when a persistent cursor
// store is configured.
func TestStreamSessionCursorStorePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursors.json")

	store := NewFileStreamCursorStore(path)
	require.NoError(t, store.SaveCursor("s1", StreamCursor{Version: 42, Generation: 3}))
	require.NoError(t, store.SaveCursor("s2", StreamCursor{Version: 7, Generation: 1}))

	// A fresh store over the same file sees the persisted cursors.
	reopened := NewFileStreamCursorStore(path)
	cur, err := reopened.LoadCursor("s1")
	require.NoError(t, err)
	require.Equal(t, StreamCursor{Version: 42, Generation: 3}, cur)
	cur, err = reopened.LoadCursor("s2")
	require.NoError(t, err)
	require.Equal(t, StreamCursor{Version: 7, Generation: 1}, cur)

	// Unknown sessions start at the zero cursor; deletes persist too.
	cur, err = reopened.LoadCursor("unknown")
	require.NoError(t, err)
	require.Equal(t, StreamCursor{}, cur)
	require.NoError(t, reopened.DeleteCursor("s1"))
	reopened2 := NewFileStreamCursorStore(path)
	cur, err = reopened2.LoadCursor("s1")
	require.NoError(t, err)
	require.Equal(t, StreamCursor{}, cur)
}

// A session whose persisted cursor fell behind the retained log floor is
// rejected instead of silently skipping unacknowledged records.
func TestStreamSessionCursorExpired(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		db.streamMgr.maxRetained = 2
		matches := []pb.Match{{Prefix: []byte("ke")}}

		sess1, err := db.NewStreamSession("s1", matches)
		require.NoError(t, err)
		col1 := &streamCollector{}
		ctx1, cancel1 := context.WithCancel(context.Background())
		serve1 := make(chan error, 1)
		go func() { serve1 <- sess1.Serve(ctx1, col1.cb) }()

		// Overflow the retained log; only the last 2 records survive.
		writeStreamKeys(t, db, "key", 0, 5)
		waitForDelivered(t, col1, 5)
		require.Eventually(t, func() bool {
			return retainedLen(db) == 2
		}, 10*time.Second, time.Millisecond)

		// Ack only the first record, then disconnect.
		v0 := col1.snapshot()[0].Version
		require.NoError(t, sess1.Ack(v0))
		cancel1()
		require.ErrorIs(t, <-serve1, context.Canceled)

		// The unacked range (v0, ...] starts before the retained floor, so
		// resuming must fail loudly rather than skip records.
		_, err = db.NewStreamSession("s1", matches)
		require.ErrorIs(t, err, ErrStreamCursorExpired)
	})
}

// dispatch must stable-sort each batch by write version so every session
// observes a version-ordered stream even when commits interleave.
func TestStreamSessionDispatchSortsByVersion(t *testing.T) {
	m := &streamSessionManager{
		maxRetained: 16,
		sessions:    make(map[string]*streamSessionState),
	}
	tr := trie.NewTrie()
	require.NoError(t, tr.AddMatch(pb.Match{Prefix: []byte{}}, 1))
	st := &streamSessionState{id: "s", gen: 1, matches: tr}
	st.cond = sync.NewCond(&m.mu)
	m.sessions["s"] = st

	mk := func(version uint64, key string) *pb.KV {
		return &pb.KV{Key: []byte(key), Version: version}
	}
	// Versions arrive out of order within a single batch.
	m.dispatch(&pb.KVList{Kv: []*pb.KV{mk(9, "c"), mk(3, "a"), mk(6, "b")}})

	require.Len(t, st.queue, 1)
	batch := st.queue[0]
	require.Equal(t, []uint64{3, 6, 9},
		[]uint64{batch.Kv[0].Version, batch.Kv[1].Version, batch.Kv[2].Version})
	require.Equal(t, uint64(9), st.delivered)

	// The retained log is version-ordered as well.
	require.Len(t, m.retained, 3)
	require.Equal(t, uint64(3), m.retained[0].version)
	require.Equal(t, uint64(9), m.retained[2].version)

	// A record at or below the delivered watermark is not delivered again.
	m.dispatch(&pb.KVList{Kv: []*pb.KV{mk(6, "dup"), mk(12, "d")}})
	require.Len(t, st.queue, 2)
	require.Len(t, st.queue[1].Kv, 1)
	require.Equal(t, uint64(12), st.queue[1].Kv[0].Version)
}

// Closing the DB with sessions attached must release the shared reader and
// let Serve return instead of hanging.
func TestStreamSessionDBClose(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)

	sess, err := db.NewStreamSession("s1", []pb.Match{{Prefix: []byte("ke")}})
	require.NoError(t, err)
	col := &streamCollector{}
	serve := make(chan error, 1)
	go func() { serve <- sess.Serve(context.Background(), col.cb) }()

	writeStreamKeys(t, db, "key", 0, 2)
	waitForDelivered(t, col, 2)

	require.NoError(t, db.Close())
	select {
	case err := <-serve:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after DB close")
	}
}
