/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// Backup format constants. A backup produced by BackupWithManifest is framed
// as:
//
//	magic ("BDBK0001")
//	[len uint64][header manifest JSON]          -- format version, required features
//	([len uint64][crc32 uint32][KVList proto])* -- data shards, each checksummed
//	magic ("BDBKEND1")
//	[len uint64][trailer manifest JSON]         -- shard declarations, validated at restore
//
// Backups in the legacy format (a bare sequence of [len][KVList] records, as
// written by Stream.Backup) contain no manifest and are still accepted by
// Restore for backward compatibility.
const (
	backupHeaderMagic   = "BDBK0001"
	backupTrailerMagic  = "BDBKEND1"
	backupFormatVersion = 1

	maxBackupHeaderSize   = 1 << 20
	maxBackupManifestSize = 1 << 30
	maxBackupRecordSize   = 2 << 30
)

// supportedRequiredFeatures lists the required-feature identifiers this build
// understands. Format version 1 defines none; any feature a backup declares
// that is not listed here aborts the restore before any data is written.
var supportedRequiredFeatures = map[string]struct{}{}

// backupShard describes one length-prefixed KVList record in the backup
// stream. The restore validates every declared shard against what it read.
type backupShard struct {
	Index  int    `json:"index"`
	Bytes  uint64 `json:"bytes"`
	NumKVs uint64 `json:"num_kvs"`
	CRC32  uint32 `json:"crc32"`
}

// backupManifest describes a backup file. The header copy carries only the
// format version and required features so an incompatible backup is rejected
// before anything is written; the trailer copy additionally declares every
// shard, the key/value counts and the source table and value-log files.
type backupManifest struct {
	FormatVersion    int           `json:"format_version"`
	RequiredFeatures []string      `json:"required_features,omitempty"`
	CreatedAt        int64         `json:"created_at"`
	NumRecords       uint64        `json:"num_records,omitempty"`
	NumKVs           uint64        `json:"num_kvs,omitempty"`
	MaxVersion       uint64        `json:"max_version,omitempty"`
	NumTables        int           `json:"num_tables,omitempty"`
	NumVlogFiles     int           `json:"num_vlog_files,omitempty"`
	Shards           []backupShard `json:"shards,omitempty"`
}

// manifestBackupWriter frames KVList records with per-shard checksums and
// emits the header/trailer manifests around them.
type manifestBackupWriter struct {
	w        io.Writer
	manifest backupManifest
}

func newManifestBackupWriter(w io.Writer) *manifestBackupWriter {
	return &manifestBackupWriter{
		w: w,
		manifest: backupManifest{
			FormatVersion: backupFormatVersion,
			CreatedAt:     time.Now().Unix(),
		},
	}
}

func writeJSONBlob(w io.Writer, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(data))); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (m *manifestBackupWriter) writeHeader() error {
	if _, err := io.WriteString(m.w, backupHeaderMagic); err != nil {
		return err
	}
	// The header carries only what a restore needs to decide up front whether
	// it can read this backup at all.
	return writeJSONBlob(m.w, backupManifest{
		FormatVersion:    m.manifest.FormatVersion,
		RequiredFeatures: m.manifest.RequiredFeatures,
		CreatedAt:        m.manifest.CreatedAt,
	})
}

func (m *manifestBackupWriter) writeRecord(list *pb.KVList) error {
	payload, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	crc := crc32.Checksum(payload, y.CastagnoliCrcTable)
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[:8], uint64(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:], crc)
	if _, err := m.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := m.w.Write(payload); err != nil {
		return err
	}
	m.manifest.Shards = append(m.manifest.Shards, backupShard{
		Index:  len(m.manifest.Shards),
		Bytes:  uint64(len(payload)),
		NumKVs: uint64(len(list.Kv)),
		CRC32:  crc,
	})
	m.manifest.NumKVs += uint64(len(list.Kv))
	return nil
}

func (m *manifestBackupWriter) finish(maxVersion uint64, numTables, numVlogFiles int) error {
	m.manifest.MaxVersion = maxVersion
	m.manifest.NumRecords = uint64(len(m.manifest.Shards))
	m.manifest.NumTables = numTables
	m.manifest.NumVlogFiles = numVlogFiles
	if _, err := io.WriteString(m.w, backupTrailerMagic); err != nil {
		return err
	}
	return writeJSONBlob(m.w, m.manifest)
}

// BackupWithManifest is like DB.Backup but additionally frames the output
// with a manifest: a header declaring the format version and required
// features, per-shard checksums, and a trailer manifest describing every
// shard plus the source table and value-log counts. Restore validates all of
// it before making the restored data visible.
func (db *DB) BackupWithManifest(w io.Writer, since uint64) (uint64, error) {
	stream := db.NewStream()
	stream.LogPrefix = "DB.BackupWithManifest"
	stream.SinceTs = since
	return stream.BackupWithManifest(w, since)
}

// BackupWithManifest is like Stream.Backup but emits the manifest-based
// backup format described at the top of restore.go.
func (stream *Stream) BackupWithManifest(w io.Writer, since uint64) (uint64, error) {
	stream.KeyToList = backupKeyToListFunc(stream, since)

	mw := newManifestBackupWriter(w)
	if err := mw.writeHeader(); err != nil {
		return 0, err
	}

	var maxVersion uint64
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		if err != nil {
			return err
		}
		out := list.Kv[:0]
		for _, kv := range list.Kv {
			if maxVersion < kv.Version {
				maxVersion = kv.Version
			}
			if !kv.StreamDone {
				// Don't pick stream done changes.
				out = append(out, kv)
			}
		}
		list.Kv = out
		return mw.writeRecord(list)
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		return 0, err
	}

	// Descriptive source statistics: how many tables and value-log files the
	// backup was built from. Informational only.
	numTables, numVlogFiles := 0, 0
	if stream.db != nil {
		for _, li := range stream.db.Levels() {
			numTables += li.NumTables
		}
		stream.db.vlog.filesLock.RLock()
		numVlogFiles = len(stream.db.vlog.filesMap)
		stream.db.vlog.filesLock.RUnlock()
	}

	if err := mw.finish(maxVersion, numTables, numVlogFiles); err != nil {
		return 0, err
	}
	return maxVersion, nil
}

// backupStreamReader reads either the manifest-based or the legacy backup
// format, transparently detecting which one it is given.
type backupStreamReader struct {
	br           *bufio.Reader
	legacy       bool
	header       backupManifest
	unmarshalBuf []byte

	// Statistics computed while reading, validated against the trailer
	// manifest (new format) and recorded in the restore commit marker.
	shards     []backupShard
	numKVs     uint64
	maxVersion uint64
}

func newBackupStreamReader(r io.Reader) (*backupStreamReader, error) {
	bsr := &backupStreamReader{
		br:           bufio.NewReaderSize(r, 16<<10),
		unmarshalBuf: make([]byte, 1<<10),
	}
	magic, err := bsr.br.Peek(len(backupHeaderMagic))
	switch {
	case err == nil && string(magic) == backupHeaderMagic:
		if _, err := bsr.br.Discard(len(backupHeaderMagic)); err != nil {
			return nil, err
		}
		blob, err := bsr.readJSONBlob(maxBackupHeaderSize)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot read backup header: %v", ErrBackupCorrupt, err)
		}
		if err := json.Unmarshal(blob, &bsr.header); err != nil {
			return nil, fmt.Errorf("%w: malformed backup header: %v", ErrBackupCorrupt, err)
		}
		if bsr.header.FormatVersion > backupFormatVersion {
			return nil, fmt.Errorf("%w: backup format version %d, this build supports up to %d",
				ErrBackupVersion, bsr.header.FormatVersion, backupFormatVersion)
		}
		for _, feature := range bsr.header.RequiredFeatures {
			if _, ok := supportedRequiredFeatures[feature]; !ok {
				return nil, fmt.Errorf("%w: %q", ErrBackupFeature, feature)
			}
		}
	default:
		// Includes io.EOF (empty stream): treat anything without the magic as
		// a legacy backup, which fails naturally later if it is garbage.
		bsr.legacy = true
	}
	return bsr, nil
}

func (r *backupStreamReader) readJSONBlob(maxSize uint64) ([]byte, error) {
	var sz uint64
	if err := binary.Read(r.br, binary.LittleEndian, &sz); err != nil {
		return nil, err
	}
	if sz > maxSize {
		return nil, fmt.Errorf("declared size %d exceeds limit %d", sz, maxSize)
	}
	buf := make([]byte, sz)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// next returns the next KVList in the backup, or io.EOF when the backup has
// been fully consumed (and, for the new format, fully validated).
func (r *backupStreamReader) next() (*pb.KVList, error) {
	if r.legacy {
		return r.nextLegacy()
	}

	var prefix [8]byte
	if _, err := io.ReadFull(r.br, prefix[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("%w: backup ended before the trailer manifest",
				ErrBackupCorrupt)
		}
		return nil, err
	}
	if string(prefix[:]) == backupTrailerMagic {
		if err := r.readTrailer(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	sz := binary.LittleEndian.Uint64(prefix[:])
	if sz > maxBackupRecordSize {
		return nil, fmt.Errorf("%w: record %d declares %d bytes, limit is %d",
			ErrBackupCorrupt, len(r.shards), sz, uint64(maxBackupRecordSize))
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r.br, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("%w: truncated checksum for record %d",
			ErrBackupCorrupt, len(r.shards))
	}
	wantCRC := binary.LittleEndian.Uint32(crcBuf[:])

	if cap(r.unmarshalBuf) < int(sz) {
		r.unmarshalBuf = make([]byte, sz)
	}
	payload := r.unmarshalBuf[:sz]
	if _, err := io.ReadFull(r.br, payload); err != nil {
		return nil, fmt.Errorf("%w: truncated payload for record %d",
			ErrBackupCorrupt, len(r.shards))
	}
	if crc := crc32.Checksum(payload, y.CastagnoliCrcTable); crc != wantCRC {
		return nil, fmt.Errorf("%w: checksum mismatch for record %d",
			ErrBackupCorrupt, len(r.shards))
	}

	list := &pb.KVList{}
	if err := proto.Unmarshal(payload, list); err != nil {
		return nil, fmt.Errorf("%w: malformed record %d: %v", ErrBackupCorrupt, len(r.shards), err)
	}
	r.account(list, backupShard{
		Index:  len(r.shards),
		Bytes:  sz,
		NumKVs: uint64(len(list.Kv)),
		CRC32:  wantCRC,
	})
	return list, nil
}

func (r *backupStreamReader) nextLegacy() (*pb.KVList, error) {
	var sz uint64
	if err := binary.Read(r.br, binary.LittleEndian, &sz); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("%w: %v", ErrBackupCorrupt, err)
	}
	if sz > maxBackupRecordSize {
		return nil, fmt.Errorf("%w: record declares %d bytes, limit is %d",
			ErrBackupCorrupt, sz, uint64(maxBackupRecordSize))
	}
	if cap(r.unmarshalBuf) < int(sz) {
		r.unmarshalBuf = make([]byte, sz)
	}
	payload := r.unmarshalBuf[:sz]
	if _, err := io.ReadFull(r.br, payload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBackupCorrupt, err)
	}
	list := &pb.KVList{}
	if err := proto.Unmarshal(payload, list); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBackupCorrupt, err)
	}
	r.account(list, backupShard{
		Index:  len(r.shards),
		Bytes:  sz,
		NumKVs: uint64(len(list.Kv)),
	})
	return list, nil
}

func (r *backupStreamReader) account(list *pb.KVList, shard backupShard) {
	r.shards = append(r.shards, shard)
	r.numKVs += uint64(len(list.Kv))
	for _, kv := range list.Kv {
		if kv.Version > r.maxVersion {
			r.maxVersion = kv.Version
		}
	}
}

// readTrailer reads and validates the trailer manifest against everything
// read so far. It also requires the trailer to be the last thing in the
// stream, so truncated or trailing garbage is caught.
func (r *backupStreamReader) readTrailer() error {
	blob, err := r.readJSONBlob(maxBackupManifestSize)
	if err != nil {
		return fmt.Errorf("%w: cannot read trailer manifest: %v", ErrBackupCorrupt, err)
	}
	var m backupManifest
	if err := json.Unmarshal(blob, &m); err != nil {
		return fmt.Errorf("%w: malformed trailer manifest: %v", ErrBackupCorrupt, err)
	}
	if m.FormatVersion != r.header.FormatVersion {
		return fmt.Errorf("%w: trailer format version %d does not match header %d",
			ErrBackupCorrupt, m.FormatVersion, r.header.FormatVersion)
	}
	if m.NumRecords != uint64(len(m.Shards)) || m.NumRecords != uint64(len(r.shards)) {
		return fmt.Errorf("%w: manifest declares %d records, backup contains %d",
			ErrBackupCorrupt, m.NumRecords, len(r.shards))
	}
	for i, declared := range m.Shards {
		got := r.shards[i]
		if declared.CRC32 != got.CRC32 || declared.Bytes != got.Bytes ||
			declared.NumKVs != got.NumKVs {
			return fmt.Errorf("%w: shard %d does not match the manifest",
				ErrBackupCorrupt, i)
		}
	}
	if m.NumKVs != r.numKVs {
		return fmt.Errorf("%w: manifest declares %d kvs, backup contains %d",
			ErrBackupCorrupt, m.NumKVs, r.numKVs)
	}
	if m.MaxVersion != r.maxVersion {
		return fmt.Errorf("%w: manifest declares max version %d, backup contains %d",
			ErrBackupCorrupt, m.MaxVersion, r.maxVersion)
	}
	// The trailer must terminate the backup.
	if _, err := r.br.ReadByte(); err != io.EOF {
		return fmt.Errorf("%w: unexpected data after the trailer manifest", ErrBackupCorrupt)
	}
	return nil
}

// RestoreOptions controls a transactional Restore.
type RestoreOptions struct {
	// Options describes the database to create. Options.Dir (and
	// Options.ValueDir, if different) is the restore target; the restored
	// database is built in a staging directory next to it and only switched
	// over once the backup has been fully read and validated.
	Options Options
	// MaxPendingWrites bounds concurrent writes while loading. Values <= 0
	// select a default of 256.
	MaxPendingWrites int
}

// Directory layout used while a restore is in flight. The commit marker is
// written only after the staged database is complete and synced; its presence
// after a crash tells recovery that the switch had committed, while a staging
// directory without the marker is a half-written restore that is discarded.
const (
	restoreStagingSuffix = ".restore-staging"
	restoreOldSuffix     = ".restore-old"
	restoreCommitSuffix  = ".restore-commit"
)

type restoreDirTriple struct {
	target  string
	staging string
	old     string
}

func restoreTriples(dir, valueDir string) []restoreDirTriple {
	triples := []restoreDirTriple{{
		target:  dir,
		staging: dir + restoreStagingSuffix,
		old:     dir + restoreOldSuffix,
	}}
	if valueDir != "" && valueDir != dir {
		triples = append(triples, restoreDirTriple{
			target:  valueDir,
			staging: valueDir + restoreStagingSuffix,
			old:     valueDir + restoreOldSuffix,
		})
	}
	return triples
}

func restoreCommitMarkerPath(dir string) string {
	return dir + restoreCommitSuffix
}

// restoreCommitMarker is written once the staged database is complete, just
// before the directory switch. After a crash it distinguishes a committed
// restore (marker present: complete the switch) from a half-written one
// (marker absent: discard the staging directories).
type restoreCommitMarker struct {
	CommittedAt   int64  `json:"committed_at"`
	FormatVersion int    `json:"format_version"`
	NumRecords    uint64 `json:"num_records"`
	NumKVs        uint64 `json:"num_kvs"`
	MaxVersion    uint64 `json:"max_version"`
}

func dirExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// RecoverIncompleteRestore inspects dir (and valueDir, if different) for
// leftovers of an interrupted restore and brings the target back to a
// consistent state: a restore that had reached its commit marker is
// completed, anything else is rolled back so the pre-existing database, if
// any, keeps working. Restore runs this automatically before starting; it is
// exported so applications can also run it at startup.
func RecoverIncompleteRestore(dir, valueDir string) error {
	triples := restoreTriples(dir, valueDir)
	markerPath := restoreCommitMarkerPath(dir)

	if fileExists(markerPath) {
		// The restore had committed. Complete the directory switch; every
		// step is idempotent so re-running after another crash is safe.
		for _, tr := range triples {
			if err := os.RemoveAll(tr.old); err != nil {
				return err
			}
			switch {
			case dirExists(tr.staging):
				if dirExists(tr.target) {
					if err := os.Rename(tr.target, tr.old); err != nil {
						return err
					}
				}
				if err := os.Rename(tr.staging, tr.target); err != nil {
					return err
				}
			case !dirExists(tr.target) && dirExists(tr.old):
				// Unreachable in the normal sequence; restore the old
				// directory rather than leaving nothing behind.
				if err := os.Rename(tr.old, tr.target); err != nil {
					return err
				}
			}
			if err := os.RemoveAll(tr.old); err != nil {
				return err
			}
		}
		if err := os.Remove(markerPath); err != nil {
			return err
		}
		return syncDir(filepath.Dir(dir))
	}

	// No commit marker: the restore never committed. Discard half-written
	// staging directories; the original target was never touched.
	for _, tr := range triples {
		if err := os.RemoveAll(tr.staging); err != nil {
			return err
		}
		if dirExists(tr.old) {
			// Defensive: the switch only runs after the marker is written,
			// so this should not exist. Prefer keeping a working database.
			if dirExists(tr.target) {
				if err := os.RemoveAll(tr.old); err != nil {
					return err
				}
			} else if err := os.Rename(tr.old, tr.target); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeRestoreCommitMarker(path string, marker restoreCommitMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(path))
}

// commitRestoreSwitch performs the committed directory switch. Every step is
// idempotent and mirrored by RecoverIncompleteRestore, so a crash at any
// point is recovered correctly on the next start.
func commitRestoreSwitch(triples []restoreDirTriple, markerPath string) error {
	for _, tr := range triples {
		if dirExists(tr.target) {
			if err := os.Rename(tr.target, tr.old); err != nil {
				return fmt.Errorf("moving aside %s: %w", tr.target, err)
			}
		}
	}
	for _, tr := range triples {
		if err := os.Rename(tr.staging, tr.target); err != nil {
			return fmt.Errorf("switching %s into place: %w", tr.staging, err)
		}
	}
	if err := syncDir(filepath.Dir(triples[0].target)); err != nil {
		return err
	}
	for _, tr := range triples {
		if err := os.RemoveAll(tr.old); err != nil {
			return err
		}
	}
	return os.Remove(markerPath)
}

// Restore loads a backup produced by DB.Backup or DB.BackupWithManifest into
// Options.Dir as a single transaction: the database is fully built and
// validated in a staging directory first, and the target is switched over
// only after a commit marker has been written. Any failure — a corrupt or
// truncated backup, an unsupported format version or required feature, or a
// cancellation — leaves a pre-existing target database untouched. See
// RestoreContext for cancellation support.
func Restore(r io.Reader, ro RestoreOptions) error {
	return RestoreContext(context.Background(), r, ro)
}

// RestoreContext is like Restore but aborts the restore when ctx is
// cancelled, cleaning up the staging directories and all open handles so the
// original database remains usable.
func RestoreContext(ctx context.Context, r io.Reader, ro RestoreOptions) (err error) {
	opt := ro.Options
	if opt.Dir == "" {
		return fmt.Errorf("Restore: Options.Dir must be set")
	}
	if opt.InMemory {
		return fmt.Errorf("Restore: cannot restore into an in-memory database")
	}
	if opt.ValueDir == "" {
		opt.ValueDir = opt.Dir
	}
	maxPendingWrites := ro.MaxPendingWrites
	if maxPendingWrites <= 0 {
		maxPendingWrites = 256
	}

	// Parse and validate the backup header (format version, required
	// features) before anything on disk is touched.
	br, err := newBackupStreamReader(r)
	if err != nil {
		return err
	}

	// Clean up any leftovers from a previously interrupted restore.
	if err := RecoverIncompleteRestore(opt.Dir, opt.ValueDir); err != nil {
		return err
	}

	triples := restoreTriples(opt.Dir, opt.ValueDir)
	for _, tr := range triples {
		if err := os.MkdirAll(tr.staging, 0o755); err != nil {
			return err
		}
	}
	// From here on, any failure must leave no staging state behind.
	committed := false
	defer func() {
		if !committed {
			for _, tr := range triples {
				os.RemoveAll(tr.staging)
			}
		}
	}()

	stagingOpt := opt
	stagingOpt.Dir = triples[0].staging
	stagingOpt.ValueDir = triples[len(triples)-1].staging
	stagingOpt.ReadOnly = false

	db, err := Open(stagingOpt)
	if err != nil {
		return err
	}

	ldr := db.NewKVLoader(maxPendingWrites)
	loadErr := func() error {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			list, err := br.next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			for _, kv := range list.Kv {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := ldr.Set(kv); err != nil {
					return err
				}
				// Update nextTxnTs, memtable stores this timestamp in badger
				// head when flushed.
				if kv.Version >= db.orc.nextTxnTs {
					db.orc.nextTxnTs = kv.Version + 1
				}
			}
		}
	}()
	// Always drain the loader so no write is in flight when the DB is closed.
	if finishErr := ldr.Finish(); loadErr == nil {
		loadErr = finishErr
	}
	if loadErr == nil {
		db.orc.txnMark.Done(db.orc.nextTxnTs - 1)
	}
	closeErr := db.Close()
	if loadErr != nil {
		return loadErr
	}
	if closeErr != nil {
		return closeErr
	}

	formatVersion := 0
	if !br.legacy {
		formatVersion = br.header.FormatVersion
	}
	marker := restoreCommitMarker{
		CommittedAt:   time.Now().Unix(),
		FormatVersion: formatVersion,
		NumRecords:    uint64(len(br.shards)),
		NumKVs:        br.numKVs,
		MaxVersion:    br.maxVersion,
	}
	markerPath := restoreCommitMarkerPath(opt.Dir)
	if err := writeRestoreCommitMarker(markerPath, marker); err != nil {
		return err
	}

	if err := commitRestoreSwitch(triples, markerPath); err != nil {
		return err
	}
	committed = true
	return nil
}
