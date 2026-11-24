/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newGCTestDB opens a DB tuned so that large values land in the vlog and the
// vlog rotates frequently, giving GC something to relocate.
func newGCTestDB(t *testing.T, dir string) *DB {
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20 // 1 MiB (the minimum Badger accepts)
	opt.BaseTableSize = 1 << 15
	opt.ValueThreshold = 1 << 8 // 256 B: test values always go to the vlog
	opt.MemTableSize = 1 << 15
	opt.NumCompactors = 0
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

// seedGCData writes n keys with large random values spread across several
// vlog files, then deletes half of them so the oldest vlog file is GC-able.
func seedGCData(t *testing.T, db *DB, n int) (valSize int) {
	valSize = 64 << 10 // 64 KiB > threshold; ~16 values per 1 MiB vlog
	txn := db.NewTransaction(true)
	for i := 0; i < n; i++ {
		v := make([]byte, valSize)
		_, _ = rand.Read(v)
		require.NoError(t, txn.SetEntry(NewEntry([]byte(fmt.Sprintf("key%04d", i)), v)))
		if i%8 == 7 {
			require.NoError(t, txn.Commit())
			txn = db.NewTransaction(true)
		}
	}
	require.NoError(t, txn.Commit())

	for i := 0; i < n/2; i++ {
		txnDelete(t, db, []byte(fmt.Sprintf("key%04d", i)))
	}
	return valSize
}

func oldestVlogFid(t *testing.T, db *DB) uint32 {
	db.vlog.filesLock.RLock()
	defer db.vlog.filesLock.RUnlock()
	fids := db.vlog.sortedFids()
	require.NotEmpty(t, fids)
	return fids[0]
}

// runOldestRewrite drives one rewrite on the oldest (non-writable) vlog file
// directly, mirroring existing GC tests. Going through rewrite() instead of
// RunValueLogGC avoids depending on compaction-populated discard stats, which
// are unavailable with NumCompactors=0.
func runOldestRewrite(t *testing.T, db *DB) error {
	db.vlog.filesLock.RLock()
	fid := db.vlog.sortedFids()[0]
	lf := db.vlog.filesMap[fid]
	db.vlog.filesLock.RUnlock()
	require.NotNil(t, lf)
	return db.vlog.rewrite(lf)
}

// TestGCCheckpointEncodeDecode round-trips a checkpoint through its wire
// format and rejects corruption.
func TestGCCheckpointEncodeDecode(t *testing.T) {
	cp := &gcCheckpoint{
		Generation: 42,
		SourceFid:  7,
		State:      gcStateCommitted,
		Targets: []gcTargetBounds{
			{Fid: 9, StartOff: 20, EndOff: 4096, Records: 11, TotalRecords: 13},
			{Fid: 10, StartOff: vlogHeaderSize, EndOff: 8192, Records: 5, TotalRecords: 5},
		},
	}
	data := cp.encode()
	got, err := decodeCheckpoint(data)
	require.NoError(t, err)
	require.Equal(t, cp.Generation, got.Generation)
	require.Equal(t, cp.SourceFid, got.SourceFid)
	require.Equal(t, cp.State, got.State)
	require.Equal(t, cp.Targets, got.Targets)

	// Bit flip in the payload must be caught by the trailing CRC.
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)/2] ^= 0xff
	_, err = decodeCheckpoint(corrupt)
	require.Error(t, err)

	// Truncation must be rejected rather than decoded as a partial checkpoint.
	_, err = decodeCheckpoint(data[:len(data)-5])
	require.Error(t, err)

	// A bad magic is rejected.
	badMagic := append([]byte(nil), data...)
	badMagic[0] ^= 0x01
	_, err = decodeCheckpoint(badMagic)
	require.Error(t, err)
}

// TestGCRewritePublishesAndConsumesCheckpoint verifies the happy path leaves
// no checkpoint or source file behind and the values remain readable.
func TestGCRewritePublishesAndConsumesCheckpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-happy")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 40
	valSize := seedGCData(t, db, n)

	sourceFid := oldestVlogFid(t, db)
	sourcePath := db.vlog.fpath(sourceFid)
	require.NoError(t, runOldestRewrite(t, db))

	// Source vlog and its checkpoint must both be gone after a clean commit.
	_, err = os.Stat(sourcePath)
	require.True(t, os.IsNotExist(err), "source vlog should be retired")
	_, err = os.Stat(gcCheckpointPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "checkpoint should be consumed")
	_, err = os.Stat(gcCheckpointTempPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "temp checkpoint should not linger")

	for i := n / 2; i < n; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get(key)
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, valSize)
			return nil
		}))
	}
	require.NoError(t, db.Close())

	// Reopen: recovery must find nothing to reconcile and data stays intact.
	db2 := newGCTestDB(t, dir)
	defer func() { require.NoError(t, db2.Close()) }()
	for i := n / 2; i < n; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		require.NoError(t, db2.View(func(txn *Txn) error {
			_, err := txn.Get(key)
			require.NoError(t, err)
			return nil
		}))
	}
}

// crashAtPhase runs a rewrite that aborts at the given phase, then reopens the
// DB and asserts the source and its values are still fully serviceable.
func crashAtPhase(t *testing.T, phase int) {
	dir, err := os.MkdirTemp("", fmt.Sprintf("badger-gc-crash-%d", phase))
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 40
	valSize := seedGCData(t, db, n)
	sourceFid := oldestVlogFid(t, db)
	sourcePath := db.vlog.fpath(sourceFid)

	db.vlogGCCrashHook = func(gotPhase int, fid uint32, _ []gcTargetBounds) error {
		if gotPhase == phase {
			return errGCCrashInjected
		}
		return nil
	}
	err = runOldestRewrite(t, db)
	require.ErrorIs(t, err, errGCCrashInjected)
	db.vlogGCCrashHook = nil

	switch phase {
	case gcPhaseBeforeWriteback, gcPhaseAfterWriteback, gcPhaseAfterSync:
		// No committed checkpoint may exist for an unpublished batch. The
		// source file must be untouched and still readable.
		_, err = os.Stat(gcCheckpointPath(dir, sourceFid))
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(sourcePath)
		require.NoError(t, err)
	case gcPhaseAfterCheckpoint:
		// The committed checkpoint exists but the source was not yet retired.
		_, err = os.Stat(gcCheckpointPath(dir, sourceFid))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// Reopen: recovery reconciles the interrupted batch.
	db2 := newGCTestDB(t, dir)

	// After recovery there must be no checkpoint residue of either kind, and
	// every surviving value remains readable through the LSM.
	_, err = os.Stat(gcCheckpointPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "phase %d: checkpoint not cleaned", phase)
	_, err = os.Stat(gcCheckpointTempPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "phase %d: temp checkpoint not cleaned", phase)

	for i := n / 2; i < n; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		require.NoError(t, db2.View(func(txn *Txn) error {
			item, err := txn.Get(key)
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, valSize)
			return nil
		}))
	}
	require.NoError(t, db2.Close())
}

func TestGCCrashBeforeWriteback(t *testing.T) { crashAtPhase(t, gcPhaseBeforeWriteback) }
func TestGCCrashAfterWriteback(t *testing.T)  { crashAtPhase(t, gcPhaseAfterWriteback) }
func TestGCCrashAfterSync(t *testing.T)       { crashAtPhase(t, gcPhaseAfterSync) }
func TestGCCrashAfterCheckpoint(t *testing.T) { crashAtPhase(t, gcPhaseAfterCheckpoint) }

// TestGCCrashRetrySucceeds checks that a batch aborted before commit can be
// retried to completion against the same source file.
func TestGCCrashRetrySucceeds(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-retry")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 40
	valSize := seedGCData(t, db, n)
	sourceFid := oldestVlogFid(t, db)
	// Give the file an explicit discard budget so it stays a GC candidate.
	db.vlog.discardStats.Update(sourceFid, 4096)

	var attempts int
	db.vlogGCCrashHook = func(phase int, _ uint32, _ []gcTargetBounds) error {
		if phase == gcPhaseAfterWriteback && attempts == 0 {
			attempts++
			return errGCCrashInjected
		}
		return nil
	}
	require.ErrorIs(t, runOldestRewrite(t, db), errGCCrashInjected)
	require.Equal(t, 1, attempts)
	db.vlogGCCrashHook = nil

	// Source is still present and retains its discard budget, so it remains a
	// valid GC candidate.
	db.vlog.filesLock.RLock()
	_, ok := db.vlog.filesMap[sourceFid]
	db.vlog.filesLock.RUnlock()
	require.True(t, ok)
	require.NotZero(t, db.vlog.discardStats.Update(sourceFid, 0))

	// Retry without the hook must commit.
	require.NoError(t, runOldestRewrite(t, db))
	db.vlog.filesLock.RLock()
	_, ok = db.vlog.filesMap[sourceFid]
	db.vlog.filesLock.RUnlock()
	require.False(t, ok, "source should be retired on retry")

	for i := n / 2; i < n; i++ {
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(fmt.Sprintf("key%04d", i)))
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, valSize)
			return nil
		}))
	}
	require.NoError(t, db.Close())
}

// TestGCCommittedCheckpointTruncatedTargetFailsRecovery reproduces the
// "truncated file treated as a complete value log" hazard: a committed
// checkpoint whose target no longer reaches its boundary must make reopen fail
// loudly instead of silently accepting the short vlog.
func TestGCCommittedCheckpointTruncatedTargetFailsRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-trunc")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 40
	seedGCData(t, db, n)

	// Capture the boundary the real rewrite would commit, then abort before
	// the source is retired.
	var captured []gcTargetBounds
	db.vlogGCCrashHook = func(phase int, _ uint32, bounds []gcTargetBounds) error {
		if phase == gcPhaseAfterCheckpoint {
			captured = append([]gcTargetBounds(nil), bounds...)
			return errGCCrashInjected
		}
		return nil
	}
	require.ErrorIs(t, runOldestRewrite(t, db), errGCCrashInjected)
	db.vlogGCCrashHook = nil
	require.NotEmpty(t, captured)
	require.NoError(t, db.Close())

	// Corrupt the first target by truncating it below the recorded boundary.
	target := captured[0]
	targetPath := filepath.Join(dir, fmt.Sprintf("%06d.vlog", target.Fid))
	require.NoError(t, os.Truncate(targetPath, int64(target.StartOff)+1))

	// Reopen must fail before the DB becomes usable. Open returns a partially
	// constructed handle on this path, so we only assert the error rather than
	// closing it.
	_, err = Open(getTestOptions(dir))
	require.Error(t, err, "reopen must reject the truncated committed target")
	require.Contains(t, err.Error(), "gc checkpoint recovery")
}

// TestGCPreparedTempResidueCleaned ensures a leftover temp checkpoint from an
// unpublished batch is discarded on restart without touching the source.
func TestGCPreparedTempResidueCleaned(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-temp")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 32
	valSize := seedGCData(t, db, n)
	sourceFid := oldestVlogFid(t, db)

	db.vlogGCCrashHook = func(phase int, fid uint32, _ []gcTargetBounds) error {
		if phase == gcPhaseAfterSync {
			// Simulate a crash after the prepared temp was fsynced but before
			// the atomic rename: plant the temp file directly.
			cp := &gcCheckpoint{
				Generation: 1,
				SourceFid:  fid,
				State:      gcStatePrepared,
			}
			require.NoError(t, os.WriteFile(gcCheckpointTempPath(dir, fid), cp.encode(), 0666))
			return errGCCrashInjected
		}
		return nil
	}
	require.ErrorIs(t, runOldestRewrite(t, db), errGCCrashInjected)
	db.vlogGCCrashHook = nil
	require.NoError(t, db.Close())

	_, err = os.Stat(gcCheckpointTempPath(dir, sourceFid))
	require.NoError(t, err)

	db2 := newGCTestDB(t, dir)
	_, err = os.Stat(gcCheckpointTempPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "temp residue must be cleaned on recovery")
	// Source survives and is still readable.
	for i := n / 2; i < n; i++ {
		require.NoError(t, db2.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(fmt.Sprintf("key%04d", i)))
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, valSize)
			return nil
		}))
	}
	require.NoError(t, db2.Close())
}

// TestGCDiscardNotAdvancedBeforeCommit proves the source keeps its discard
// budget while the rewrite is in flight: a failed batch leaves the stats
// intact so the file can be picked again.
func TestGCDiscardNotAdvancedBeforeCommit(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-discard")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	seedGCData(t, db, 32)
	sourceFid := oldestVlogFid(t, db)
	// With compactors off, populate a discard budget explicitly so we can
	// observe that a failed rewrite neither consumes nor changes it.
	before := db.vlog.discardStats.Update(sourceFid, 4096)
	require.NotZero(t, before)

	db.vlogGCCrashHook = func(phase int, _ uint32, _ []gcTargetBounds) error {
		if phase == gcPhaseAfterWriteback {
			// Mid-batch the discard budget must be unchanged.
			require.Equal(t, before, db.vlog.discardStats.Update(sourceFid, 0))
			return errGCCrashInjected
		}
		return nil
	}
	require.ErrorIs(t, runOldestRewrite(t, db), errGCCrashInjected)
	db.vlogGCCrashHook = nil

	require.Equal(t, before, db.vlog.discardStats.Update(sourceFid, 0),
		"failed batch must not consume discard budget")
	require.NoError(t, db.Close())
}

// TestGCConcurrentReadersNotBlocked runs foreground readers continuously while
// a full rewrite commits. Every read must finish promptly: the durability
// barrier and boundary verification take only read locks on target files and
// short-lived locks on the file map, so readers cannot be stalled for the
// duration of the sync.
func TestGCConcurrentReadersNotBlocked(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-concurrent")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 40
	valSize := seedGCData(t, db, n)

	// Slow down the commit window so concurrent reads actually overlap the
	// sync and checkpoint phases.
	release := make(chan struct{})
	db.vlogGCCrashHook = func(phase int, _ uint32, _ []gcTargetBounds) error {
		if phase == gcPhaseAfterSync {
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				t.Fatal("hook never released")
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := []byte(fmt.Sprintf("key%04d", n/2+(w%(n/2))))
				done := make(chan error, 1)
				go func() {
					done <- db.View(func(txn *Txn) error {
						item, err := txn.Get(key)
						if err != nil {
							return err
						}
						v, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						if len(v) != valSize {
							return fmt.Errorf("bad value size %d", len(v))
						}
						return nil
					})
				}()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					panic("foreground read blocked by GC for over 10s")
				}
			}
		}()
	}

	gcDone := make(chan error, 1)
	go func() { gcDone <- runOldestRewrite(t, db) }()

	// Give the readers a head start, then let the rewrite pass the barrier.
	time.Sleep(100 * time.Millisecond)
	close(release)
	require.NoError(t, <-gcDone)

	close(stop)
	wg.Wait()
	require.NoError(t, db.Close())
}

// TestGCMultiTargetBoundary forces the writable vlog to rotate while a single
// rewrite batch is written back, so one batch spans several target files. The
// checkpoint must record a boundary per target and recovery must validate all
// of them.
func TestGCMultiTargetBoundary(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-multi")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20
	opt.BaseTableSize = 1 << 15
	opt.ValueThreshold = 1 << 8
	opt.MemTableSize = 1 << 15
	opt.NumCompactors = 0
	// Rotate the writable vlog after only a handful of entries so a relocated
	// batch reliably crosses file boundaries.
	opt.ValueLogMaxEntries = 3
	db, err := Open(opt)
	require.NoError(t, err)

	n := 40
	valSize := seedGCData(t, db, n)

	var captured []gcTargetBounds
	db.vlogGCCrashHook = func(phase int, _ uint32, bounds []gcTargetBounds) error {
		if phase == gcPhaseAfterCheckpoint {
			captured = append([]gcTargetBounds(nil), bounds...)
		}
		return nil
	}
	require.NoError(t, runOldestRewrite(t, db))
	db.vlogGCCrashHook = nil

	// The batch must have landed in more than one target file.
	require.Greater(t, len(captured), 1, "rewrite should span multiple target vlogs")
	var totalGC, totalAll uint32
	seenFids := make(map[uint32]bool)
	for _, b := range captured {
		require.False(t, seenFids[b.Fid], "target fid repeated in checkpoint")
		seenFids[b.Fid] = true
		require.NotZero(t, b.Records)
		require.Greater(t, b.EndOff, b.StartOff)
		require.GreaterOrEqual(t, b.TotalRecords, b.Records)
		totalGC += b.Records
		totalAll += b.TotalRecords
	}
	require.NotZero(t, totalGC)
	require.GreaterOrEqual(t, totalAll, totalGC)

	// Values stay readable immediately and after a reopen.
	checkValues := func(handle *DB) {
		for i := n / 2; i < n; i++ {
			require.NoError(t, handle.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(fmt.Sprintf("key%04d", i)))
				require.NoError(t, err)
				v, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Len(t, v, valSize)
				return nil
			}))
		}
	}
	checkValues(db)
	require.NoError(t, db.Close())

	db2, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	checkValues(db2)
}

// TestGCCheckpointRejectedWhenCorrupt writes a syntactically invalid
// published checkpoint and confirms reopen surfaces it instead of ignoring it.
func TestGCCheckpointRejectedWhenCorrupt(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-badcp")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	seedGCData(t, db, 16)
	sourceFid := oldestVlogFid(t, db)
	require.NoError(t, db.Close())

	// Garbage content at the published checkpoint path.
	cpPath := gcCheckpointPath(dir, sourceFid)
	require.NoError(t, os.WriteFile(cpPath, []byte("not-a-checkpoint"), 0666))

	// Reopen must fail during recovery; the returned handle is only partially
	// constructed, so do not close it.
	_, err = Open(getTestOptions(dir))
	require.Error(t, err, "corrupt checkpoint must fail recovery")
	require.Contains(t, err.Error(), "gc checkpoint recovery")
}

// TestGCTempResidueAlongsideCommitted covers the edge where a stale temp file
// sits next to an older, already published checkpoint for the same source.
// Recovery must remove the temp residue but still reconcile the committed
// batch rather than skipping the published record.
func TestGCTempResidueAlongsideCommitted(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-gc-both")
	require.NoError(t, err)
	defer removeDir(dir)

	db := newGCTestDB(t, dir)
	n := 32
	valSize := seedGCData(t, db, n)
	sourceFid := oldestVlogFid(t, db)
	sourcePath := db.vlog.fpath(sourceFid)

	// Produce a real committed checkpoint, aborting before source retirement.
	db.vlogGCCrashHook = func(phase int, fid uint32, _ []gcTargetBounds) error {
		if phase == gcPhaseAfterCheckpoint && fid == sourceFid {
			return errGCCrashInjected
		}
		return nil
	}
	require.ErrorIs(t, runOldestRewrite(t, db), errGCCrashInjected)
	db.vlogGCCrashHook = nil
	require.NoError(t, db.Close())

	// Both the published checkpoint and a stale prepared temp now coexist.
	pubPath := gcCheckpointPath(dir, sourceFid)
	_, err = os.Stat(pubPath)
	require.NoError(t, err)
	stale := &gcCheckpoint{
		Generation: 999,
		SourceFid:  sourceFid,
		State:      gcStatePrepared,
	}
	require.NoError(t, os.WriteFile(gcCheckpointTempPath(dir, sourceFid), stale.encode(), 0666))

	db2 := newGCTestDB(t, dir)

	// Temp residue cleaned, committed batch reconciled: source retired and the
	// published checkpoint consumed.
	_, err = os.Stat(gcCheckpointTempPath(dir, sourceFid))
	require.True(t, os.IsNotExist(err), "temp residue must be removed")
	_, err = os.Stat(pubPath)
	require.True(t, os.IsNotExist(err), "committed checkpoint must be consumed")
	_, err = os.Stat(sourcePath)
	require.True(t, os.IsNotExist(err), "source must be retired")

	for i := n / 2; i < n; i++ {
		require.NoError(t, db2.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(fmt.Sprintf("key%04d", i)))
			require.NoError(t, err)
			v, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Len(t, v, valSize)
			return nil
		}))
	}
	require.NoError(t, db2.Close())
}
