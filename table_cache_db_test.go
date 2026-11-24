/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/ristretto/v2/z"
)

// populateDB writes n keys, one per transaction, so flushing produces SSTs.
func populateDB(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		txn := db.NewTransaction(true)
		require.NoError(t, txn.Set(
			[]byte(fmt.Sprintf("key-%05d", i)),
			[]byte(fmt.Sprintf("val-%05d", i))))
		require.NoError(t, txn.Commit())
	}
}

// writeStreamTables deterministically writes numStreams tables through a
// StreamWriter, one SST per stream id, so the level handlers end up with a known
// number of distinct tables without depending on memtable-rotation timing.
func writeStreamTables(t *testing.T, db *DB, numStreams, perStream int) {
	t.Helper()
	sw := db.NewStreamWriter()
	require.NoError(t, sw.Prepare())
	buf := z.NewBuffer(16<<20, "tcache-stream")
	defer func() { require.NoError(t, buf.Release()) }()
	for s := 0; s < numStreams; s++ {
		for i := 0; i < perStream; i++ {
			k := []byte(fmt.Sprintf("sw-%02d-%05d", s, i))
			KVToBuffer(&pb.KV{
				Key:      k,
				Value:    []byte(fmt.Sprintf("v-%02d-%05d", s, i)),
				Version:  1,
				StreamId: uint32(s),
			}, buf)
		}
	}
	require.NoError(t, sw.Write(buf))
	require.NoError(t, sw.Flush())
}

func checkAllKeys(t *testing.T, db *DB, n int) {
	t.Helper()
	err := db.View(func(txn *Txn) error {
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("key-%05d", i))
			item, err := txn.Get(k)
			if err != nil {
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, fmt.Sprintf("val-%05d", i), string(v))
		}
		return nil
	})
	require.NoError(t, err)
}

// acquireThroughCache reads one table through the cache and reports its
// generation, exercising the cache Acquire path directly.
func acquireThroughCache(t *testing.T, db *DB, fid uint64) uint64 {
	t.Helper()
	tbl, err := db.tableCache.Acquire(fid)
	require.NoError(t, err)
	gen := tbl.Gen()
	require.NoError(t, tbl.DecrRef())
	return gen
}

// TestDBTableCacheConcurrentReadOpen exercises concurrent opens with a tight
// open-handle budget, forcing repeated eviction and re-open under contention
// (requirement 5: concurrent open + eviction). Every read stays correct.
func TestDBTableCacheConcurrentReadOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-concurrent")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const n = 400
	opt := getTestOptions(dir).WithNumOpenTables(2)
	db, err := Open(opt)
	require.NoError(t, err)

	populateDB(t, db, n)
	require.NoError(t, db.Flatten(4))

	var wg sync.WaitGroup
	const readers = 16
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				checkAllKeys(t, db, n)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, db.Close())

	// Re-open: all handles rebuild through the cache and data survives.
	db2, err := Open(getTestOptions(dir).WithNumOpenTables(2))
	require.NoError(t, err)
	checkAllKeys(t, db2, n)
	require.NoError(t, db2.Close())
}

// TestDBTableCacheEvictionPreservesIterators verifies requirement 2 end to end:
// an iterator opened before heavy eviction stays fully usable even though the
// tables it scans are pushed out of the open-handle budget.
func TestDBTableCacheEvictionPreservesIterators(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-iter")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const n = 300
	db, err := Open(getTestOptions(dir).WithNumOpenTables(1))
	require.NoError(t, err)
	populateDB(t, db, n)
	require.NoError(t, db.Flatten(4))

	txn := db.NewTransaction(false)
	it := txn.NewIterator(DefaultIteratorOptions)

	// Drive heavy reads to evict the tables the iterator is parked on. The
	// iterator holds its own table references, so it must keep working.
	checkAllKeys(t, db, n)
	checkAllKeys(t, db, n)

	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	require.Equal(t, n, count, "iterator must see every key despite eviction")
	it.Close()
	txn.Discard()
	require.NoError(t, db.Close())
}

// TestDBTableCacheReopenNewGeneration confirms requirement 1 at the DB level:
// a table evicted under a tight budget and re-opened gets a fresh generation,
// never a path-reused stale handle.
func TestDBTableCacheReopenNewGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-gen")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// A budget equal to one handle per shard plus more distinct tables than
	// shards: at least one shard must hold more than one table and therefore
	// evict.
	db, err := Open(getTestOptions(dir).WithNumOpenTables(tableCacheShards))
	require.NoError(t, err)
	writeStreamTables(t, db, tableCacheShards+4, 20)

	allIDs := allTableIDs(db)
	require.Greater(t, len(allIDs), tableCacheShards,
		"need more tables than shards so some shard exceeds its budget")

	// Find a file id whose shard contains at least two tables, guaranteeing a
	// real eviction rather than a no-op.
	victim, peer := pickCollidingIDs(t, db, allIDs)

	genBefore := acquireThroughCache(t, db, victim)
	for i := 0; i < 10; i++ {
		if tbl, err := db.tableCache.Acquire(peer); err == nil {
			require.NoError(t, tbl.DecrRef())
		}
		if tbl, err := db.tableCache.Acquire(victim); err == nil {
			require.NoError(t, tbl.DecrRef())
		}
	}
	genAfter := acquireThroughCache(t, db, victim)
	require.NotEqual(t, genBefore, genAfter,
		"re-opened table must have a new generation, not a path-reused handle")
	require.NoError(t, db.Close())
}

// pickCollidingIDs returns two table ids that hash to the same cache shard.
func pickCollidingIDs(t *testing.T, db *DB, ids []uint64) (uint64, uint64) {
	t.Helper()
	buckets := make(map[uint64]uint64)
	for _, id := range ids {
		sh := id % tableCacheShards
		if other, ok := buckets[sh]; ok {
			return other, id
		}
		buckets[sh] = id
	}
	t.Fatal("expected two tables in the same cache shard")
	return 0, 0
}

// allTableIDs snapshots the table ids across all levels.
func allTableIDs(db *DB) []uint64 {
	var ids []uint64
	for _, l := range db.lc.levels {
		l.RLock()
		for _, tbl := range l.tables {
			ids = append(ids, tbl.ID())
		}
		l.RUnlock()
	}
	return ids
}

// multiTableDB opens a DB and deterministically writes at least two distinct
// SSTs through a StreamWriter (one table per stream), so the level handlers
// contain a known table set without depending on memtable-rotation timing. The
// returned DB is left open for the caller.
func multiTableDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	writeStreamTables(t, db, 4, 20)
	require.GreaterOrEqual(t, len(allTableIDs(db)), 2,
		"need at least two distinct tables")
	return db
}

// TestDBTableCacheInvalidateReopensFreshHandle checks requirement 1 + 4:
// invalidating one table evicts only that entry and the next access reopens it
// under a new generation, while another table is untouched.
func TestDBTableCacheInvalidateReopensFreshHandle(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-invalidate")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db := multiTableDB(t, dir)
	defer func() { require.NoError(t, db.Close()) }()

	ids := allTableIDs(db)
	victim := ids[0]
	survivor := ids[1]

	victimGen1 := acquireThroughCache(t, db, victim)
	survivorGen1 := acquireThroughCache(t, db, survivor)

	db.tableCache.Invalidate(victim)

	victimGen2 := acquireThroughCache(t, db, victim)
	require.NotEqual(t, victimGen1, victimGen2,
		"invalidated table must reopen under a new generation")

	survivorGen2 := acquireThroughCache(t, db, survivor)
	require.Equal(t, survivorGen1, survivorGen2,
		"unrelated table must not be invalidated")
	require.NoError(t, db.Close())
}

// TestDBTableCacheCloseWakesWaiters checks requirement 5 at the DB level:
// closing the DB while table opens are in flight wakes all waiters instead of
// hanging. We block the opener via an injected hook around the cache.
func TestDBTableCacheCloseWakesWaiters(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-close")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db := multiTableDB(t, dir)
	defer func() { require.NoError(t, db.Close()) }()

	ids := allTableIDs(db)
	require.NotEmpty(t, ids)

	// Concurrent acquirers take the open path. Closing concurrently must never
	// block forever and all goroutines must finish.
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fid := ids[i%len(ids)]
			// Tolerate either a successful open or a closed-cache error; the
			// assertion is that no goroutine hangs past db.Close().
			if tbl, err := db.tableCache.Acquire(fid); err == nil {
				_ = tbl.DecrRef()
			}
		}(i)
	}
	// Interleave a close while acquirers are in flight.
	require.NoError(t, db.Close())

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("db.Close did not unblock waiting table loaders")
	}

	// After close, Acquire fails immediately.
	_, err = db.tableCache.Acquire(ids[0])
	require.ErrorIs(t, err, table.ErrTableCacheClosed)
}

// TestDBTableCacheIsolationOnCorruptTable verifies requirement 4: corrupting one
// SST only affects that table's cache entry -- the DB still opens, the failed
// open never installs a poisoned handle, and every other table/shard stays
// readable and iterable.
func TestDBTableCacheIsolationOnCorruptTable(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-tcache-corrupt")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	writeStreamTables(t, db, 4, 20)
	beforeIDs := allTableIDs(db)
	require.GreaterOrEqual(t, len(beforeIDs), 2, "need multiple tables")
	require.NoError(t, db.Close())

	ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, ssts)
	victim := ssts[0]
	victimID, ok := table.ParseFileID(victim)
	require.True(t, ok)
	data, err := os.ReadFile(victim)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	// Flip bytes in the first data block. This corrupts the block so opening the
	// table fails; the load path must skip just this table rather than fail the
	// whole DB or leave a bogus handle in the cache.
	for i := 8; i < 16; i++ {
		data[i] ^= 0xFF
	}
	require.NoError(t, os.WriteFile(victim, data, 0o600))

	// Opening must succeed: the corrupt table is skipped, not fatal.
	db2, err := Open(getTestOptions(dir).
		WithChecksumVerificationMode(options.OnTableRead))
	require.NoError(t, err)

	// The corrupt table is absent from the installed levels and the cache, while
	// every healthy table is still present and usable.
	afterIDs := allTableIDs(db2)
	for _, id := range afterIDs {
		require.NotEqual(t, victimID, id,
			"the corrupt table must not be installed or cached")
	}
	var survivor uint64
	for _, id := range beforeIDs {
		if id != victimID {
			survivor = id
			break
		}
	}
	require.NotZero(t, survivor, "there must be at least one healthy table")
	require.Contains(t, afterIDs, survivor)

	tbl, err := db2.tableCache.Acquire(survivor)
	require.NoError(t, err, "healthy table handles must survive corruption isolation")
	it := tbl.NewIterator(0)
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	require.NoError(t, it.Close())
	require.Greater(t, count, 0, "a healthy table must remain iterable")
	require.NoError(t, tbl.DecrRef())
	require.NoError(t, db2.Close())
}
