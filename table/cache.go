/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrTableCacheClosed is returned by table-cache operations once the cache has
// been closed, i.e. the owning DB is shutting down. Waiters blocked on an
// in-flight open are woken with this error instead of hanging.
var ErrTableCacheClosed = errors.New("table cache is closed")

// TableOpener opens the table file identified by id. The cache assigns the
// generation gen for that incarnation; the opener must pass it through to
// OpenTable/OpenInMemoryTable. The returned table carries exactly one external
// reference (the one OpenTable hands its caller), which the cache transfers to
// the first Acquirer; it must not be separately retained by the opener.
type TableOpener func(id uint64, gen uint64) (*Table, error)

// tableKey identifies one incarnation of a table file: file id plus generation.
type tableKey struct {
	fid uint64
	gen uint64
}

// cacheEntry is one incarnation of a table file inside the TableCache.
//
// An entry is tracked in one of two places:
//   - shard.live while it is the current, retained incarnation of its fid and is
//     counted against the LRU capacity;
//   - shard.detached after eviction/invalidation, while the table still has
//     external references (a level handler, a reader or an iterator), or briefly
//     while an idle evicted entry waits to be finalized. It self-removes when
//     the final external reference is released.
//
// The cache's own retention is NOT counted in Table.ref: it is represented
// purely by membership in the live LRU map. Table.ref counts only external
// holders (level handlers and active readers). This separation lets an
// eviction unregister an entry and close an idle handle without touching the
// ref count or re-entering any lock, while a referenced handle simply moves to
// detached and survives until its final DecrRef.
type cacheEntry struct {
	key   tableKey
	table *Table

	// ready is closed once an in-flight open finishes (success or failure).
	// nil on already-published entries. Concurrent Acquires wait on it instead
	// of opening the same file twice.
	ready chan struct{}

	// lruElem is this entry's position in the shard LRU list; nil once detached.
	lruElem *list.Element
}

// shard is one independently locked partition of the table cache.
type shard struct {
	mu sync.Mutex
	// cond wakes waiters when an in-flight open completes or the cache closes.
	// Its Locker is &mu.
	cond *sync.Cond

	// live maps fid -> its current incarnation while retained in the LRU.
	live map[uint64]*cacheEntry
	// detached maps full (fid,gen) -> entries no longer in the LRU. Entries with
	// external references survive here until their final DecrRef; zero-ref
	// entries are finalized right after the shard lock is released.
	detached map[tableKey]*cacheEntry

	// lru orders live entries by recency, most recently used at the front. Only
	// live entries are present, so lru.Len() is the count bounded by capacity.
	lru *list.List
}

// TableCache is a capacity-bounded, generation-aware cache of open table
// handles. It is the single place that opens table files and the owner of
// handle lifetimes:
//
//   - Cache keys are (file id, generation). Every open gets a fresh,
//     monotonically increasing generation, so a re-opened or replaced file never
//     aliases the old handle or the block/index cache entries it left behind.
//   - Active reads and level handlers hold references on the Table. An
//     over-capacity eviction only unregisters an entry; an idle handle is closed
//     immediately while a referenced one is kept alive in detached until its
//     last reference is dropped.
//   - A failed open publishes an error to all concurrent waiters and never
//     inserts a usable entry, so a corrupt/new table cannot poison lookups.
//   - Invalidate/MarkDeleted affect only the named fid; every other shard and
//     any live iterator keeps working.
//   - Close fails new Acquires, wakes every waiter, and releases all idle
//     handles; externally referenced handles are released by their final
//     DecrRef.
type TableCache struct {
	numShards uint64
	shards    []*shard
	// capacity bounds the number of retained (live) handles per shard before LRU
	// eviction. A non-positive value evicts a handle as soon as it is registered
	// unless it still has external references.
	capacity int

	// nextGen supplies strictly monotonic generations.
	nextGen atomic.Uint64

	opener TableOpener

	// closed flips to true once in Close. It is read atomically; all state
	// mutations are still guarded by the per-shard locks. A dedicated mutex is
	// deliberately avoided: Acquire must be able to wait on a shard's cond
	// without holding any lock that Close needs, otherwise Close could never
	// run to broadcast the wakeup.
	closed atomic.Bool
}

// NewTableCache creates a TableCache with the given per-shard handle capacity
// and shard count. A capacity <= 0 means handles are retained without an
// open-handle limit (they are still released when removed or the cache closes).
// numShards below 1 is treated as 1.
func NewTableCache(capacity, numShards int, opener TableOpener) *TableCache {
	if numShards <= 0 {
		numShards = 1
	}
	tc := &TableCache{
		numShards: uint64(numShards),
		shards:    make([]*shard, numShards),
		capacity:  capacity,
		opener:    opener,
	}
	tc.nextGen.Store(1)
	for i := range tc.shards {
		sh := &shard{
			live:     make(map[uint64]*cacheEntry),
			detached: make(map[tableKey]*cacheEntry),
			lru:      list.New(),
		}
		sh.cond = sync.NewCond(&sh.mu)
		tc.shards[i] = sh
	}
	return tc
}

func (tc *TableCache) shardFor(fid uint64) *shard {
	return tc.shards[fid%tc.numShards]
}

// NextGen returns a fresh, never-before-used generation.
func (tc *TableCache) NextGen() uint64 {
	return tc.nextGen.Add(1)
}

// Register adopts an already-open table (built by a flush, a compaction or the
// DB-open load path) into the cache under its current generation. The table's
// release hook is wired here so its eventual final external DecrRef routes
// through the cache. Register does not change the table's external reference
// count; the cache's retention is separate.
func (tc *TableCache) Register(t *Table) error {
	if t == nil {
		return errors.New("table cache: cannot register nil table")
	}

	key := tableKey{fid: t.ID(), gen: t.Gen()}
	sh := tc.shardFor(key.fid)
	sh.mu.Lock()
	if tc.closed.Load() {
		sh.mu.Unlock()
		return ErrTableCacheClosed
	}
	if _, exists := sh.live[key.fid]; exists {
		sh.mu.Unlock()
		return errors.New("table cache: table already registered")
	}
	if _, exists := sh.detached[key]; exists {
		sh.mu.Unlock()
		return errors.New("table cache: duplicate generation for table")
	}
	e := &cacheEntry{key: key, table: t}
	e.lruElem = sh.lru.PushFront(e)
	sh.live[key.fid] = e
	t.attachCache(tc)
	idle := tc.evictIdleLocked(sh)
	sh.mu.Unlock()
	return tc.finalizeIdle(idle)
}

// Acquire returns a usable table handle for fid, opening the file on a miss.
// On success the Table has one external reference taken for the caller, who
// MUST pair it with exactly one DecrRef. Concurrent Acquires of the same fid
// coalesce onto a single open and share the incarnation. Waiting callers are
// woken with ErrTableCacheClosed if the cache closes while blocked.
func (tc *TableCache) Acquire(fid uint64) (*Table, error) {
	sh := tc.shardFor(fid)
	sh.mu.Lock()
	if tc.closed.Load() {
		sh.mu.Unlock()
		return nil, ErrTableCacheClosed
	}

	// Wait for any in-flight open of this fid. Loading entries carry a non-nil
	// ready channel; on success they are published in place, on failure removed.
	// The closed check runs while holding sh.mu, the same lock Close uses to
	// broadcast, so a close can never be missed here.
	for {
		if tc.closed.Load() {
			sh.mu.Unlock()
			return nil, ErrTableCacheClosed
		}
		if e, ok := sh.live[fid]; ok {
			if e.ready == nil {
				sh.touch(e)
				e.table.IncrRef() // external ref for the caller
				t := e.table
				sh.mu.Unlock()
				return t, nil
			}
			sh.cond.Wait()
			continue
		}
		break
	}

	// Genuine miss: claim the fid with a loading entry and a fresh generation.
	gen := tc.NextGen()
	key := tableKey{fid: fid, gen: gen}
	e := &cacheEntry{key: key, ready: make(chan struct{})}
	sh.live[fid] = e
	sh.mu.Unlock()

	t, err := tc.opener(fid, gen)

	sh.mu.Lock()
	if err != nil {
		// Drop the loading entry so a later Acquire starts a brand-new
		// generation/open instead of caching the error, then wake the waiters.
		// They re-check the map, find no entry, and open fresh themselves.
		delete(sh.live, fid)
		close(e.ready)
		sh.cond.Broadcast()
		sh.mu.Unlock()
		if tc.closed.Load() {
			return nil, ErrTableCacheClosed
		}
		return nil, err
	}

	e.table = t
	t.attachCache(tc)
	if tc.closed.Load() {
		// The cache closed during the (slow) open. Do not publish the handle or
		// hand it to the caller; remove the loading entry, signal any waiters
		// (they observe the closed flag), and finalize the table after unlocking.
		delete(sh.live, fid)
		close(e.ready)
		sh.cond.Broadcast()
		sh.mu.Unlock()
		_ = tc.releaseEntry(t) // drops the opener's single external reference
		return nil, ErrTableCacheClosed
	}

	e.lruElem = sh.lru.PushFront(e)
	close(e.ready) // Signal completion before clearing, so waiters see the published entry.
	e.ready = nil
	sh.cond.Broadcast()

	// The opener contract hands us a table with exactly one external reference,
	// which is transferred to this caller: no extra IncrRef here, and the handle
	// therefore cannot be closed by eviction at capacity zero until the caller
	// releases it. A cache *hit* above takes its own IncrRef instead.
	idle := tc.evictIdleLocked(sh)
	sh.mu.Unlock()
	if relErr := tc.finalizeIdle(idle); relErr != nil {
		// The caller still holds a valid handle; surface but do not lose it.
		return t, relErr
	}
	return t, nil
}

// MarkDeleted tombstones the given fid and detaches its current incarnation. It
// is used when a table is removed by a compaction/drop: the file is deleted once
// external references drain. Active readers are not disturbed.
func (tc *TableCache) MarkDeleted(fid uint64) {
	sh := tc.shardFor(fid)
	sh.mu.Lock()

	var idle []*Table
	if e, ok := sh.live[fid]; ok && e.table != nil {
		e.table.markDeleted()
		idle = sh.detachEntryLocked(e)
	}
	for key, e := range sh.detached {
		if key.fid == fid && e.table != nil {
			e.table.markDeleted()
		}
	}
	sh.mu.Unlock()
	_ = tc.finalizeIdle(idle)
}

// Invalidate closes and detaches the current incarnation of fid so the next
// Acquire re-opens the file under a fresh generation. Unlike MarkDeleted it
// does not delete the file, which makes it the right action for a table detected
// as corrupt: only that entry is discarded while other shards and any live
// iterator on the old handle keep working. A referenced incarnation is kept
// alive until its readers finish; an idle one is closed immediately.
func (tc *TableCache) Invalidate(fid uint64) {
	sh := tc.shardFor(fid)
	sh.mu.Lock()
	var idle []*Table
	if e, ok := sh.live[fid]; ok && e.table != nil {
		idle = sh.detachEntryLocked(e)
	}
	sh.mu.Unlock()
	_ = tc.finalizeIdle(idle)
}

// releaseEntry is the Table release hook. It runs when a cache-managed table's
// external reference count reaches zero, on the goroutine that dropped the
// final ref (block/index cache entries were already removed by DecrRef). It
// unregisters the entry under the shard lock, then performs file I/O unlocked,
// closing or deleting depending on whether the table was tombstoned.
func (tc *TableCache) releaseEntry(t *Table) error {
	key := tableKey{fid: t.ID(), gen: t.Gen()}
	sh := tc.shardFor(key.fid)
	sh.mu.Lock()
	tc.unregisterLocked(sh, key)
	sh.mu.Unlock()
	return tc.finalizeTable(t)
}

// unregisterLocked removes the entry identified by key from whichever map it
// currently lives in. Caller holds sh.mu.
func (tc *TableCache) unregisterLocked(sh *shard, key tableKey) {
	if e, ok := sh.live[key.fid]; ok && e.key == key {
		sh.removeLiveLocked(e)
	}
	delete(sh.detached, key)
}

// finalizeTable performs the actual file release for a table with no remaining
// external references.
func (tc *TableCache) finalizeTable(t *Table) error {
	if t.isDeleted() {
		return t.Delete()
	}
	return t.closeAndRelease()
}

// finalizeIdle releases zero-external-reference tables collected during an
// eviction/invalidation. It is always called after the shard lock is released.
func (tc *TableCache) finalizeIdle(tables []*Table) error {
	var err error
	for _, t := range tables {
		if relErr := tc.releaseEntry(t); relErr != nil && err == nil {
			err = relErr
		}
	}
	return err
}

// Close shuts the cache down. New Acquires fail with ErrTableCacheClosed and
// every waiter on an in-flight open is woken. Idle live handles are closed;
// handles still referenced externally move to detached and are released by
// their final DecrRef. An open still running when Close is called is left to
// finish: its opener goroutine notices the closed flag on completion and
// finalizes the resulting handle itself (without republishing it). Close is
// idempotent.
func (tc *TableCache) Close() error {
	if tc.closed.Swap(true) {
		return nil
	}

	var toFinalize []*Table
	for _, sh := range tc.shards {
		sh.mu.Lock()
		// Wake every waiter, including those parked on an in-flight open. The
		// loading entry itself stays in live so the completing opener can find
		// it; waiters re-check closed and bail out immediately.
		sh.cond.Broadcast()

		for fid, e := range sh.live {
			if e.ready != nil {
				// In-flight open: do not touch its ready channel (the opener owns
				// the close). It finalizes itself when the open returns.
				continue
			}
			if e.table == nil {
				delete(sh.live, fid)
				continue
			}
			sh.removeLiveLocked(e)
			if e.table.ref.Load() == 0 {
				toFinalize = append(toFinalize, e.table)
			} else {
				// Readers/level handlers remain; finalize on their last DecrRef.
				sh.detached[e.key] = e
			}
		}
		sh.lru.Init()
		sh.mu.Unlock()
	}

	var err error
	for _, t := range toFinalize {
		if closeErr := tc.finalizeTable(t); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

// ---- shard helpers (caller holds sh.mu) ----

func (sh *shard) touch(e *cacheEntry) {
	if e.lruElem != nil {
		sh.lru.MoveToFront(e.lruElem)
	}
}

// removeLiveLocked unlinks e from the LRU and the live map. It does not add the
// entry to detached; callers decide.
func (sh *shard) removeLiveLocked(e *cacheEntry) {
	if e.lruElem != nil {
		sh.lru.Remove(e.lruElem)
		e.lruElem = nil
	}
	if cur, ok := sh.live[e.key.fid]; ok && cur == e {
		delete(sh.live, e.key.fid)
	}
}

// detachEntryLocked moves a live entry out of the LRU/live map into detached.
// An entry with no external references is returned for immediate finalization
// (after unlock); a referenced one stays in detached until its final DecrRef.
func (sh *shard) detachEntryLocked(e *cacheEntry) []*Table {
	sh.removeLiveLocked(e)
	sh.detached[e.key] = e
	if e.table != nil && e.table.ref.Load() == 0 {
		// Keep it in detached momentarily so a concurrent releaseEntry still
		// finds and removes it; the caller finalizes right after unlocking.
		return []*Table{e.table}
	}
	return nil
}

// evictIdleLocked enforces per-shard capacity from the LRU back. Evicted
// entries move to detached; zero-external-reference ones are returned for
// immediate finalization. No file I/O happens under sh.mu. A non-positive
// capacity retains everything (eviction disabled).
func (tc *TableCache) evictIdleLocked(sh *shard) []*Table {
	var idle []*Table
	if tc.capacity <= 0 {
		return idle
	}
	for sh.lru.Len() > tc.capacity {
		elem := sh.lru.Back()
		if elem == nil {
			return idle
		}
		e := elem.Value.(*cacheEntry)
		idle = append(idle, sh.detachEntryLocked(e)...)
	}
	return idle
}

// NumEntries reports how many entries the cache tracks, including detached
// entries awaiting their last external DecrRef. Intended for tests/metrics.
func (tc *TableCache) NumEntries() int {
	n := 0
	for _, sh := range tc.shards {
		sh.mu.Lock()
		n += len(sh.live) + len(sh.detached)
		sh.mu.Unlock()
	}
	return n
}

// NumOpen reports how many handles are currently retained in the LRU lists,
// i.e. the count the capacity bounds. Intended for tests/metrics.
func (tc *TableCache) NumOpen() int {
	n := 0
	for _, sh := range tc.shards {
		sh.mu.Lock()
		n += sh.lru.Len()
		sh.mu.Unlock()
	}
	return n
}
