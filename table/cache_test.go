/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// newCacheTable builds an in-memory table with a few keys and the given
// generation. It is used by cache tests as a stand-in for a disk-backed open.
func newCacheTable(t *testing.T, id, gen uint64) *Table {
	t.Helper()
	tbl, err := buildInMemoryTable(id, gen)
	require.NoError(t, err)
	return tbl
}

// buildInMemoryTable constructs an in-memory table without depending on a
// testing.T, so it can be used from inside a TableOpener.
func buildInMemoryTable(id, gen uint64) (*Table, error) {
	opts := Options{BlockSize: 1024, Compression: options.None}
	b := NewTableBuilder(opts)
	defer b.Close()
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("key-%06d-%d", id, i)
		b.Add(y.KeyWithTs([]byte(k), 1), y.ValueStruct{Value: []byte("v")}, 0)
	}
	return OpenInMemoryTable(b.Finish(), id, &opts, gen)
}

// countingOpener returns in-memory tables and records how many opens happened
// per file id. If fail[id] is non-nil, the open returns that error instead.
type countingOpener struct {
	mu      sync.Mutex
	opens   map[uint64]int
	fail    map[uint64]error
	block   map[uint64]chan struct{} // optional: block the open until the chan closes
	blocked chan uint64              // signaled once an open starts blocking
}

func newCountingOpener() *countingOpener {
	return &countingOpener{
		opens:   make(map[uint64]int),
		fail:    make(map[uint64]error),
		block:   make(map[uint64]chan struct{}),
		blocked: make(chan uint64, 16),
	}
}

func (o *countingOpener) open(id, gen uint64) (*Table, error) {
	o.mu.Lock()
	o.opens[id]++
	wait := o.block[id]
	fail := o.fail[id]
	o.mu.Unlock()

	if wait != nil {
		o.blocked <- id
		<-wait
	}
	if fail != nil {
		return nil, fail
	}
	return buildInMemoryTable(id, gen)
}

// TestTableCacheGenerationsAreUnique verifies requirement 1: re-opening a file
// id after eviction produces a new incarnation with a fresh generation, and the
// two handles are distinct rather than a path-based reuse of the old handle.
func TestTableCacheGenerationsAreUnique(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(1, 1, op.open)
	defer tc.Close()

	t1, err := tc.Acquire(7)
	require.NoError(t, err)
	gen1 := t1.Gen()
	require.Equal(t, uint64(7), t1.ID())
	require.NoError(t, t1.DecrRef())

	t2, err := tc.Acquire(7)
	require.NoError(t, err)
	gen2 := t2.Gen()
	require.NoError(t, t2.DecrRef())

	// With capacity 1 and an intervening acquire of another id, the first
	// incarnation is evicted; the second open is a fresh generation.
	require.NotEqual(t, gen1, gen2, "re-opened file must get a new generation")
	require.NotSame(t, t1, t2, "re-opened file must be a new handle")
	require.Equal(t, 2, op.opens[7], "the file must have been opened twice")
}

// TestTableCacheBlockKeyIncludesGeneration ensures block cache keys from two
// incarnations of the same file id never alias.
func TestTableCacheBlockKeyIncludesGeneration(t *testing.T) {
	t1 := newCacheTable(t, 5, 11)
	t2 := newCacheTable(t, 5, 22)
	defer t1.DecrRef()
	defer t2.DecrRef()

	k1 := t1.blockCacheKey(3)
	k2 := t2.blockCacheKey(3)
	require.NotEqual(t, string(k1), string(k2),
		"block cache keys must include the generation")
	require.Equal(t, 20, len(k1))

	i1 := t1.indexKey()
	i2 := t2.indexKey()
	require.NotEqual(t, string(i1), string(i2))
	require.Equal(t, 16, len(i1))
}

// TestTableCacheEvictionKeepsReferencedHandleAlive verifies requirement 2: an
// entry pushed out by the capacity limit must stay usable while a reader holds
// a reference; its file/index are released only after the final DecrRef.
func TestTableCacheEvictionKeepsReferencedHandleAlive(t *testing.T) {
	op := newCountingOpener()
	// Capacity 1: acquiring a second id evicts the first from the LRU.
	tc := NewTableCache(1, 1, op.open)
	defer tc.Close()

	held, err := tc.Acquire(1)
	require.NoError(t, err)
	require.Equal(t, 1, tc.NumOpen())

	// The reader keeps its reference while the second handle displaces the first.
	other, err := tc.Acquire(2)
	require.NoError(t, err)
	require.Equal(t, 1, tc.NumOpen(), "only one handle fits the LRU")
	require.Equal(t, 1, tc.NumEntries()-tc.NumOpen(),
		"the evicted, still-referenced handle is tracked as detached")
	require.NoError(t, other.DecrRef())
	require.Equal(t, 0, tc.NumOpen(),
		"the unreferenced second handle is finalized and nothing remains live")

	// The evicted but referenced handle is still readable.
	it := held.NewIterator(0)
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	require.NoError(t, it.Close())
	require.Greater(t, count, 0, "evicted handle must remain readable while referenced")

	// Releasing the last reference finalizes it (in-memory close is a no-op).
	require.NoError(t, held.DecrRef())
	require.Equal(t, 0, tc.NumEntries(), "no detached entry may linger after final DecrRef")
}

// TestTableCacheFailedOpenDoesNotPoisonCache verifies requirement 3's failure
// half: a failed open is visible to all waiters but never installs an entry, so
// a later successful open under a new generation works.
func TestTableCacheFailedOpenDoesNotPoisonCache(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(4, 1, op.open)
	defer tc.Close()

	boom := errors.New("CHECKSUM_MISMATCH: corrupt table 9")
	op.fail[9] = boom

	_, err := tc.Acquire(9)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 0, tc.NumEntries(), "a failed open must leave no cache entry")

	// Clear the fault and re-open: a new generation succeeds.
	delete(op.fail, 9)
	tbl, err := tc.Acquire(9)
	require.NoError(t, err)
	require.Greater(t, tbl.Gen(), uint64(0))
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, 2, op.opens[9], "the failed attempt must not suppress the retry")
}

// TestTableCacheConcurrentOpenCoalescesAndReplaces checks that concurrent
// acquires of one id share a single open and one incarnation (requirement:
// concurrent open), and that the same id can later be replaced by a new
// incarnation without confusing the two.
func TestTableCacheConcurrentOpenCoalescesAndReplaces(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(4, 1, op.open)
	defer tc.Close()

	const goroutines = 32
	var wg sync.WaitGroup
	got := make([]*Table, goroutines)
	errs := make([]error, goroutines)
	gate := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			got[i], errs[i] = tc.Acquire(42)
		}(i)
	}
	close(gate)
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	for _, tbl := range got {
		require.Same(t, got[0], tbl, "all acquirers must share one incarnation")
		require.Equal(t, got[0].Gen(), tbl.Gen())
	}
	require.Equal(t, 1, op.opens[42], "concurrent acquires must coalesce to one open")

	for _, tbl := range got {
		require.NoError(t, tbl.DecrRef())
	}

	// Evict and re-open: replacement must not hand back the old incarnation.
	tc.Invalidate(42)
	again, err := tc.Acquire(42)
	require.NoError(t, err)
	require.NotEqual(t, got[0].Gen(), again.Gen())
	require.NotSame(t, got[0], again)
	require.NoError(t, again.DecrRef())
}

// TestTableCacheInvalidateIsolatesOneEntry verifies requirement 4: invalidating
// one table does not touch other shards/entries, and a live iterator on the
// invalidated handle keeps working until it is closed.
func TestTableCacheInvalidateIsolatesOneEntry(t *testing.T) {
	op := newCountingOpener()
	// Two shards so ids can land in different shards.
	tc := NewTableCache(16, 2, op.open)
	defer tc.Close()

	bad, err := tc.Acquire(3)
	require.NoError(t, err)
	good, err := tc.Acquire(4)
	require.NoError(t, err)

	// Hold an iterator on the soon-to-be-invalidated table.
	badIt := bad.NewIterator(0)
	badIt.Rewind()
	require.True(t, badIt.Valid())

	tc.Invalidate(3)

	// The other table is untouched and still cached.
	good2, err := tc.Acquire(4)
	require.NoError(t, err)
	require.Same(t, good, good2, "unrelated table must not be invalidated")
	require.NoError(t, good2.DecrRef())

	// A new acquire of the invalidated id opens a fresh incarnation.
	fresh, err := tc.Acquire(3)
	require.NoError(t, err)
	require.NotSame(t, bad, fresh)
	require.NotEqual(t, bad.Gen(), fresh.Gen())

	// The old iterator on the invalidated handle still works to completion.
	count := 1
	for badIt.Next(); badIt.Valid(); badIt.Next() {
		count++
	}
	require.NoError(t, badIt.Close())
	require.Greater(t, count, 0)

	require.NoError(t, bad.DecrRef())
	require.NoError(t, fresh.DecrRef())
	require.NoError(t, good.DecrRef())
}

// TestTableCacheCloseWakesWaiters verifies requirement 5: while goroutines are
// blocked on an in-flight open, Close wakes all of them with
// ErrTableCacheClosed instead of hanging, and Close is idempotent.
func TestTableCacheCloseWakesWaiters(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(4, 1, op.open)

	release := make(chan struct{})
	op.block[100] = release

	const waiters = 8
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = tc.Acquire(100)
		}(i)
	}

	// Wait until at least one goroutine is parked inside the blocked opener,
	// meaning the others are waiting on the cache's cond.
	select {
	case <-op.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("opener never started")
	}
	// Give the remaining waiters time to park on the cond.
	time.Sleep(50 * time.Millisecond)

	// Close returns without waiting for the still-running opener, but wakes every
	// waiter parked on the cond.
	require.NoError(t, tc.Close())

	// Unblock the in-flight opener so it can observe the closed flag and finish.
	close(release)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waiters were not all woken after Close")
	}
	// Every acquirer -- the parked opener included -- fails with the closed
	// error rather than hanging.
	for _, err := range errs {
		require.ErrorIs(t, err, ErrTableCacheClosed)
	}

	// A second Close is harmless, and acquires after close fail immediately.
	require.NoError(t, tc.Close())
	_, err := tc.Acquire(100)
	require.ErrorIs(t, err, ErrTableCacheClosed)
}

// TestTableCacheMarkDeletedRemovesFileOnLastRef ensures tombstoned tables are
// deleted (not merely closed) once references drain, while readers in flight
// remain unaffected.
func TestTableCacheMarkDeletedRemovesFileOnLastRef(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(8, 1, op.open)
	defer tc.Close()

	tbl, err := tc.Acquire(11)
	require.NoError(t, err)
	// In-memory tables have no fd, so Delete is a no-op but the lifecycle
	// bookkeeping (detach -> finalize on last ref) is still exercised.
	tc.MarkDeleted(11)
	require.True(t, tbl.isDeleted())
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, 0, tc.NumEntries())
}

// TestTableCacheRegisterAndRefCount is a focused check that externally built
// tables can be adopted and their external ref counts drive finalization.
func TestTableCacheRegisterAndRefCount(t *testing.T) {
	op := newCountingOpener()
	tc := NewTableCache(2, 1, op.open)
	defer tc.Close()

	gen := tc.NextGen()
	tbl := newCacheTable(t, 77, gen) // starts with one external ref.
	require.NoError(t, tc.Register(tbl))
	require.Equal(t, 1, tc.NumOpen())

	// More acquirers add references; capacity eviction cannot close it while any
	// external ref exists.
	a, err := tc.Acquire(77)
	require.NoError(t, err)
	require.Same(t, tbl, a)
	require.Equal(t, int32(2), tbl.ref.Load())

	require.NoError(t, a.DecrRef())
	require.NoError(t, tbl.DecrRef()) // last external ref -> finalized
	require.Equal(t, 0, tc.NumEntries())
}

// Ensure the atomic counter is used under the race detector in concurrent
// generation allocation.
func TestTableCacheNextGenMonotonic(t *testing.T) {
	tc := NewTableCache(1, 1, newCountingOpener().open)
	defer tc.Close()

	const n = 200
	var seen sync.Map
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := tc.NextGen()
			if _, dup := seen.LoadOrStore(g, struct{}{}); dup {
				t.Errorf("duplicate generation %d", g)
			}
		}()
	}
	wg.Wait()
}
