/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

// readValue returns the string value of a key within the given transaction, or
// "" when the key is not visible.
func readValue(t *testing.T, txn *Txn, key string) string {
	t.Helper()
	item, err := txn.Get([]byte(key))
	if errors.Is(err, ErrKeyNotFound) {
		return ""
	}
	require.NoError(t, err)
	var val string
	require.NoError(t, item.Value(func(v []byte) error {
		val = string(v)
		return nil
	}))
	return val
}

func commitKV(t *testing.T, db *DB, key, val string) {
	t.Helper()
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte(key), []byte(val))
	}))
}

// TestTxnSnapshotReadTsIsFixed verifies requirement 1: all reads inside a
// transaction are served from the same snapshot (fixed readTs), including
// repeated reads of a key while concurrent writers commit newer versions.
func TestTxnSnapshotReadTsIsFixed(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitKV(t, db, "k", "v0")

		txn := db.NewTransaction(true)
		defer txn.Discard()
		snapTs := txn.ReadTs()
		require.NotZero(t, snapTs)

		// Repeated reads before any external change return the same version.
		for i := 0; i < 5; i++ {
			require.Equal(t, "v0", readValue(t, txn, "k"))
			require.Equal(t, snapTs, txn.ReadTs())
		}

		// Writers commit newer versions while the transaction is still open.
		commitKV(t, db, "k", "v1")
		commitKV(t, db, "k", "v2")

		// The long-running transaction must keep observing its original snapshot.
		for i := 0; i < 5; i++ {
			require.Equal(t, "v0", readValue(t, txn, "k"))
			require.Equal(t, snapTs, txn.ReadTs())
		}

		// A fresh transaction observes the latest committed value.
		require.NoError(t, db.View(func(fresh *Txn) error {
			require.Equal(t, "v2", readValue(t, fresh, "k"))
			require.Greater(t, fresh.ReadTs(), snapTs)
			return nil
		}))
	})
}

// TestTxnConcurrentReadWriteIsolation runs many reader/writer goroutines and
// asserts every reader sees a self-consistent snapshot for its whole lifetime.
func TestTxnConcurrentReadWriteIsolation(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		const writers = 8
		const readers = 8
		const iterations = 50

		var wg sync.WaitGroup
		errCh := make(chan error, writers+readers)

		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					err := db.Update(func(txn *Txn) error {
						return txn.Set([]byte("shared"), []byte(fmt.Sprintf("w%d-i%d", id, i)))
					})
					if err != nil {
						errCh <- err
						return
					}
				}
			}(w)
		}

		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					txn := db.NewTransaction(false)
					snapTs := txn.ReadTs()
					first := readValue(t, txn, "shared")
					for j := 0; j < 5; j++ {
						if got := readValue(t, txn, "shared"); got != first {
							errCh <- fmt.Errorf("snapshot drifted: %q -> %q", first, got)
							txn.Discard()
							return
						}
						if txn.ReadTs() != snapTs {
							errCh <- fmt.Errorf("readTs changed: %d -> %d", snapTs, txn.ReadTs())
							txn.Discard()
							return
						}
					}
					txn.Discard()
				}
			}()
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}
	})
}

// TestTxnConflictAtCommitNoPartialWrites verifies requirement 2: conflicts are
// decided only at commit time and a conflicting transaction leaves none of its
// writes visible.
func TestTxnConflictAtCommitNoPartialWrites(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitKV(t, db, "balance", "100")

		t1 := db.NewTransaction(true)
		t2 := db.NewTransaction(true)
		defer t1.Discard()
		defer t2.Discard()

		// Both transactions read the same key, establishing a read/write conflict.
		require.Equal(t, "100", readValue(t, t1, "balance"))
		require.Equal(t, "100", readValue(t, t2, "balance"))

		require.NoError(t, t1.Set([]byte("balance"), []byte("110")))
		require.NoError(t, t2.Set([]byte("balance"), []byte("120")))
		// t2 also writes a second key that must never become visible on conflict.
		require.NoError(t, t2.Set([]byte("t2-only"), []byte("partial")))

		require.NoError(t, t1.Commit())
		err := t2.Commit()
		require.ErrorIs(t, err, ErrConflict)

		require.NoError(t, db.View(func(txn *Txn) error {
			require.Equal(t, "110", readValue(t, txn, "balance"))
			_, err := txn.Get([]byte("t2-only"))
			require.ErrorIs(t, err, ErrKeyNotFound)
			return nil
		}))
	})
}

// TestTxnConflictRetrySucceeds models the standard retry loop: a conflicting
// transaction is retried with a fresh snapshot and eventually commits.
func TestTxnConflictRetrySucceeds(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitKV(t, db, "counter", "0")

		// txnA takes a snapshot and reads the counter.
		txnA := db.NewTransaction(true)
		defer txnA.Discard()
		require.Equal(t, "0", readValue(t, txnA, "counter"))

		// A concurrent writer commits a new version after txnA's snapshot.
		commitKV(t, db, "counter", "1")

		// txnA's first commit must conflict (it read a key that changed).
		require.NoError(t, txnA.Set([]byte("counter"), []byte("0-A")))
		require.ErrorIs(t, txnA.Commit(), ErrConflict)

		// Retry with a fresh transaction that reads the committed value first.
		attempts := 0
		for {
			attempts++
			err := db.Update(func(txn *Txn) error {
				cur := readValue(t, txn, "counter")
				return txn.Set([]byte("counter"), []byte(cur+"+final"))
			})
			if errors.Is(err, ErrConflict) {
				continue
			}
			require.NoError(t, err)
			break
		}
		require.GreaterOrEqual(t, attempts, 1)

		require.NoError(t, db.View(func(txn *Txn) error {
			require.Equal(t, "1+final", readValue(t, txn, "counter"))
			return nil
		}))
	})
}

// TestTxnBlindWriteNoConflict checks that write-only transactions (no reads)
// never report conflicts, matching SSI semantics.
func TestTxnBlindWriteNoConflict(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		const n = 20
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func(i int) {
				defer wg.Done()
				txn := db.NewTransaction(true)
				defer txn.Discard()
				require.NoError(t, txn.Set([]byte("blind"), []byte(fmt.Sprintf("%d", i))))
				require.NoError(t, txn.Commit())
			}(i)
		}
		wg.Wait()

		require.NoError(t, db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("blind"))
			return err
		}))
	})
}

// TestIteratorPinsSnapshotAndReleasesOnClose verifies requirement 3: an active
// iterator holds references on the memtables/tables it reads (so versions in
// its snapshot cannot be reclaimed), and closing it releases those references;
// discarding the transaction then releases the read-watermark pin.
func TestIteratorPinsSnapshotAndReleasesOnClose(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitKV(t, db, "k", "v0")

		oldTxn := db.NewTransaction(false)
		oldTs := oldTxn.ReadTs()
		opts := DefaultIteratorOptions
		opts.PrefetchValues = false
		it := oldTxn.NewIterator(opts)
		it.Rewind()
		require.True(t, it.Valid())

		// With the iterator open, the txn reports an active iterator.
		require.Equal(t, int32(1), oldTxn.numIterators.Load())

		it.Close()
		// Repeated Close must be idempotent and the count returns to zero.
		require.NotPanics(t, func() { it.Close() })
		require.Equal(t, int32(0), oldTxn.numIterators.Load())

		// The txn still pins the read watermark until Discard, even though its
		// iterator was closed. Newer commits must not advance reclamation past
		// the open snapshot.
		commitKV(t, db, "k", "v1")
		commitKV(t, db, "k", "v2")
		require.LessOrEqual(t, db.orc.discardAtOrBelow(), oldTs,
			"open txn should keep version reclamation at or below its readTs")

		oldTxn.Discard()
		require.Eventually(t, func() bool {
			return db.orc.readMark.DoneUntil() > oldTs
		}, 2*time.Second, 10*time.Millisecond,
			"discarding the txn should release the read watermark pin")
	})
}

// TestTxnDiscardIdempotent verifies requirement 4: repeated Discard calls are
// safe no-ops and the read watermark is released exactly once.
func TestTxnDiscardIdempotent(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		commitKV(t, db, "k", "v0")
		txn := db.NewTransaction(true)
		require.Equal(t, "v0", readValue(t, txn, "k"))
		ts := txn.ReadTs()

		require.NotPanics(t, func() {
			txn.Discard()
			txn.Discard()
			txn.Discard()
		})
		require.True(t, txn.discarded)
		require.True(t, txn.doneRead)

		// Operations after discard keep returning ErrDiscardedTxn.
		_, err := txn.Get([]byte("k"))
		require.ErrorIs(t, err, ErrDiscardedTxn)
		err = txn.Set([]byte("k"), []byte("x"))
		require.ErrorIs(t, err, ErrDiscardedTxn)

		require.Eventually(t, func() bool {
			return db.orc.discardAtOrBelow() >= ts
		}, 2*time.Second, 10*time.Millisecond)
	})
}

// TestDBCloseWakesWatermarkWaiters verifies requirement 4: closing the DB wakes
// up goroutines blocked acquiring a transaction snapshot instead of leaking
// them, and closing twice is idempotent.
func TestDBCloseWakesWatermarkWaiters(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)

	// Pin a begin watermark above the current mark, then register waiters for an
	// index that will never complete while the oracle is alive.
	orc := db.orc
	orc.txnMark.Begin(orc.nextTs() + 10)

	const n = 4
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = orc.txnMark.WaitForMark(t.Context(), orc.nextTs()+10)
		}(i)
	}

	// Let the waiters register before closing.
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, db.Close())
	wg.Wait()
	for i, err := range errs {
		require.ErrorIs(t, err, y.ErrWaterMarkClosed, "waiter %d not woken", i)
	}

	// Repeated Close must be idempotent.
	require.NoError(t, db.Close())
	require.NoError(t, db.Close())
}

// TestNewTransactionAfterCloseReturnsSafeTxn ensures transactions created
// during/after close are non-nil and fail with ErrDBClosed rather than panicking.
func TestNewTransactionAfterCloseReturnsSafeTxn(t *testing.T) {
	dir := t.TempDir()
	opts := getTestOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	for _, update := range []bool{false, true} {
		txn := db.NewTransaction(update)
		require.NotNil(t, txn)
		_, err := txn.Get([]byte("k"))
		require.ErrorIs(t, err, ErrDBClosed)
		if update {
			// A write attempt must be rejected without touching the closed DB.
			require.ErrorIs(t, txn.Set([]byte("k"), []byte("v")), ErrDBClosed)
			// Committing an empty txn is a no-op (just discards); a txn with
			// pending writes cannot be built after close, so the precheck error
			// path is exercised above via Set.
		}
		require.NotPanics(t, func() { txn.Discard() })
		require.NotPanics(t, func() { txn.Discard() })

		// Operations after discard keep reporting ErrDiscardedTxn.
		_, err = txn.Get([]byte("k"))
		require.ErrorIs(t, err, ErrDiscardedTxn)
	}
}
