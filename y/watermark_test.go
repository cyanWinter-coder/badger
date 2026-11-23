/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
)

// TestWaterMarkWaitForMark covers the normal begin/done/wait lifecycle plus
// repeated Done calls (idempotency).
func TestWaterMarkWaitForMark(t *testing.T) {
	closer := z.NewCloser(1)
	defer closer.SignalAndWait()

	w := &WaterMark{Name: "test"}
	w.Init(closer)

	w.Begin(1)
	w.Begin(2)

	done := make(chan error, 1)
	go func() {
		done <- w.WaitForMark(context.Background(), 2)
	}()

	// The waiter must block until index 2 is marked done.
	select {
	case err := <-done:
		t.Fatalf("WaitForMark returned before marks were done: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	w.Done(1)
	// Repeated Done must not undo a completed index or panic.
	w.Done(1)
	w.Done(2)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForMark failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForMark did not return after index was done")
	}

	if got := w.DoneUntil(); got != 2 {
		t.Fatalf("DoneUntil = %d, want 2", got)
	}
}

// TestWaterMarkCloseWakesWaiters verifies that stopping the closer wakes up
// every blocked waiter with ErrWaterMarkClosed instead of leaking goroutines,
// and that WaitForMark fails fast once the watermark is closed.
func TestWaterMarkCloseWakesWaiters(t *testing.T) {
	closer := z.NewCloser(1)
	w := &WaterMark{Name: "test-close"}
	w.Init(closer)

	w.Begin(1) // index 1 stays in-flight, so waiters at index 5 cannot complete normally.

	const waiters = 8
	var wg sync.WaitGroup
	wg.Add(waiters)
	errs := make([]error, waiters)
	for i := 0; i < waiters; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.WaitForMark(context.Background(), 5)
		}(i)
	}

	// Give the waiters time to register with the processing goroutine.
	time.Sleep(100 * time.Millisecond)

	// A context-canceled waiter must also be supported alongside shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancelErr := make(chan error, 1)
	go func() {
		cancelErr <- w.WaitForMark(ctx, 5)
	}()
	cancel()

	select {
	case err := <-cancelErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("context waiter got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("context-canceled WaitForMark did not return")
	}

	closer.SignalAndWait()
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, ErrWaterMarkClosed) {
			t.Fatalf("waiter %d got %v, want ErrWaterMarkClosed", i, err)
		}
	}

	// New waiters after close must fail immediately rather than block.
	if err := w.WaitForMark(context.Background(), 5); !errors.Is(err, ErrWaterMarkClosed) {
		t.Fatalf("post-close WaitForMark got %v, want ErrWaterMarkClosed", err)
	}

	// Begin/Done after close are idempotent no-ops and must not block.
	doneCh := make(chan struct{})
	go func() {
		w.Begin(100)
		w.Done(100)
		w.BeginMany([]uint64{101, 102})
		w.DoneMany([]uint64{101, 102})
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("Begin/Done blocked after watermark close")
	}
}

// TestWaterMarkCloseDrainsBufferedMarks ensures marks buffered in the channel
// right before shutdown are still processed, advancing DoneUntil where valid.
func TestWaterMarkCloseDrainsBufferedMarks(t *testing.T) {
	closer := z.NewCloser(1)
	w := &WaterMark{Name: "test-drain"}
	w.Init(closer)

	w.Begin(1)
	w.Begin(2)
	w.Done(1)
	w.Done(2)

	// Signal immediately; the processing goroutine drains any buffered marks.
	closer.SignalAndWait()

	if got := w.DoneUntil(); got != 2 {
		t.Fatalf("DoneUntil = %d, want 2 after drain", got)
	}
}
