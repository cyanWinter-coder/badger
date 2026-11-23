/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"container/heap"
	"context"
	"errors"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2/z"
)

// ErrWaterMarkClosed is returned by WaitForMark when the WaterMark has been
// stopped via its closer, so callers waiting on it can be woken up instead of
// blocking forever while the database is shutting down.
var ErrWaterMarkClosed = errors.New("Watermark has been closed")

type uint64Heap []uint64

func (u uint64Heap) Len() int            { return len(u) }
func (u uint64Heap) Less(i, j int) bool  { return u[i] < u[j] }
func (u uint64Heap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{}) { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// mark contains one of more indices, along with a done boolean to indicate the
// status of the index: begin or done. It also contains waiters, who could be
// waiting for the watermark to reach >= a certain index.
type mark struct {
	// Either this is an (index, waiter) pair or (index, done) or (indices, done).
	index   uint64
	waiter  chan struct{}
	indices []uint64
	done    bool // Set to true if the index is done.
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//  1. as many times as Begin(k) has, AND
//  2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	doneUntil atomic.Uint64
	lastIndex atomic.Uint64
	// closed is set to 1 once the closer has been signaled. Once closed, Begin
	// and Done become no-ops and WaitForMark returns ErrWaterMarkClosed.
	closed atomic.Bool
	Name   string
	markCh chan mark
	// closeCh is closed when the closer passed to Init is signaled. Senders use
	// it as an escape hatch so a full markCh can never block shutdown.
	closeCh <-chan struct{}
}

// isClosed reports whether the watermark's processing goroutine has been (or is
// being) shut down.
func (w *WaterMark) isClosed() bool {
	return w.closed.Load()
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *z.Closer) {
	w.markCh = make(chan mark, 100)
	w.closeCh = closer.HasBeenClosed()
	go w.process(closer)
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index uint64) {
	if w.isClosed() {
		return
	}
	w.lastIndex.Store(index)
	select {
	case w.markCh <- mark{index: index, done: false}:
	case <-w.closerDone():
	}
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []uint64) {
	if w.isClosed() {
		return
	}
	w.lastIndex.Store(indices[len(indices)-1])
	select {
	case w.markCh <- mark{index: 0, indices: indices, done: false}:
	case <-w.closerDone():
	}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index uint64) {
	if w.isClosed() {
		return
	}
	select {
	case w.markCh <- mark{index: index, done: true}:
	case <-w.closerDone():
	}
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []uint64) {
	if w.isClosed() {
		return
	}
	select {
	case w.markCh <- mark{index: 0, indices: indices, done: true}:
	case <-w.closerDone():
	}
}

// closerDone returns a channel that is closed when the processing goroutine is
// shutting down. WaterMark doesn't own the closer (it is shared across the
// database's background goroutines), so it keeps a reference to it through the
// channel passed to Init.
func (w *WaterMark) closerDone() <-chan struct{} {
	return w.closeCh
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() uint64 {
	return w.doneUntil.Load()
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val uint64) {
	w.doneUntil.Store(val)
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() uint64 {
	return w.lastIndex.Load()
}

// WaitForMark waits until the given index is marked as done. If the WaterMark
// is closed (typically because the database is shutting down) before the index
// is reached, ErrWaterMarkClosed is returned; callers waiting on the mark are
// woken up instead of blocking forever.
func (w *WaterMark) WaitForMark(ctx context.Context, index uint64) error {
	if w.DoneUntil() >= index {
		return nil
	}
	if w.isClosed() {
		return ErrWaterMarkClosed
	}
	waitCh := make(chan struct{})
	select {
	case w.markCh <- mark{index: index, waiter: waitCh}:
	case <-w.closerDone():
		return ErrWaterMarkClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	case <-w.closerDone():
		return ErrWaterMarkClosed
	}
}

// process is used to process the Mark channel. This is not thread-safe,
// so only run one goroutine for process. One is sufficient, because
// all goroutine ops use purely memory and cpu.
// Each index has to emit at least one begin watermark in serial order otherwise waiters
// can get blocked idefinitely. Example: We had an watermark at 100 and a waiter at 101,
// if no watermark is emitted at index 101 then waiter would get stuck indefinitely as it
// can't decide whether the task at 101 has decided not to emit watermark or it didn't get
// scheduled yet.
func (w *WaterMark) process(closer *z.Closer) {
	defer closer.Done()

	var indices uint64Heap
	// pending maps raft proposal index to the number of pending mutations for this proposal.
	pending := make(map[uint64]int)
	waiters := make(map[uint64][]chan struct{})

	heap.Init(&indices)

	processOne := func(index uint64, done bool) {
		// If not already done, then set. Otherwise, don't undo a done entry.
		prev, present := pending[index]
		if !present {
			heap.Push(&indices, index)
		}

		delta := 1
		if done {
			delta = -1
		}
		pending[index] = prev + delta

		// Update mark by going through all indices in order; and checking if they have
		// been done. Stop at the first index, which isn't done.
		doneUntil := w.DoneUntil()
		if doneUntil > index {
			AssertTruef(false, "Name: %s doneUntil: %d. Index: %d", w.Name, doneUntil, index)
		}

		until := doneUntil
		loops := 0

		for len(indices) > 0 {
			min := indices[0]
			if done := pending[min]; done > 0 {
				break // len(indices) will be > 0.
			}
			// Even if done is called multiple times causing it to become
			// negative, we should still pop the index.
			heap.Pop(&indices)
			delete(pending, min)
			until = min
			loops++
		}

		if until != doneUntil {
			AssertTrue(w.doneUntil.CompareAndSwap(doneUntil, until))
		}

		notifyAndRemove := func(idx uint64, toNotify []chan struct{}) {
			for _, ch := range toNotify {
				close(ch)
			}
			delete(waiters, idx) // Release the memory back.
		}

		if until-doneUntil <= uint64(len(waiters)) {
			// Issue #908 showed that if doneUntil is close to 2^60, while until is zero, this loop
			// can hog up CPU just iterating over integers creating a busy-wait loop. So, only do
			// this path if until - doneUntil is less than the number of waiters.
			for idx := doneUntil + 1; idx <= until; idx++ {
				if toNotify, ok := waiters[idx]; ok {
					notifyAndRemove(idx, toNotify)
				}
			}
		} else {
			for idx, toNotify := range waiters {
				if idx <= until {
					notifyAndRemove(idx, toNotify)
				}
			}
		} // end of notifying waiters.
	}

	for {
		select {
		case <-closer.HasBeenClosed():
			// Mark the watermark as closed first, so concurrent Begin/Done calls
			// stop enqueueing and new WaitForMark callers fail fast.
			w.closed.Store(true)
			w.drainAndWakeup(processOne, waiters)
			return
		case mark := <-w.markCh:
			if mark.waiter != nil {
				doneUntil := w.doneUntil.Load()
				if doneUntil >= mark.index {
					close(mark.waiter)
				} else {
					ws, ok := waiters[mark.index]
					if !ok {
						waiters[mark.index] = []chan struct{}{mark.waiter}
					} else {
						waiters[mark.index] = append(ws, mark.waiter)
					}
				}
			} else {
				// it is possible that mark.index is zero. We need to handle that case as well.
				if mark.index > 0 || (mark.index == 0 && len(mark.indices) == 0) {
					processOne(mark.index, mark.done)
				}
				for _, index := range mark.indices {
					processOne(index, mark.done)
				}
			}
		}
	}
}

// drainAndWakeup processes any marks already buffered in markCh (so indices
// that completed right before shutdown can still advance doneUntil) and then
// wakes up every blocked waiter. No waiter must outlive the processing
// goroutine: otherwise its channel would never be closed and the goroutine
// blocked in WaitForMark would leak.
func (w *WaterMark) drainAndWakeup(
	processOne func(index uint64, done bool),
	waiters map[uint64][]chan struct{},
) {
	for {
		select {
		case mark := <-w.markCh:
			if mark.waiter != nil {
				if w.doneUntil.Load() >= mark.index {
					close(mark.waiter)
				} else {
					waiters[mark.index] = append(waiters[mark.index], mark.waiter)
				}
			} else {
				if mark.index > 0 || (mark.index == 0 && len(mark.indices) == 0) {
					processOne(mark.index, mark.done)
				}
				for _, index := range mark.indices {
					processOne(index, mark.done)
				}
			}
		default:
			for _, toNotify := range waiters {
				for _, ch := range toNotify {
					close(ch)
				}
			}
			return
		}
	}
}
