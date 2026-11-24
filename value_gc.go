/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// Recoverable value-log GC commit protocol.
//
// A rewrite batch moves live values out of one source vlog file into whatever
// vlog files are currently writable (the "target" files). Historically the
// batch had no on-disk commit record: a crash between writing the rewritten
// values and deleting the source left no way to tell whether a partial target
// file held a complete, usable value log or merely uncommitted fragments, and
// the discard state could be consumed twice on restart.
//
// The protocol below makes every batch an all-or-nothing transaction:
//
//  1. Before write-back starts, the bounds of every value the batch is about
//     to relocate are recorded into a gcTracker attached to the write path.
//  2. Once all rewritten values have landed, every target vlog is fsynced and
//     its generational boundary (the byte range the batch occupied and the
//     number of GC records inside it) is verified by re-reading the file.
//  3. Only then is a checkpoint atomically published (write + fsync + rename +
//     directory fsync). The source file and its discard entry are retired
//     after the publish, never before, so the old values stay readable for
//     every in-flight reader.
//  4. On reopen, an unpublished (prepared) checkpoint is discarded together
//     with any temp residue; the source is simply re-GCed later. A published
//     (committed) checkpoint is re-verified against the target files: a target
//     that no longer reaches its recorded boundary is reported as corruption
//     instead of being silently treated as a complete value log.

const (
	// gcCheckpointMagic lets an opener recognize and validate checkpoint files.
	gcCheckpointMagic uint32 = 0x47434350 // "GCCP"
	gcCheckpointVer   uint16 = 1

	// gcStatePrepared: all new locations are durable and verified, but the
	// checkpoint has not been atomically published yet. A checkpoint found in
	// this state after restart means the batch never committed and must be
	// rolled back.
	gcStatePrepared uint8 = 1
	// gcStateCommitted: the checkpoint was published; source retirement has to
	// be completed (or confirmed) on restart.
	gcStateCommitted uint8 = 2
)

// Crash-injection phases, observed by the test-only vlogGCCrashHook. They mark
// every ordering boundary a real crash could land at, so restart behavior can
// be exercised deterministically.
const (
	gcPhaseBeforeWriteback = iota // tracker installed, nothing relocated yet
	gcPhaseAfterWriteback         // entries relocated, target bytes not yet synced
	gcPhaseAfterSync              // targets + WAL fsynced, no checkpoint published yet
	gcPhaseAfterCheckpoint        // committed checkpoint published, source still present
)

// gcCrashInProgress is returned by rewrite tests through the crash hook to
// abort the batch at a chosen phase without it looking like an unrelated error.
var errGCCrashInjected = errors.New("injected gc crash")

// gcTargetBounds is the verifiable generational boundary a single rewrite
// batch leaves inside one target vlog file.
type gcTargetBounds struct {
	Fid      uint32
	StartOff uint32 // first offset written by this batch
	EndOff   uint32 // one past the last byte written by this batch
	Records  uint32 // number of GC entries that landed in this file
	// TotalRecords is the number of vlog entries contained in [StartOff, EndOff)
	// at commit time. Foreground traffic can interleave its own entries between
	// GC entries, so this can exceed Records. Re-verification walks the range
	// and must see exactly TotalRecords entries ending precisely at EndOff.
	TotalRecords uint32
}

// gcCheckpoint is the on-disk commit record for one rewrite batch.
type gcCheckpoint struct {
	Generation uint64 // monotonically increasing batch id, unique per DB life
	SourceFid  uint32 // vlog file whose live values were rewritten
	State      uint8
	Targets    []gcTargetBounds
}

func gcCheckpointPath(dirPath string, fid uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("VLOGGC-%06d", fid))
}

func gcCheckpointTempPath(dirPath string, fid uint32) string {
	return gcCheckpointPath(dirPath, fid) + ".tmp"
}

// encode serializes the checkpoint. Layout:
//
//	magic(4) | version(2) | state(1) | generation(8) | sourceFid(4) |
//	numTargets(4) | targets[ n * (4+4+4+4+4) ] | crc32(4, over all preceding bytes)
func (cp *gcCheckpoint) encode() []byte {
	n := len(cp.Targets)
	buf := make([]byte, 4+2+1+8+4+4+n*20+4)
	off := 0
	binary.BigEndian.PutUint32(buf[off:], gcCheckpointMagic)
	off += 4
	binary.BigEndian.PutUint16(buf[off:], gcCheckpointVer)
	off += 2
	buf[off] = cp.State
	off++
	binary.BigEndian.PutUint64(buf[off:], cp.Generation)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], cp.SourceFid)
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(n))
	off += 4
	for _, t := range cp.Targets {
		binary.BigEndian.PutUint32(buf[off:], t.Fid)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], t.StartOff)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], t.EndOff)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], t.Records)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], t.TotalRecords)
		off += 4
	}
	crc := crc32.Checksum(buf[:off], y.CastagnoliCrcTable)
	binary.BigEndian.PutUint32(buf[off:], crc)
	return buf
}

// decodeCheckpoint parses and validates a checkpoint file's contents.
func decodeCheckpoint(data []byte) (*gcCheckpoint, error) {
	if len(data) < 23 {
		return nil, fmt.Errorf("gc checkpoint too short: %d bytes", len(data))
	}
	off := 0
	if magic := binary.BigEndian.Uint32(data[off:]); magic != gcCheckpointMagic {
		return nil, fmt.Errorf("gc checkpoint has bad magic: %x", magic)
	}
	off += 4
	if ver := binary.BigEndian.Uint16(data[off:]); ver != gcCheckpointVer {
		return nil, fmt.Errorf("gc checkpoint unsupported version: %d", ver)
	}
	off += 2
	cp := &gcCheckpoint{State: data[off]}
	off++
	cp.Generation = binary.BigEndian.Uint64(data[off:])
	off += 8
	cp.SourceFid = binary.BigEndian.Uint32(data[off:])
	off += 4
	n := binary.BigEndian.Uint32(data[off:])
	off += 4
	wantLen := off + int(n)*20 + 4
	if len(data) != wantLen {
		return nil, fmt.Errorf("gc checkpoint length mismatch: got %d want %d", len(data), wantLen)
	}
	cp.Targets = make([]gcTargetBounds, 0, n)
	for i := uint32(0); i < n; i++ {
		t := gcTargetBounds{
			Fid:          binary.BigEndian.Uint32(data[off:]),
			StartOff:     binary.BigEndian.Uint32(data[off+4:]),
			EndOff:       binary.BigEndian.Uint32(data[off+8:]),
			Records:      binary.BigEndian.Uint32(data[off+12:]),
			TotalRecords: binary.BigEndian.Uint32(data[off+16:]),
		}
		off += 20
		cp.Targets = append(cp.Targets, t)
	}
	wantCrc := binary.BigEndian.Uint32(data[off:])
	if got := crc32.Checksum(data[:off], y.CastagnoliCrcTable); got != wantCrc {
		return nil, fmt.Errorf("gc checkpoint checksum mismatch")
	}
	if cp.State != gcStatePrepared && cp.State != gcStateCommitted {
		return nil, fmt.Errorf("gc checkpoint has unknown state: %d", cp.State)
	}
	return cp, nil
}

// persist writes the checkpoint to a temp file and, when state is committed,
// publishes it atomically over the final path. A prepared checkpoint is left
// under its temp name so that a crash before publish can never be mistaken for
// a committed batch.
func (vlog *valueLog) persistCheckpoint(cp *gcCheckpoint) error {
	finalPath := gcCheckpointPath(vlog.dirPath, cp.SourceFid)
	tmpPath := gcCheckpointTempPath(vlog.dirPath, cp.SourceFid)

	data := cp.encode()
	if err := os.WriteFile(tmpPath, data, 0666); err != nil {
		return y.Wrapf(err, "while writing gc checkpoint: %s", tmpPath)
	}
	f, err := os.OpenFile(tmpPath, os.O_RDONLY, 0)
	if err != nil {
		return y.Wrapf(err, "while opening gc checkpoint for sync: %s", tmpPath)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return y.Wrapf(err, "while syncing gc checkpoint: %s", tmpPath)
	}
	if err := f.Close(); err != nil {
		return y.Wrapf(err, "while closing gc checkpoint: %s", tmpPath)
	}

	if cp.State != gcStateCommitted {
		return nil
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return y.Wrapf(err, "while publishing gc checkpoint: %s", finalPath)
	}
	// The rename must survive a power loss, otherwise a committed batch could
	// look uncommitted after restart.
	if err := z.SyncDir(vlog.dirPath); err != nil {
		return y.Wrapf(err, "while syncing value dir after gc checkpoint publish")
	}
	return nil
}

// removeCheckpoint deletes any checkpoint (published or temp) for the source
// and syncs the directory so the removal is itself durable. Missing files are
// not an error: retirement may already have completed before a crash.
func (vlog *valueLog) removeCheckpoint(sourceFid uint32) error {
	for _, p := range []string{
		gcCheckpointPath(vlog.dirPath, sourceFid),
		gcCheckpointTempPath(vlog.dirPath, sourceFid),
	} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return y.Wrapf(err, "while removing gc checkpoint: %s", p)
		}
	}
	if err := z.SyncDir(vlog.dirPath); err != nil {
		return y.Wrapf(err, "while syncing value dir after gc checkpoint removal")
	}
	return nil
}

// loadCheckpoint reads and validates the published checkpoint, if any.
func (vlog *valueLog) loadCheckpoint(sourceFid uint32) (*gcCheckpoint, error) {
	data, err := os.ReadFile(gcCheckpointPath(vlog.dirPath, sourceFid))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, y.Wrapf(err, "while reading gc checkpoint for fid %d", sourceFid)
	}
	cp, err := decodeCheckpoint(data)
	if err != nil {
		return nil, y.Wrapf(err, "while decoding gc checkpoint for fid %d", sourceFid)
	}
	return cp, nil
}

// listGCCheckpointFids returns the source fids referenced by checkpoint files
// and temp checkpoint files left in the value directory.
func (vlog *valueLog) listGCCheckpointFids() ([]uint32, error) {
	entries, err := os.ReadDir(vlog.dirPath)
	if err != nil {
		return nil, y.Wrapf(err, "while scanning for gc checkpoints")
	}
	var fids []uint32
	seen := make(map[uint32]struct{})
	for _, ent := range entries {
		name := ent.Name()
		var rest string
		switch {
		case strings.HasPrefix(name, "VLOGGC-"):
			rest = strings.TrimPrefix(name, "VLOGGC-")
		default:
			continue
		}
		rest = strings.TrimSuffix(rest, ".tmp")
		var fid uint64
		if _, err := fmt.Sscanf(rest, "%06d", &fid); err != nil {
			// A name that does not parse is not a checkpoint we own; leave it alone.
			continue
		}
		if _, dup := seen[uint32(fid)]; dup {
			continue
		}
		seen[uint32(fid)] = struct{}{}
		fids = append(fids, uint32(fid))
	}
	return fids, nil
}

// recoverGCBatches reconciles every checkpoint left behind by a crashed run.
//
// Prepared (temp) checkpoints describe batches that never reached the atomic
// publish: their target fragments are uncommitted residue and are ignored, the
// temp files removed, and the source's discard state is left untouched so the
// file can be picked for GC again.
//
// Committed checkpoints describe batches whose new locations were durable and
// boundary-verified. Each target boundary is re-verified here: a target whose
// file no longer spans the recorded range means a truncated file would
// otherwise masquerade as a complete value log, so recovery fails loudly. When
// all targets check out, the source is retired and its discard entry cleared.
func (vlog *valueLog) recoverGCBatches() error {
	fids, err := vlog.listGCCheckpointFids()
	if err != nil {
		return err
	}
	for _, sourceFid := range fids {
		if _, err := os.Stat(gcCheckpointTempPath(vlog.dirPath, sourceFid)); err == nil {
			// Temp residue means the atomic publish of *some* batch never
			// happened. Discard it and keep going: an older committed
			// checkpoint for the same source fid may still be published and
			// must be reconciled below rather than skipped.
			vlog.opt.Warningf("Found uncommitted gc checkpoint residue for fid %d; discarding",
				sourceFid)
			if err := os.Remove(gcCheckpointTempPath(vlog.dirPath, sourceFid)); err != nil &&
				!os.IsNotExist(err) {
				return y.Wrapf(err, "while removing uncommitted gc checkpoint for fid %d", sourceFid)
			}
			if err := z.SyncDir(vlog.dirPath); err != nil {
				return y.Wrapf(err, "while syncing value dir after gc residue cleanup")
			}
		}

		cp, err := vlog.loadCheckpoint(sourceFid)
		if err != nil {
			return err
		}
		if cp == nil {
			// Only temp residue existed and was just cleaned up.
			continue
		}
		if cp.State == gcStatePrepared {
			// A prepared record can only exist under the temp name in normal
			// operation. Finding one at the published path means an interrupted
			// rename; treat it as uncommitted.
			vlog.opt.Warningf("Found prepared gc checkpoint at published path for fid %d; discarding",
				sourceFid)
			if err := vlog.removeCheckpoint(sourceFid); err != nil {
				return err
			}
			continue
		}

		// Committed batch: the values' new homes must still be intact. This is
		// the guard against treating a truncated target as a complete vlog.
		if err := vlog.verifyGCBounds(cp.Targets); err != nil {
			return y.Wrapf(err, "while recovering committed gc batch (gen %d, source fid %d)",
				cp.Generation, cp.SourceFid)
		}
		if err := vlog.retireRecoveredSource(cp.SourceFid); err != nil {
			return err
		}
	}
	return nil
}

// retireRecoveredSource finishes the committed batch's source retirement after
// a restart: remove it from the file map (or mark it pending if iterators are
// somehow active), reset its discard stats and consume the checkpoint.
func (vlog *valueLog) retireRecoveredSource(sourceFid uint32) error {
	vlog.filesLock.Lock()
	lf, ok := vlog.filesMap[sourceFid]
	if !ok {
		// Source already gone (e.g. delete persisted but checkpoint removal did
		// not). Just consume the leftover checkpoint.
		vlog.filesLock.Unlock()
		vlog.discardStats.Update(sourceFid, -1)
		return vlog.removeCheckpoint(sourceFid)
	}
	if vlog.iteratorCount() == 0 {
		delete(vlog.filesMap, sourceFid)
		vlog.filesLock.Unlock()
	} else {
		vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, sourceFid)
		vlog.filesLock.Unlock()
		// Pending deletion: decrIteratorCount removes the file and checkpoint.
		return nil
	}

	if err := vlog.deleteLogFile(lf); err != nil {
		return err
	}
	return vlog.removeCheckpoint(sourceFid)
}
