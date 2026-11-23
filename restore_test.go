/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/pb"
)

// buildManifestBackup creates a database in dir with n entries and returns a
// manifest-based backup of it along with the entries written.
func buildManifestBackup(t *testing.T, dir string, n int) ([]byte, []*pb.KV) {
	t.Helper()
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	entries := createEntries(n)
	require.NoError(t, populateEntries(db, entries))
	var buf bytes.Buffer
	_, err = db.BackupWithManifest(&buf, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return buf.Bytes(), entries
}

// verifyRestoredEntries checks that the database in dir contains exactly the
// given entries.
func verifyRestoredEntries(t *testing.T, dir string, entries []*pb.KV) {
	t.Helper()
	db, err := Open(getTestOptions(dir))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *Txn) error {
		for _, e := range entries {
			item, err := txn.Get(e.Key)
			if err != nil {
				return fmt.Errorf("key %q: %w", e.Key, err)
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !bytes.Equal(val, e.Value) {
				return fmt.Errorf("key %q: got value %v, want %v", e.Key, val, e.Value)
			}
		}
		return nil
	}))
}

// requireNoRestoreLeftovers checks that no staging, old or commit-marker
// state remains next to dir.
func requireNoRestoreLeftovers(t *testing.T, dir string) {
	t.Helper()
	for _, suffix := range []string{restoreStagingSuffix, restoreOldSuffix, restoreCommitSuffix} {
		_, err := os.Stat(dir + suffix)
		require.True(t, os.IsNotExist(err), "leftover restore artifact: %s", dir+suffix)
	}
}

func TestRestoreTransactional(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, entries := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 100)

	target := filepath.Join(tmpdir, "dst")
	require.NoError(t, Restore(bytes.NewReader(backup), RestoreOptions{
		Options: getTestOptions(target),
	}))
	verifyRestoredEntries(t, target, entries)
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreLegacyBackup(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	// A backup in the legacy format (no manifest) must still restore.
	db, err := Open(getTestOptions(filepath.Join(tmpdir, "src")))
	require.NoError(t, err)
	entries := createEntries(100)
	require.NoError(t, populateEntries(db, entries))
	var buf bytes.Buffer
	_, err = db.Backup(&buf, 0)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	target := filepath.Join(tmpdir, "dst")
	require.NoError(t, Restore(bytes.NewReader(buf.Bytes()), RestoreOptions{
		Options: getTestOptions(target),
	}))
	verifyRestoredEntries(t, target, entries)
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreReplacesExistingAtomically(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, entries := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 50)

	// Pre-existing database at the target with a key the backup does not have.
	target := filepath.Join(tmpdir, "dst")
	db, err := Open(getTestOptions(target))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte("stale-key"), []byte("stale")))
	}))
	require.NoError(t, db.Close())

	require.NoError(t, Restore(bytes.NewReader(backup), RestoreOptions{
		Options: getTestOptions(target),
	}))

	// The restored database must contain exactly the backup contents.
	verifyRestoredEntries(t, target, entries)
	db, err = Open(getTestOptions(target))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("stale-key"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreTruncatedBackup(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 100)

	// A pre-existing database at the target must survive every failed restore.
	target := filepath.Join(tmpdir, "dst")
	db, err := Open(getTestOptions(target))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte("original"), []byte("untouched")))
	}))
	require.NoError(t, db.Close())

	cuts := []int{
		4,               // inside the header magic
		20,              // inside the header manifest
		len(backup) / 2, // inside the data records
		len(backup) - 9, // inside the trailer manifest
	}
	for _, cut := range cuts {
		err := Restore(bytes.NewReader(backup[:cut]), RestoreOptions{
			Options: getTestOptions(target),
		})
		require.Error(t, err, "cut at %d", cut)
		require.ErrorIs(t, err, ErrBackupCorrupt, "cut at %d", cut)

		// The original database must still open and hold its data.
		db, err := Open(getTestOptions(target))
		require.NoError(t, err, "cut at %d", cut)
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte("original"))
			require.NoError(t, err)
			return item.Value(func(val []byte) error {
				require.Equal(t, []byte("untouched"), val)
				return nil
			})
		}))
		require.NoError(t, db.Close())
		requireNoRestoreLeftovers(t, target)
	}
}

func TestRestoreCorruptShard(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 100)

	// Flip a byte in the middle of the data records.
	corrupt := bytes.Clone(backup)
	corrupt[len(corrupt)/2] ^= 0xff

	target := filepath.Join(tmpdir, "dst")
	err = Restore(bytes.NewReader(corrupt), RestoreOptions{
		Options: getTestOptions(target),
	})
	require.ErrorIs(t, err, ErrBackupCorrupt)

	// The failed restore must not have created a database at the target.
	_, err = os.Stat(filepath.Join(target, ManifestFilename))
	require.True(t, os.IsNotExist(err))
	requireNoRestoreLeftovers(t, target)
}

// rewriteBackupHeader replaces the header manifest of a manifest-based
// backup, leaving the records and trailer untouched.
func rewriteBackupHeader(t *testing.T, backup []byte, mutate func(m map[string]interface{})) []byte {
	t.Helper()
	require.Equal(t, backupHeaderMagic, string(backup[:8]))
	hlen := binary.LittleEndian.Uint64(backup[8:16])
	var header map[string]interface{}
	require.NoError(t, json.Unmarshal(backup[16:16+int(hlen)], &header))
	mutate(header)
	newHeader, err := json.Marshal(header)
	require.NoError(t, err)
	var out bytes.Buffer
	out.WriteString(backupHeaderMagic)
	require.NoError(t, binary.Write(&out, binary.LittleEndian, uint64(len(newHeader))))
	out.Write(newHeader)
	out.Write(backup[16+int(hlen):])
	return out.Bytes()
}

func TestRestoreRejectsUnsupportedVersion(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 10)
	future := rewriteBackupHeader(t, backup, func(m map[string]interface{}) {
		m["format_version"] = float64(backupFormatVersion + 1)
	})

	target := filepath.Join(tmpdir, "dst")
	err = Restore(bytes.NewReader(future), RestoreOptions{
		Options: getTestOptions(target),
	})
	require.ErrorIs(t, err, ErrBackupVersion)
	// Rejected before anything was written.
	_, err = os.Stat(target)
	require.True(t, os.IsNotExist(err))
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreRejectsUnknownRequiredFeature(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 10)
	exotic := rewriteBackupHeader(t, backup, func(m map[string]interface{}) {
		m["required_features"] = []interface{}{"lz4-compression"}
	})

	target := filepath.Join(tmpdir, "dst")
	err = Restore(bytes.NewReader(exotic), RestoreOptions{
		Options: getTestOptions(target),
	})
	require.ErrorIs(t, err, ErrBackupFeature)
	_, err = os.Stat(target)
	require.True(t, os.IsNotExist(err))
	requireNoRestoreLeftovers(t, target)
}

// cancelAfterReader cancels the context once more than `after` bytes have
// been consumed from the underlying reader.
type cancelAfterReader struct {
	r      *bytes.Reader
	after  int
	n      int
	cancel context.CancelFunc
}

func (c *cancelAfterReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	if c.n > c.after && c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	return n, err
}

func TestRestoreCancel(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 1000)

	// Pre-existing database at the target must remain usable.
	target := filepath.Join(tmpdir, "dst")
	db, err := Open(getTestOptions(target))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte("original"), []byte("untouched")))
	}))
	require.NoError(t, db.Close())

	ctx, cancel := context.WithCancel(context.Background())
	cr := &cancelAfterReader{
		r:      bytes.NewReader(backup),
		after:  len(backup) / 2,
		cancel: cancel,
	}
	err = RestoreContext(ctx, cr, RestoreOptions{
		Options: getTestOptions(target),
	})
	require.ErrorIs(t, err, context.Canceled)

	// Staging resources and handles are cleaned up; the original database
	// still opens and holds its data.
	requireNoRestoreLeftovers(t, target)
	db, err = Open(getTestOptions(target))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("original"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("untouched"), val)
			return nil
		})
	}))
}

func TestRecoverIncompleteRestore(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	writeDB := func(dir, key, val string) {
		db, err := Open(getTestOptions(dir))
		require.NoError(t, err)
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte(key), []byte(val)))
		}))
		require.NoError(t, db.Close())
	}
	readKey := func(dir, key string) (string, error) {
		db, err := Open(getTestOptions(dir))
		require.NoError(t, err)
		defer db.Close()
		var out string
		err = db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				out = string(val)
				return nil
			})
		})
		return out, err
	}

	t.Run("half-written staging is discarded", func(t *testing.T) {
		target := filepath.Join(tmpdir, "case1")
		writeDB(target, "original", "untouched")
		// Simulate a crash mid-load: staging exists, no commit marker.
		writeDB(target+restoreStagingSuffix, "partial", "junk")

		require.NoError(t, RecoverIncompleteRestore(target, target))
		val, err := readKey(target, "original")
		require.NoError(t, err)
		require.Equal(t, "untouched", val)
		requireNoRestoreLeftovers(t, target)
	})

	t.Run("committed restore is completed", func(t *testing.T) {
		target := filepath.Join(tmpdir, "case2")
		writeDB(target, "original", "old-data")
		// Simulate a crash after the commit marker but before the switch.
		writeDB(target+restoreStagingSuffix, "restored", "new-data")
		require.NoError(t, os.WriteFile(
			restoreCommitMarkerPath(target), []byte(`{}`), 0o644))

		require.NoError(t, RecoverIncompleteRestore(target, target))
		val, err := readKey(target, "restored")
		require.NoError(t, err)
		require.Equal(t, "new-data", val)
		_, err = readKey(target, "original")
		require.ErrorIs(t, err, ErrKeyNotFound)
		requireNoRestoreLeftovers(t, target)
	})

	t.Run("stale old directory is removed", func(t *testing.T) {
		target := filepath.Join(tmpdir, "case3")
		writeDB(target, "current", "data")
		// Simulate a crash after the switch but before old-dir cleanup.
		writeDB(target+restoreOldSuffix, "previous", "junk")

		require.NoError(t, RecoverIncompleteRestore(target, target))
		val, err := readKey(target, "current")
		require.NoError(t, err)
		require.Equal(t, "data", val)
		requireNoRestoreLeftovers(t, target)
	})
}

func TestRestoreRecoversPreviousAttempt(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, entries := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 50)

	// Leave a half-written staging directory behind, as an interrupted
	// restore would.
	target := filepath.Join(tmpdir, "dst")
	db, err := Open(getTestOptions(target + restoreStagingSuffix))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	require.NoError(t, Restore(bytes.NewReader(backup), RestoreOptions{
		Options: getTestOptions(target),
	}))
	verifyRestoredEntries(t, target, entries)
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreEmptyBackup(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 0)

	target := filepath.Join(tmpdir, "dst")
	require.NoError(t, Restore(bytes.NewReader(backup), RestoreOptions{
		Options: getTestOptions(target),
	}))
	db, err := Open(getTestOptions(target))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *Txn) error {
		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()
		it.Rewind()
		require.False(t, it.Valid())
		return nil
	}))
	requireNoRestoreLeftovers(t, target)
}

func TestRestoreCommitMarkerWritten(t *testing.T) {
	// The commit marker must exist while the switch is in flight and be gone
	// afterwards; verify the happy path leaves a clean directory layout.
	tmpdir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(tmpdir)

	backup, _ := buildManifestBackup(t, filepath.Join(tmpdir, "src"), 10)
	target := filepath.Join(tmpdir, "dst")
	require.NoError(t, Restore(bytes.NewReader(backup), RestoreOptions{
		Options: getTestOptions(target),
	}))

	// After a successful restore the target is a complete database and no
	// transaction state remains.
	require.FileExists(t, filepath.Join(target, ManifestFilename))
	requireNoRestoreLeftovers(t, target)
}
