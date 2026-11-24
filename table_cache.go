/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4/table"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/dgraph-io/ristretto/v2/z"
)

// tableCacheShards is the number of independently locked shards in the table
// cache. More shards reduce contention on the open/eviction paths at the cost
// of a small fixed overhead; 8 keeps the per-shard LRU coarse enough to be
// effective while spreading concurrent opens.
const tableCacheShards = 8

// initTableCache creates the generation-aware table handle cache used by this
// DB. The cache is the sole owner of table-file opens and handle lifetimes.
func (db *DB) initTableCache() {
	perShard := 0 // 0 disables the open-handle limit (retain everything).
	if db.opt.NumOpenTables > 0 {
		perShard = (db.opt.NumOpenTables + tableCacheShards - 1) / tableCacheShards
	}
	db.tableCache = table.NewTableCache(perShard, tableCacheShards, db.openTableFromDisk)
}

// openTableFromDisk is the table.TableOpener used by the table cache. It opens
// the SSTable identified by id from disk under the supplied generation, using
// the compression/datakey recorded in the manifest. The returned table carries
// one reference (the cache's hold). A checksum failure on a table whose
// checksum mode is OnTableRead is surfaced as an error so the cache never
// installs a corrupt handle; the caller may then Invalidate the entry.
func (db *DB) openTableFromDisk(id, gen uint64) (*table.Table, error) {
	tf, ok := db.manifest.manifest.Tables[id]
	if !ok {
		return nil, y.Wrapf(nil, "table %d is not present in the manifest", id)
	}
	dk, err := db.registry.DataKey(tf.KeyID)
	if err != nil {
		return nil, y.Wrapf(err, "while reading datakey for table %d", id)
	}
	topt := buildTableOptions(db)
	topt.Compression = tf.Compression
	topt.DataKey = dk

	fname := table.NewFilename(id, db.opt.Dir)
	mf, err := z.OpenMmapFile(fname, db.opt.getFileFlags(), 0)
	if err != nil {
		return nil, y.Wrapf(err, "opening table file: %q", fname)
	}
	t, err := table.OpenTable(mf, topt, gen)
	if err != nil {
		return nil, y.Wrapf(err, "opening table: %q", fname)
	}
	return t, nil
}

// openInMemoryTable opens an in-memory (L0) table and registers it with the
// table cache. The caller supplies the generation (reserved up front so it can
// be plumbed into the builder options); the table's block/index cache entries
// stay unique across incarnations. The returned table carries one reference
// owned by the caller in addition to the cache's retention.
func (db *DB) openInMemoryTable(data []byte, id uint64, gen uint64, opt *table.Options) (*table.Table, error) {
	if db.tableCache == nil {
		// Defensive: in-memory DBs without a wired cache still need a handle.
		return table.OpenInMemoryTable(data, id, opt, gen)
	}
	t, err := table.OpenInMemoryTable(data, id, opt, gen)
	if err != nil {
		return nil, err
	}
	if err := db.tableCache.Register(t); err != nil {
		_ = t.Close(-1)
		return nil, err
	}
	return t, nil
}

// registerCreatedTable adopts a table freshly written by a flush, compaction
// or stream writer into the table cache. The table must already carry the
// generation reserved before it was built (via TableCache.NextGen, plumbed
// through the builder options), so the on-disk file and its cache keys share
// one identity. On failure the caller still owns its reference and is
// responsible for releasing it.
func (db *DB) registerCreatedTable(t *table.Table) error {
	if db.tableCache == nil {
		return nil
	}
	return db.tableCache.Register(t)
}

// isCorruptTableOpenErr reports whether err from opening a table indicates the
// table's own bytes are corrupt, in which case the load path skips that single
// table instead of failing the whole DB. It matches the checksum sentinel
// directly and also recognizes the decompression errors produced when a block's
// bytes were altered. Structural errors (missing file, bad options) are not
// corruption and remain fatal.
func isCorruptTableOpenErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, y.ErrChecksumMismatch) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "failed to decompress") ||
		strings.Contains(msg, "corrupt input")
}

// isChecksumOpenErr is retained as a narrow alias for the checksum-only case.
func isChecksumOpenErr(err error) bool {
	return isCorruptTableOpenErr(err)
}
