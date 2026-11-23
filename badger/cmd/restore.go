/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"context"
	"errors"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var restoreFile string
var maxPendingWrites int
var restoreForce bool

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore Badger database.",
	Long: `Restore Badger database from a file.

It reads a file generated using the backup command (or by calling the
DB.Backup() or DB.BackupWithManifest() API methods) and writes each key-value
pair found in the file to the Badger database.

Restore is transactional: the database is fully rebuilt and validated in a
staging directory first, and the target directory is switched over only after
a commit marker has been written. A corrupt or truncated backup, an
unsupported backup version, or an interrupted restore therefore never damages
a pre-existing database, and leftovers from an interrupted restore are
cleaned up automatically on the next run.

By default restore refuses to replace an existing database; pass --force to
atomically replace it.`,
	RunE: doRestore,
}

func init() {
	RootCmd.AddCommand(restoreCmd)
	restoreCmd.Flags().StringVarP(&restoreFile, "backup-file", "f",
		"badger.bak", "File to restore from")
	// Default value for maxPendingWrites is 256, to minimise memory usage
	// and overall finish time.
	restoreCmd.Flags().IntVarP(&maxPendingWrites, "max-pending-writes", "w",
		256, "Max number of pending writes at any time while restore")
	restoreCmd.Flags().BoolVar(&restoreForce, "force", false,
		"Atomically replace an existing database at the target directory")
}

func doRestore(cmd *cobra.Command, args []string) error {
	// Check if the DB already exists
	manifestFile := filepath.Join(sstDir, badger.ManifestFilename)
	if _, err := os.Stat(manifestFile); err == nil && !restoreForce { // File already exists.
		return errors.New("Cannot restore to an already existing database (use --force to replace it atomically)")
	} else if err != nil && !os.IsNotExist(err) { // Return an error if anything other than the error above
		return err
	}

	// Open File
	f, err := os.Open(restoreFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Abort the restore on interrupt; the staging directory is cleaned up and
	// any pre-existing database is left untouched.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithNumVersionsToKeep(math.MaxInt32)
	return badger.RestoreContext(ctx, f, badger.RestoreOptions{
		Options:          opt,
		MaxPendingWrites: maxPendingWrites,
	})
}
