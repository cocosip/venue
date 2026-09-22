package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// TestListBackupsNewestFirstOrdersByModificationTimeThenStamp keeps the
// automatic-restore fallback order stable: the newest backup is the first
// candidate, and a coarse file-system timestamp is broken by the stamp encoded
// in the frozen file name.
func TestListBackupsNewestFirstOrdersByModificationTimeThenStamp(t *testing.T) {
	directory := t.TempDir()
	names := []string{
		backupFilePrefix + "20240101T000000Z" + backupFileSuffix,
		backupFilePrefix + "20240103T000000Z" + backupFileSuffix,
		backupFilePrefix + "20240102T000000Z" + backupFileSuffix,
	}
	base := time.Now()
	for index, name := range names {
		full := filepath.Join(directory, name)
		if err := os.WriteFile(full, []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		stamp := base.Add(time.Duration(index) * time.Hour)
		if err := os.Chtimes(full, stamp, stamp); err != nil {
			t.Fatalf("Chtimes() error = %v", err)
		}
	}

	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		t.Fatalf("listBackupsNewestFirst() error = %v", err)
	}
	got := make([]string, 0, len(backups))
	for _, backup := range backups {
		got = append(got, filepath.Base(backup.path))
	}
	want := []string{names[2], names[1], names[0]}
	if !slices.Equal(got, want) {
		t.Fatalf("listBackupsNewestFirst() = %v, want %v", got, want)
	}
	if len(backups) != len(names) {
		t.Fatalf("listBackupsNewestFirst() returned %d backups, want %d", len(backups), len(names))
	}

	// Equal modification times must still produce a deterministic order, which
	// the stamp in the file name provides.
	for _, name := range names {
		full := filepath.Join(directory, name)
		if err := os.Chtimes(full, base, base); err != nil {
			t.Fatalf("Chtimes() error = %v", err)
		}
	}
	tied, err := listBackupsNewestFirst(directory)
	if err != nil {
		t.Fatalf("listBackupsNewestFirst() with tied timestamps error = %v", err)
	}
	if len(tied) != len(names) {
		t.Fatalf("listBackupsNewestFirst() with tied timestamps returned %d backups, want %d", len(tied), len(names))
	}
	for index := 1; index < len(tied); index++ {
		previous, previousErr := parseBackupTimestamp(filepath.Base(tied[index-1].path))
		current, currentErr := parseBackupTimestamp(filepath.Base(tied[index].path))
		if previousErr != nil || currentErr != nil {
			t.Fatalf("parseBackupTimestamp() error = %v, %v", previousErr, currentErr)
		}
		if previous.Before(current) {
			t.Fatalf("tied backups = %v, want the newer stamp first", tied)
		}
	}
}

// TestBackupInventoryRecognisesOnlyTheFrozenName pins the operator contract: a
// backup set is a glob on metadata.<yyyyMMddTHHmmssZ>.bak, including the
// collision suffix two backups inside one second add, and nothing else.
func TestBackupInventoryRecognisesOnlyTheFrozenName(t *testing.T) {
	testCases := []struct {
		name string
		want bool
	}{
		{name: backupFilePrefix + "20240506T070809Z" + backupFileSuffix, want: true},
		{name: backupFilePrefix + "20240506T070809Z.1" + backupFileSuffix, want: true},
		{name: backupFilePrefix + "20240506T070809Z.999" + backupFileSuffix, want: true},
		{name: backupFilePrefix + "20240506T070809Z", want: false},
		{name: "metadata.bak", want: false},
		{name: backupFilePrefix + "20240506T070809Z" + backupFileSuffix + ".tmp", want: false},
		{name: "metadata.2024-05-06.bak", want: false},
		{name: "quotas.db", want: false},
		{name: "metadata.20240506T070809Z" + backupRestoreSuffix + "20240506T070809Z", want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBackupFileName(tc.name); got != tc.want {
				t.Fatalf("isBackupFileName(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}

	// A directory that holds only foreign names yields no backup candidates, so
	// automatic restore never mistakes another artifact for a backup.
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "metadata.20240506T070809Z"+backupFileSuffix+".tmp"), []byte("staging"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, backupFilePrefix+"not-a-stamp"+backupFileSuffix), []byte("foreign"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	backups, err := listBackupsNewestFirst(directory)
	if err != nil {
		t.Fatalf("listBackupsNewestFirst() error = %v", err)
	}
	if len(backups) != 0 {
		t.Fatalf("listBackupsNewestFirst() = %v, want no candidate", backups)
	}

	// A missing directory is an empty inventory, never an error.
	if missing, err := listBackupsNewestFirst(filepath.Join(directory, "absent")); err != nil || len(missing) != 0 {
		t.Fatalf("listBackupsNewestFirst(missing) = (%v, %v), want no backups and no error", missing, err)
	}
}

// TestQuarantinePathIsWindowsSafeAndCollisionFree keeps the quarantine naming
// contract: the stamp contains no colon, and two quarantines inside one second
// cannot overwrite each other.
func TestQuarantinePathIsWindowsSafeAndCollisionFree(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, metadataDatabaseFileName)
	instant := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

	first, err := freeQuarantinePath(databasePath, instant)
	if err != nil {
		t.Fatalf("freeQuarantinePath() error = %v", err)
	}
	wantName := metadataDatabaseFileName + corruptedDatabaseSuffix + "20240506T070809Z"
	if got := filepath.Base(first); got != wantName {
		t.Fatalf("quarantine name = %q, want %q", got, wantName)
	}
	if strings.ContainsRune(filepath.Base(first), ':') {
		t.Fatalf("quarantine name %q carries a colon, which no Windows path segment accepts", filepath.Base(first))
	}

	// Same second, same directory: the second quarantine takes a numeric suffix.
	if err := os.WriteFile(first, []byte("rescue"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	second, err := freeQuarantinePath(databasePath, instant)
	if err != nil {
		t.Fatalf("freeQuarantinePath() on a collision error = %v", err)
	}
	if second != first+".1" {
		t.Fatalf("collision quarantine path = %q, want %q", second, first+".1")
	}
}

// TestClassifyBackupErrorLabelsAreStableAndPathFree pins the recovery log
// classification: a stable label instead of a path-bearing raw error.
func TestClassifyBackupErrorLabelsAreStableAndPathFree(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: "none"},
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "deadline_exceeded"},
		{name: "invalid argument", err: core.ErrInvalidArgument, want: "invalid_argument"},
		{name: "database error", err: fmt.Errorf("open %s: %w", filepath.Join("C:", "secret", "metadata.db"), core.ErrDatabaseError), want: "database_error"},
		{name: "permission", err: os.ErrPermission, want: "permission_denied"},
		{name: "other", err: errors.New("connection reset by peer"), want: "error"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBackupError(tc.err)
			if got != tc.want {
				t.Fatalf("classifyBackupError(%v) = %q, want %q", tc.err, got, tc.want)
			}
			if strings.ContainsAny(got, `/\:`) {
				t.Fatalf("classifyBackupError(%v) = %q, want a path-free label", tc.err, got)
			}
		})
	}
}
