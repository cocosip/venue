package cleanup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/volume"
)

// managedPayloadKey is a real managed file key: 32 lowercase hexadecimal
// characters, exactly the shape WriteFile generates.
const managedPayloadKey = "0123456789abcdef0123456789abcdef"

// writeTestFile creates a file (and its parents) below a volume mount path.
func writeTestFile(t *testing.T, mountPath string, relativePath string, content string) {
	t.Helper()

	fullPath := filepath.Join(mountPath, relativePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", fullPath, err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", fullPath, err)
	}
}

// newAdditionalTestVolume creates one more real volume for multi-volume sweeps.
func newAdditionalTestVolume(t *testing.T, volumeID string) core.StorageVolume {
	t.Helper()

	vol, err := volume.NewLocalFileSystemVolume(&volume.LocalFileSystemVolumeOptions{
		VolumeID:  volumeID,
		MountPath: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewLocalFileSystemVolume(%s) error = %v", volumeID, err)
	}
	return vol
}

// TestCleanupJunkFiles_RemovesOnlyJunkFilesAndKeepsManagedPayloads is the
// regression test for the Locus junk-file sweep (StorageCleanupService.cs
// _ignoredFilenames): Thumbs.db, .DS_Store and desktop.ini are removed from
// every volume, matching case-insensitively, while a managed payload written
// next to them survives untouched.
func TestCleanupJunkFiles_RemovesOnlyJunkFilesAndKeepsManagedPayloads(t *testing.T) {
	ctx := context.Background()

	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	first := volumes["test-volume"]
	second := newAdditionalTestVolume(t, "second-volume")
	volumes[second.VolumeID()] = second

	junkFiles := []string{
		filepath.Join("tenant-001", "Thumbs.db"),
		filepath.Join("tenant-001", "ab", "cd", ".DS_Store"),
		filepath.Join("tenant-001", "ab", "cd", "desktop.ini"),
		filepath.Join("2026", "04", "04", "thumbs.DB"),
	}
	const junkContent = "junk"
	for _, relativePath := range junkFiles {
		writeTestFile(t, first.MountPath(), relativePath, junkContent)
	}
	// One junk file on the second volume proves every configured volume is swept.
	writeTestFile(t, second.MountPath(), filepath.Join("tenant-001", "Thumbs.db"), junkContent)

	// A managed payload and a look-alike that is not junk.
	payloadPath := filepath.Join("tenant-001", managedPayloadKey[:2], managedPayloadKey[2:4], managedPayloadKey+".txt")
	writeTestFile(t, first.MountPath(), payloadPath, "payload")
	lookAlike := filepath.Join("tenant-001", "Thumbs.db.bak")
	writeTestFile(t, first.MountPath(), lookAlike, "look-alike")

	stats, err := service.CleanupJunkFiles(ctx)
	if err != nil {
		t.Fatalf("CleanupJunkFiles() error = %v", err)
	}

	wantRemoved := len(junkFiles) + 1
	if stats.JunkFilesRemoved != wantRemoved {
		t.Errorf("JunkFilesRemoved = %d, want %d", stats.JunkFilesRemoved, wantRemoved)
	}
	if wantFreed := int64(wantRemoved * len(junkContent)); stats.SpaceFreed != wantFreed {
		t.Errorf("SpaceFreed = %d, want %d", stats.SpaceFreed, wantFreed)
	}

	for _, relativePath := range junkFiles {
		if _, statErr := os.Stat(filepath.Join(first.MountPath(), relativePath)); !os.IsNotExist(statErr) {
			t.Errorf("junk file %s still exists, stat error = %v", relativePath, statErr)
		}
	}
	if _, statErr := os.Stat(filepath.Join(second.MountPath(), "tenant-001", "Thumbs.db")); !os.IsNotExist(statErr) {
		t.Errorf("junk file on the second volume still exists, stat error = %v", statErr)
	}
	for _, survivor := range []string{payloadPath, lookAlike} {
		if _, statErr := os.Stat(filepath.Join(first.MountPath(), survivor)); statErr != nil {
			t.Errorf("expected %s to survive, stat error = %v", survivor, statErr)
		}
	}

	// A second sweep has nothing left to do.
	secondStats, err := service.CleanupJunkFiles(ctx)
	if err != nil {
		t.Fatalf("second CleanupJunkFiles() error = %v", err)
	}
	if secondStats.JunkFilesRemoved != 0 {
		t.Errorf("second sweep JunkFilesRemoved = %d, want 0", secondStats.JunkFilesRemoved)
	}
}

// TestCleanupJunkFiles_HonoursContextCancellation verifies that a cancelled
// context aborts the sweep instead of reporting success.
func TestCleanupJunkFiles_HonoursContextCancellation(t *testing.T) {
	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)

	relativePath := filepath.Join("tenant-001", "Thumbs.db")
	writeTestFile(t, volumes["test-volume"].MountPath(), relativePath, "junk")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := service.CleanupJunkFiles(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CleanupJunkFiles() error = %v, want context.Canceled", err)
	}
	if stats.JunkFilesRemoved != 0 {
		t.Errorf("JunkFilesRemoved = %d, want 0 for a cancelled sweep", stats.JunkFilesRemoved)
	}
	if _, statErr := os.Stat(filepath.Join(volumes["test-volume"].MountPath(), relativePath)); statErr != nil {
		t.Errorf("cancelled sweep must not remove junk files, stat error = %v", statErr)
	}
}

// TestCleanupJunkFiles_KeepsUnmanagedNonJunkFiles guards the sweep against
// over-matching: only the three Locus names are junk.
func TestCleanupJunkFiles_KeepsUnmanagedNonJunkFiles(t *testing.T) {
	ctx := context.Background()

	service, _, volumes := newRegressionCleanupService(t, []string{"test-tenant"}, nil)
	mount := volumes["test-volume"].MountPath()

	survivors := []string{
		filepath.Join("tenant-001", "thumbs.db.txt"),
		filepath.Join("tenant-001", "MyThumbs.db"),
		filepath.Join("tenant-001", ".DS_Store2"),
		filepath.Join("tenant-001", "notes.txt"),
	}
	for _, relativePath := range survivors {
		writeTestFile(t, mount, relativePath, "keep")
	}

	stats, err := service.CleanupJunkFiles(ctx)
	if err != nil {
		t.Fatalf("CleanupJunkFiles() error = %v", err)
	}
	if stats.JunkFilesRemoved != 0 {
		t.Errorf("JunkFilesRemoved = %d, want 0", stats.JunkFilesRemoved)
	}
	for _, relativePath := range survivors {
		if _, statErr := os.Stat(filepath.Join(mount, relativePath)); statErr != nil {
			t.Errorf("expected %s to survive, stat error = %v", relativePath, statErr)
		}
	}
}
