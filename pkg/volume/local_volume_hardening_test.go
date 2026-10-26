package volume

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// cancelOnReadReader cancels the write's context during the payload copy, so a
// context-aware copy notices the cancellation on its next chunk instead of
// streaming the whole payload.
type cancelOnReadReader struct {
	cancel    context.CancelFunc
	remaining int
}

func (r *cancelOnReadReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	// Cancel after the first chunk: the copy must not continue to the rest.
	r.cancel()
	r.remaining--
	return copy(p, bytes.Repeat([]byte("x"), len(p))), nil
}

// TestWriteFileObservesContextCancellationDuringCopy is the regression test for a
// payload copy that ignores the request context: a cancelled write must stop
// instead of streaming the whole payload and leaving a partial file behind.
func TestWriteFileObservesContextCancellationDuringCopy(t *testing.T) {
	volume, mountDir := createWritePathVolume(t, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &cancelOnReadReader{cancel: cancel, remaining: 64}

	_, err := volume.WriteFile(ctx, "cancelled/payload.bin", reader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteFile() error = %v, want context.Canceled", err)
	}

	if _, statErr := os.Stat(filepath.Join(mountDir, "cancelled", "payload.bin")); !os.IsNotExist(statErr) {
		t.Errorf("cancelled write left a file behind: %v", statErr)
	}
}

// TestWriteFileFsyncsParentDirectory is the regression test for a durable file
// whose directory entry was never synced: with fsync enabled the containing
// directory must be synced as well, otherwise a crash can lose the name while
// the metadata row survives.
func TestWriteFileFsyncsParentDirectory(t *testing.T) {
	volume, mountDir := createWritePathVolume(t, true)

	var (
		mu          sync.Mutex
		syncedFiles []string
		syncedDirs  []string
	)
	volume.syncFile = func(file *os.File) error {
		mu.Lock()
		defer mu.Unlock()
		syncedFiles = append(syncedFiles, file.Name())
		return nil
	}
	volume.syncDirectory = func(path string) error {
		mu.Lock()
		defer mu.Unlock()
		syncedDirs = append(syncedDirs, path)
		return nil
	}

	if _, err := volume.WriteFile(context.Background(), "nested/payload.bin", strings.NewReader("durable")); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(mountDir, "nested", "payload.bin")
	if len(syncedFiles) != 1 || syncedFiles[0] != fullPath {
		t.Errorf("synced files = %v, want [%s]", syncedFiles, fullPath)
	}
	wantDir := filepath.Dir(fullPath)
	if len(syncedDirs) != 1 || syncedDirs[0] != wantDir {
		t.Errorf("synced directories = %v, want [%s]", syncedDirs, wantDir)
	}
}

// TestWriteFileDoesNotSyncDirectoryWhenFsyncDisabled verifies the durability
// trade-off stays explicit: without EnableFsync the write path performs no sync
// at all.
func TestWriteFileDoesNotSyncDirectoryWhenFsyncDisabled(t *testing.T) {
	volume, _ := createWritePathVolume(t, false)

	var syncs int
	volume.syncFile = func(*os.File) error {
		syncs++
		return nil
	}
	volume.syncDirectory = func(string) error {
		syncs++
		return nil
	}

	if _, err := volume.WriteFile(context.Background(), "payload.bin", strings.NewReader("x")); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if syncs != 0 {
		t.Errorf("sync calls = %d, want 0 while fsync is disabled", syncs)
	}
}

// TestWriteFileRemovesFileWhenSyncFails is the regression test for a file that
// survives a failed fsync: the copy-failure path already rolls the file back, so
// the sync-failure path must not leave an unaccounted payload on the volume.
func TestWriteFileRemovesFileWhenSyncFails(t *testing.T) {
	volume, mountDir := createWritePathVolume(t, true)

	syncErr := errors.New("injected sync failure")
	volume.syncFile = func(*os.File) error { return syncErr }

	_, err := volume.WriteFile(context.Background(), "unsynced/payload.bin", strings.NewReader("payload"))
	if !errors.Is(err, syncErr) {
		t.Fatalf("WriteFile() error = %v, want %v", err, syncErr)
	}

	if _, statErr := os.Stat(filepath.Join(mountDir, "unsynced", "payload.bin")); !os.IsNotExist(statErr) {
		t.Errorf("failed fsync left the payload behind: %v", statErr)
	}
}

// TestBuildFilePathRejectsUnsafeExtension is the regression test for an
// extension that was concatenated into the physical path unchecked, so
// BuildFilePath(key, "/../../x") returned a traversing relative path and a nil
// error.
func TestBuildFilePathRejectsUnsafeExtension(t *testing.T) {
	volume, _ := createTestVolume(t, 2)

	unsafeExtensions := []string{
		"/../../x",
		`\..\x`,
		".txt/../../x",
		".a\x00b",
		".a\nb",
		"." + strings.Repeat("x", maxExtensionLength+1),
	}

	for _, extension := range unsafeExtensions {
		t.Run(extension, func(t *testing.T) {
			path, err := volume.BuildFilePath("550e8400e29b41d4a716446655440000", extension)
			if err == nil {
				t.Fatalf("BuildFilePath(extension=%q) = %q, want an error", extension, path)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Errorf("error = %v, want ErrInvalidArgument", err)
			}
			if path != "" {
				t.Errorf("path = %q, want an empty path on rejection", path)
			}
		})
	}

	// A benign extension still produces the sharded path.
	path, err := volume.BuildFilePath("550e8400e29b41d4a716446655440000", ".pdf")
	if err != nil {
		t.Fatalf("BuildFilePath(.pdf) error = %v", err)
	}
	if want := filepath.Join("55", "0e", "550e8400e29b41d4a716446655440000.pdf"); path != want {
		t.Errorf("BuildFilePath(.pdf) = %q, want %q", path, want)
	}
}

// TestReadFileMapsOpenRaceToFileNotFound is the regression test for the TOCTOU
// between the old Stat and the Open: when the file disappears in that window the
// open failure must still be reported as core.ErrFileNotFound.
func TestReadFileMapsOpenRaceToFileNotFound(t *testing.T) {
	volume, mountDir := createTestVolume(t, 0)

	if _, err := volume.WriteFile(context.Background(), "raced.txt", strings.NewReader("payload")); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	// Simulate a delete that wins the race between the existence check and the
	// open. With the old stat-then-open the stat still succeeded and the open
	// failure surfaced as a generic error.
	original := volume.openFileForRead
	volume.openFileForRead = func(name string) (*os.File, error) {
		if err := os.Remove(filepath.Join(mountDir, "raced.txt")); err != nil {
			t.Fatalf("remove raced file: %v", err)
		}
		return original(name)
	}

	reader, err := volume.ReadFile(context.Background(), "raced.txt")
	if reader != nil {
		_ = reader.Close()
	}
	if !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("ReadFile() error = %v, want ErrFileNotFound for a racing delete", err)
	}
}

// TestHealthProbeUsesInternalProbeName is the regression test for a probe file
// created under an ad-hoc name in the volume root: a crashed probe left junk that
// no other component recognized. The probe now reuses the warmup name, so its
// throwaway file carries the volume-internal prefix.
func TestHealthProbeUsesInternalProbeName(t *testing.T) {
	volume, mountDir := createTestVolume(t, 0)

	var created []string
	volume.warmupName = func() string {
		name := writeWarmupFilePrefix + "probe"
		created = append(created, name)
		return name
	}

	if !volume.ProbeHealth(context.Background()) {
		t.Fatal("ProbeHealth() = false, want a healthy volume")
	}

	if len(created) != 1 {
		t.Fatalf("probe used %d internal names, want 1", len(created))
	}
	if !strings.HasPrefix(created[0], writeWarmupFilePrefix) {
		t.Errorf("probe file name = %q, want the %q prefix", created[0], writeWarmupFilePrefix)
	}
	if entries := collectVolumeEntries(t, mountDir); len(entries) != 0 {
		t.Errorf("health probe left %v behind, want nothing", entries)
	}
}

// TestMoveFileCopyFallbackHonoursFsyncSetting is the regression test for a
// cross-device move that always synced its staging copy while the normal write
// path honours EnableFsync.
func TestMoveFileCopyFallbackHonoursFsyncSetting(t *testing.T) {
	testCases := []struct {
		name        string
		enableFsync bool
		wantSyncs   int
	}{
		{name: "fsync disabled", enableFsync: false, wantSyncs: 0},
		{name: "fsync enabled", enableFsync: true, wantSyncs: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume, mountDir := createWritePathVolume(t, tc.enableFsync)

			var syncs int
			volume.syncFile = func(*os.File) error {
				syncs++
				return nil
			}
			volume.syncDirectory = func(string) error { return nil }

			sourcePath := filepath.Join(mountDir, "src", "payload.bin")
			destinationPath := filepath.Join(mountDir, "dst", "payload.bin")
			if err := os.MkdirAll(filepath.Dir(sourcePath), 0755); err != nil {
				t.Fatalf("MkdirAll(source) error = %v", err)
			}
			if err := os.WriteFile(sourcePath, []byte("move payload"), 0644); err != nil {
				t.Fatalf("WriteFile(source) error = %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(destinationPath), 0755); err != nil {
				t.Fatalf("MkdirAll(destination) error = %v", err)
			}

			if err := volume.copyFileAndRemoveSource(context.Background(), sourcePath, destinationPath); err != nil {
				t.Fatalf("copyFileAndRemoveSource() error = %v", err)
			}
			if syncs != tc.wantSyncs {
				t.Errorf("sync calls = %d, want %d", syncs, tc.wantSyncs)
			}
		})
	}
}
