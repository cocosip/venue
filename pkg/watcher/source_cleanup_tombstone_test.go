package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPruneTerminalKeepsKeptTombstoneWhileSourceExists pins the Keep dedup
// contract: the Kept job row is the durable tombstone that stops a still-present
// source file from being re-imported after the terminal retention window.
// Pruning it would re-import every Keep file once per retention period.
func TestPruneTerminalKeepsKeptTombstoneWhileSourceExists(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := OpenSourceCleanupStore(ctx, filepath.Join(dir, "cleanup.db"), SourceCleanupStoreOptions{MaxActiveJobs: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	liveSource := filepath.Join(dir, "kept-source.dcm")
	if err := os.WriteFile(liveSource, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	kept := SourceCleanupJob{WatcherID: "w", TenantID: "t", SourcePath: liveSource, Fingerprint: "fp", Action: SourceCleanupActionKeep}
	if reserved, err := store.TryReserve(ctx, &kept); err != nil || !reserved {
		t.Fatalf("TryReserve() = %v, %v", reserved, err)
	}
	if err := store.MarkImported(ctx, kept.JobID, "file-1", "op-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(ctx, time.Now(), time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimDue() = %d, %v; want one job", len(claimed), err)
	}
	if err := store.MarkSucceeded(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}

	// Long after the retention window: the row must survive while its source
	// file still exists.
	pruned, err := store.PruneTerminal(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("PruneTerminal() error = %v", err)
	}
	if pruned != 0 {
		t.Fatalf("PruneTerminal() removed %d rows, want 0 while the Kept source exists", pruned)
	}
	row, err := store.GetBySource(ctx, kept.WatcherID, kept.SourcePath)
	if err != nil || row == nil {
		t.Fatalf("GetBySource() = %v, %v; want the Kept tombstone to survive", row, err)
	}

	// Once the source is gone, the tombstone has nothing left to dedup and the
	// prune must retire it, keeping the table bounded.
	if err := os.Remove(liveSource); err != nil {
		t.Fatal(err)
	}
	pruned, err = store.PruneTerminal(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("PruneTerminal() after source removal error = %v", err)
	}
	if pruned != 1 {
		t.Fatalf("PruneTerminal() removed %d rows, want 1 once the source is gone", pruned)
	}
}

// TestWatcherIDValidationRejectsUnsafeSegments pins the path-safety rule for
// watcher IDs: they become directory segments below the failure directory, so
// separators and traversal segments must be rejected at registration.
func TestWatcherIDValidationRejectsUnsafeSegments(t *testing.T) {
	for _, watcherID := range []string{"a/b", `a\b`, "..", ".", "a\x00b"} {
		if err := validateWatcherID(watcherID); err == nil {
			t.Errorf("validateWatcherID(%q) = nil, want an error", watcherID)
		}
	}
	if err := validateWatcherID("watcher-1"); err != nil {
		t.Errorf("validateWatcherID(watcher-1) error = %v, want nil", err)
	}
}
