package config

import (
	"testing"
)

// TestCloneDeepCopiesRetiredVolumes pins the deep-copy contract of Clone: the
// clone must not alias the source's RetiredVolumes backing array, or mutating
// the clone through WithRetiredVolumes would rewrite the source's elements.
func TestCloneDeepCopiesRetiredVolumes(t *testing.T) {
	source := DefaultConfig().
		WithCleanup(NewCleanupConfig().WithRetiredVolumes(NewRetiredVolumeConfig("gone")))
	clone := source.Clone()

	if len(clone.Cleanup.RetiredVolumes) != 1 || clone.Cleanup.RetiredVolumes[0].VolumeID != "gone" {
		t.Fatalf("clone retired volumes = %#v, want the source's entries", clone.Cleanup.RetiredVolumes)
	}

	// Rewrite the clone's collection: the source must be untouched.
	clone.Cleanup.WithRetiredVolumes(NewRetiredVolumeConfig("other"))
	if len(source.Cleanup.RetiredVolumes) != 1 || source.Cleanup.RetiredVolumes[0].VolumeID != "gone" {
		t.Fatalf("source retired volumes = %#v, want them unmodified after the clone changed", source.Cleanup.RetiredVolumes)
	}
}

// TestWithRetiredVolumesDoesNotTruncateSharedBackingArray verifies that
// replacing the collection rebuilds the slice instead of truncating it in
// place, which would overwrite elements shared with a Clone()d config.
func TestWithRetiredVolumesDoesNotTruncateSharedBackingArray(t *testing.T) {
	source := DefaultConfig().
		WithCleanup(NewCleanupConfig().WithRetiredVolumes(NewRetiredVolumeConfig("gone")))
	clone := source.Clone()

	_ = clone.Cleanup.WithRetiredVolumes(NewRetiredVolumeConfig("replacement"))

	if source.Cleanup.RetiredVolumes[0].VolumeID != "gone" {
		t.Fatalf("source retired volume = %q, want %q after the clone was replaced",
			source.Cleanup.RetiredVolumes[0].VolumeID, "gone")
	}
}
