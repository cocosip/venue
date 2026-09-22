package sqlite

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestOptionsValidate covers the configuration whitelists: the defaults must be
// accepted, and every rejection must wrap core.ErrInvalidArgument so that a
// configuration defect is classifiable with errors.Is.
func TestOptionsValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{name: "defaults", opts: DefaultOptions()},
		{name: "wal and normal", opts: Options{JournalMode: "WAL", SynchronousMode: "NORMAL", CacheSizeKb: -4000, BusyTimeoutMs: 5000}},
		{name: "lowercase journal and synchronous", opts: Options{JournalMode: "wal", SynchronousMode: "normal", CacheSizeKb: -4000, BusyTimeoutMs: 5000}},
		{name: "mixed case journal and synchronous", opts: Options{JournalMode: "Wal", SynchronousMode: "fUlL", CacheSizeKb: -2000, BusyTimeoutMs: 100}},
		{name: "delete journal", opts: Options{JournalMode: "DELETE", SynchronousMode: "OFF", CacheSizeKb: -4000, BusyTimeoutMs: 0}},
		{name: "truncate journal", opts: Options{JournalMode: "TRUNCATE", SynchronousMode: "EXTRA", CacheSizeKb: -1, BusyTimeoutMs: 1}},
		{name: "persist journal", opts: Options{JournalMode: "PERSIST", SynchronousMode: "0", CacheSizeKb: 2000, BusyTimeoutMs: 250}},
		{name: "memory journal", opts: Options{JournalMode: "MEMORY", SynchronousMode: "1", CacheSizeKb: -8000, BusyTimeoutMs: 5000}},
		{name: "off journal", opts: Options{JournalMode: "OFF", SynchronousMode: "2", CacheSizeKb: -4000, BusyTimeoutMs: 5000}},
		{name: "numeric synchronous three", opts: Options{JournalMode: "WAL", SynchronousMode: "3", CacheSizeKb: -4000, BusyTimeoutMs: 5000}},
		{name: "failure mode described by number", opts: Options{JournalMode: "WAL", SynchronousMode: "WAL", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "invalid journal mode", opts: Options{JournalMode: "NO_SUCH_MODE", SynchronousMode: "NORMAL", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "empty journal mode", opts: Options{JournalMode: "", SynchronousMode: "NORMAL", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "empty synchronous mode", opts: Options{JournalMode: "WAL", SynchronousMode: "", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "invalid synchronous mode", opts: Options{JournalMode: "WAL", SynchronousMode: "SOMETIMES", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "synchronous out of range", opts: Options{JournalMode: "WAL", SynchronousMode: "4", CacheSizeKb: -4000, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "zero cache size", opts: Options{JournalMode: "WAL", SynchronousMode: "NORMAL", CacheSizeKb: 0, BusyTimeoutMs: 5000}, wantErr: true},
		{name: "negative busy timeout", opts: Options{JournalMode: "WAL", SynchronousMode: "NORMAL", CacheSizeKb: -4000, BusyTimeoutMs: -1}, wantErr: true},
		{name: "zero value options", opts: Options{}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.opts.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Validate() = nil, want an error")
				}
				if !errors.Is(err, core.ErrInvalidArgument) {
					t.Fatalf("Validate() = %v, want a wrapped core.ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

// TestDefaultOptions pins the documented Locus-compatible defaults.
func TestDefaultOptions(t *testing.T) {
	t.Parallel()

	got := DefaultOptions()
	if got.JournalMode != "WAL" {
		t.Errorf("JournalMode = %q, want WAL", got.JournalMode)
	}
	if got.SynchronousMode != "NORMAL" {
		t.Errorf("SynchronousMode = %q, want NORMAL", got.SynchronousMode)
	}
	if got.CacheSizeKb != -4000 {
		t.Errorf("CacheSizeKb = %d, want -4000", got.CacheSizeKb)
	}
	if got.BusyTimeoutMs != 5000 {
		t.Errorf("BusyTimeoutMs = %d, want 5000", got.BusyTimeoutMs)
	}
	if got.CheckpointAfterBatch {
		t.Error("CheckpointAfterBatch = true, want false")
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("DefaultOptions().Validate() = %v, want nil", err)
	}
}

// TestOptionsContainCheckpointAfterBatchAsDataOnly documents that
// CheckpointAfterBatch is read by callers, never by DSN or Open: changing it
// must not change the produced data source name at all.
func TestOptionsContainCheckpointAfterBatchAsDataOnly(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "checkpoint.db")
	off := DefaultOptions()
	on := DefaultOptions()
	on.CheckpointAfterBatch = true

	offDSN, err := off.DSN(path)
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}
	onDSN, err := on.DSN(path)
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}
	if offDSN != onDSN {
		t.Fatalf("CheckpointAfterBatch changed the DSN:\n off: %s\n  on: %s", offDSN, onDSN)
	}
}
