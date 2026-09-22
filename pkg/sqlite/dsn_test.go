package sqlite

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// TestOptionsDSNCarriesEveryPragma pins the data source name contract: a single
// string that creates the file and configures each pooled connection
// identically before any statement runs.
func TestOptionsDSNCarriesEveryPragma(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.db")

	opts := DefaultOptions()
	opts.JournalMode = "WAL"
	opts.SynchronousMode = "FULL"
	opts.CacheSizeKb = -8000
	opts.BusyTimeoutMs = 1234

	dsn, err := opts.DSN(path)
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}

	wantParts := []string{
		"file:",
		"?mode=rwc",
		"&_busy_timeout=1234",
		"&_journal_mode=WAL",
		"&_synchronous=FULL",
		"&_foreign_keys=off",
		"&_pragma=cache_size(-8000)",
		"&_pragma=temp_store(MEMORY)",
		"&_txlock=immediate",
	}
	for _, want := range wantParts {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN() = %q, want it to contain %q", dsn, want)
		}
	}

	// The path must appear in the URI in slash form, including on Windows.
	slashed := filepath.ToSlash(path)
	if !strings.Contains(dsn, slashed) {
		t.Errorf("DSN() = %q, want it to contain the slash form of the path %q", dsn, slashed)
	}
	if strings.Contains(dsn, `\`) {
		t.Errorf("DSN() = %q, must not carry a backslash, which the URI parser treats as a literal name character", dsn)
	}
}

// TestOptionsDSNWindowsAbsolutePath verifies the URI stays absolute on Windows:
// without the added leading slash, "file:C:/..." is read as a drive-relative
// path. On other platforms the same construction has to leave the path alone.
func TestOptionsDSNWindowsAbsolutePath(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metadata.db")
	dsn, err := DefaultOptions().DSN(path)
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}

	slashed := filepath.ToSlash(path)
	wantPrefix := "file:" + slashed + "?mode=rwc"
	if isWindowsDrivePath(slashed) {
		wantPrefix = "file:/" + slashed + "?mode=rwc"
	}
	if !strings.HasPrefix(dsn, wantPrefix) {
		t.Fatalf("DSN() = %q, want prefix %q", dsn, wantPrefix)
	}
	if strings.Contains(dsn, `\`) {
		t.Fatalf("DSN() = %q, must not carry a backslash", dsn)
	}
}

// TestIsWindowsDrivePath pins the drive-designator detection that decides
// whether a leading slash is needed.
func TestIsWindowsDrivePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "upper drive", in: "C:/data/metadata.db", want: true},
		{name: "lower drive", in: "d:/data/metadata.db", want: true},
		{name: "drive relative", in: "C:metadata.db", want: true},
		{name: "unix absolute", in: "/data/metadata.db", want: false},
		{name: "relative", in: "tenant/metadata.db", want: false},
		{name: "single char", in: "C", want: false},
		{name: "empty", in: "", want: false},
		{name: "digit prefix", in: "1:/data/metadata.db", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isWindowsDrivePath(tc.in); got != tc.want {
				t.Fatalf("isWindowsDrivePath(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestOptionsDSNPragmaValuesRoundTrip checks that the values a caller configures
// are the values SQLite reports, which is what makes the DSN the single source
// of connection configuration.
func TestOptionsDSNPragmaValuesRoundTrip(t *testing.T) {
	t.Parallel()

	opts := Options{
		JournalMode:     "truncate",
		SynchronousMode: "extra",
		CacheSizeKb:     -2048,
		BusyTimeoutMs:   750,
	}
	dsn, err := opts.DSN(filepath.Join(t.TempDir(), "round-trip.db"))
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}
	for _, want := range []string{
		"&_busy_timeout=750",
		"&_journal_mode=TRUNCATE",
		"&_synchronous=EXTRA",
		"&_pragma=cache_size(-2048)",
	} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN() = %q, want it to contain %q", dsn, want)
		}
	}
}

// TestOptionsDSNRejects covers the DSN-level argument validation.
func TestOptionsDSNRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts Options
		path string
	}{
		{name: "empty path", opts: DefaultOptions(), path: ""},
		{name: "blank path", opts: DefaultOptions(), path: "   "},
		{name: "path with NUL byte", opts: DefaultOptions(), path: "bad\x00path.db"},
		{name: "invalid options", opts: Options{JournalMode: "NOPE", SynchronousMode: "NORMAL", CacheSizeKb: -4000}, path: "x.db"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dsn, err := tc.opts.DSN(tc.path)
			if err == nil {
				t.Fatalf("DSN(%q) = %q, want an error", tc.path, dsn)
			}
			if !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("DSN(%q) = %v, want a wrapped core.ErrInvalidArgument", tc.path, err)
			}
			if dsn != "" {
				t.Fatalf("DSN(%q) returned %q alongside an error, want an empty string", tc.path, dsn)
			}
		})
	}
}

// TestOptionsDSNAcceptsRelativePath documents that a relative path is passed
// through unchanged, so a caller can rely on the process working directory.
func TestOptionsDSNAcceptsRelativePath(t *testing.T) {
	t.Parallel()

	dsn, err := DefaultOptions().DSN(filepath.Join("tenant-1", "metadata.db"))
	if err != nil {
		t.Fatalf("DSN() = %v, want nil", err)
	}
	if !strings.HasPrefix(dsn, "file:tenant-1/metadata.db?mode=rwc") {
		t.Fatalf("DSN() = %q, want the relative path preserved", dsn)
	}
}
