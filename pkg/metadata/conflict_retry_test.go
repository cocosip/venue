package metadata

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/sqlite"
	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// sqliteBusyError produces a genuine SQLITE_BUSY failure: one connection holds
// the write lock of a database file and a second one tries to write it with the
// lock wait disabled.
//
// The retry trigger is a property of the engine's result code, so the tests
// drive the real engine instead of a hand-built error value.
func sqliteBusyError(t *testing.T) error {
	t.Helper()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "contended.db")

	options := sqlite.DefaultOptions()
	options.BusyTimeoutMs = 0 // fail immediately instead of waiting out the lock

	holder, err := sqlite.Open(path, options)
	if err != nil {
		t.Fatalf("sqlite.Open(holder) error = %v", err)
	}
	t.Cleanup(func() { _ = holder.Close() })

	contender, err := sqlite.Open(path, options)
	if err != nil {
		t.Fatalf("sqlite.Open(contender) error = %v", err)
	}
	t.Cleanup(func() { _ = contender.Close() })

	if _, err := holder.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS contended (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}

	locked, err := holder.Conn(ctx)
	if err != nil {
		t.Fatalf("holder.Conn() error = %v", err)
	}
	defer func() { _ = locked.Close() }()

	// BEGIN IMMEDIATE matches the repository's own _txlock policy: the write
	// lock is taken when the transaction starts.
	if _, err := locked.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("BEGIN IMMEDIATE error = %v", err)
	}
	defer func() { _, _ = locked.ExecContext(ctx, "ROLLBACK") }()

	_, writeErr := contender.ExecContext(ctx, "INSERT INTO contended (id) VALUES (1)")
	if writeErr == nil {
		t.Fatal("a write behind a held write lock succeeded, want SQLITE_BUSY")
	}
	return writeErr
}

// TestIsSQLiteConflictErrorIsStructural pins the retry trigger: the engine's
// SQLITE_BUSY result code is what makes a write retryable, and a failure that is
// not a transient lock stays untouched.
func TestIsSQLiteConflictErrorIsStructural(t *testing.T) {
	busy := sqliteBusyError(t)

	var driverErr *sqlitedriver.Error
	if !errors.As(busy, &driverErr) {
		t.Fatalf("contended write error = %v, want a *sqlite.Error", busy)
	}
	if code := driverErr.Code() & 0xff; code != sqlite3.SQLITE_BUSY {
		t.Fatalf("contended write result code = %d, want SQLITE_BUSY (%d)", code, sqlite3.SQLITE_BUSY)
	}

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "engine busy", err: busy, want: true},
		{name: "wrapped engine busy", err: fmt.Errorf("commit failed: %w", busy), want: true},
		{name: "busy message", err: errors.New("database is locked (5) (SQLITE_BUSY)"), want: true},
		{name: "locked message", err: errors.New("database table is locked"), want: true},
		{name: "corruption message", err: errors.New("database disk image is malformed (11)"), want: false},
		{name: "permission message", err: errors.New("attempt to write a readonly database (8)"), want: false},
		{name: "domain missing record", err: core.ErrFileNotFound, want: false},
		{name: "domain contention", err: core.ErrFileNotClaimable, want: false},
		{name: "domain database error", err: core.ErrDatabaseError, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSQLiteConflictError(tc.err); got != tc.want {
				t.Fatalf("isSQLiteConflictError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestRetryOnConflictBoundsAttempts pins the helper contract: a conflicting
// transaction is retried a bounded number of times, a success stops the loop,
// and the last error is returned when the budget is exhausted.
func TestRetryOnConflictBoundsAttempts(t *testing.T) {
	busy := sqliteBusyError(t)

	testCases := []struct {
		name      string
		attempts  int
		conflicts int32
		wantCalls int32
		wantError bool
	}{
		{name: "first attempt succeeds", attempts: 3, conflicts: 0, wantCalls: 1},
		{name: "one conflict then success", attempts: 3, conflicts: 1, wantCalls: 2},
		{name: "two conflicts then success", attempts: 3, conflicts: 2, wantCalls: 3},
		{name: "budget exhausted", attempts: 3, conflicts: 99, wantCalls: 3, wantError: true},
		{name: "single attempt never retries", attempts: 1, conflicts: 99, wantCalls: 1, wantError: true},
		{name: "non-positive attempts fall back to the default", attempts: 0, conflicts: 99, wantCalls: 3, wantError: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int32
			err := retryOnConflict(context.Background(), tc.attempts, func() error {
				if atomic.AddInt32(&calls, 1) <= tc.conflicts {
					return busy
				}
				return nil
			})

			if got := atomic.LoadInt32(&calls); got != tc.wantCalls {
				t.Fatalf("attempts = %d, want %d", got, tc.wantCalls)
			}
			if tc.wantError {
				if !errors.Is(err, busy) {
					t.Fatalf("error = %v, want %v", err, busy)
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
		})
	}
}

// TestRetryOnConflictRetriesWrappedConflicts covers the repository shape where
// the engine's lock conflict is wrapped before it reaches the helper.
func TestRetryOnConflictRetriesWrappedConflicts(t *testing.T) {
	busy := sqliteBusyError(t)

	var calls int32
	err := retryOnConflict(context.Background(), 3, func() error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return fmt.Errorf("failed to save metadata: %w", busy)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
}

// TestRetryOnConflictReturnsTheLastError confirms a caller can still classify
// the final failure after the retry budget is exhausted.
func TestRetryOnConflictReturnsTheLastError(t *testing.T) {
	busy := sqliteBusyError(t)

	var calls int
	err := retryOnConflict(context.Background(), 3, func() error {
		calls++
		return fmt.Errorf("attempt %d: %w", calls, busy)
	})

	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
	if !errors.Is(err, busy) {
		t.Fatalf("error = %v, want a wrapped conflict", err)
	}
	if !strings.Contains(err.Error(), "attempt 3") {
		t.Fatalf("error = %v, want the last failure", err)
	}
}

// TestRetryOnConflictDoesNotRetryOrdinaryErrors keeps unrelated failures (for
// example a missing record or a domain rejection) from being retried.
func TestRetryOnConflictDoesNotRetryOrdinaryErrors(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "missing record", err: core.ErrFileNotFound},
		{name: "claim contention", err: core.ErrFileNotClaimable},
		{name: "lease mismatch", err: core.ErrProcessingLeaseMismatch},
		{name: "invalid argument", err: core.ErrInvalidArgument},
		{name: "infrastructure failure", err: core.ErrDatabaseError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int32
			err := retryOnConflict(context.Background(), 3, func() error {
				atomic.AddInt32(&calls, 1)
				return tc.err
			})

			if calls != 1 {
				t.Fatalf("attempts = %d, want 1", calls)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
		})
	}
}

// TestRetryOnConflictHonoursContextCancellation makes sure a cancelled caller
// never keeps writing, including while the helper backs off.
func TestRetryOnConflictHonoursContextCancellation(t *testing.T) {
	busy := sqliteBusyError(t)

	t.Run("cancelled before the first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var calls int32
		err := retryOnConflict(ctx, 3, func() error {
			atomic.AddInt32(&calls, 1)
			return nil
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want %v", err, context.Canceled)
		}
		if calls != 0 {
			t.Fatalf("attempts = %d, want 0 for an already-cancelled context", calls)
		}
	})

	t.Run("cancelled while backing off", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var calls int32
		err := retryOnConflict(ctx, 3, func() error {
			if atomic.AddInt32(&calls, 1) == 1 {
				cancel()
			}
			return busy
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want %v", err, context.Canceled)
		}
		if calls != 1 {
			t.Fatalf("attempts = %d, want 1: the helper must stop backing off once the context is done", calls)
		}
	})
}
