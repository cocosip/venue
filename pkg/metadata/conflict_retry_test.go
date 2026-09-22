package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cocosip/venue/pkg/core"
	"github.com/dgraph-io/badger/v4"
)

// TestRetryOnConflictBoundsAttempts pins the helper contract: a conflicting
// transaction is retried a bounded number of times, a success stops the loop,
// and the last error is returned when the budget is exhausted.
func TestRetryOnConflictBoundsAttempts(t *testing.T) {
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
					return badger.ErrConflict
				}
				return nil
			})

			if got := atomic.LoadInt32(&calls); got != tc.wantCalls {
				t.Fatalf("attempts = %d, want %d", got, tc.wantCalls)
			}
			if tc.wantError {
				if !errors.Is(err, badger.ErrConflict) {
					t.Fatalf("error = %v, want %v", err, badger.ErrConflict)
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
// BadgerDB's conflict is wrapped before it reaches the helper.
func TestRetryOnConflictRetriesWrappedConflicts(t *testing.T) {
	var calls int32
	err := retryOnConflict(context.Background(), 3, func() error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return fmt.Errorf("failed to save metadata: %w", badger.ErrConflict)
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
	var calls int
	err := retryOnConflict(context.Background(), 3, func() error {
		calls++
		return fmt.Errorf("attempt %d: %w", calls, badger.ErrConflict)
	})

	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
	if !errors.Is(err, badger.ErrConflict) {
		t.Fatalf("error = %v, want a wrapped %v", err, badger.ErrConflict)
	}
	if !strings.Contains(err.Error(), "attempt 3") {
		t.Fatalf("error = %v, want the last failure", err)
	}
}

// TestRetryOnConflictDoesNotRetryOrdinaryErrors keeps unrelated failures (for
// example a missing record or a domain rejection) from being retried.
func TestRetryOnConflictDoesNotRetryOrdinaryErrors(t *testing.T) {
	var calls int32
	err := retryOnConflict(context.Background(), 3, func() error {
		atomic.AddInt32(&calls, 1)
		return core.ErrFileNotFound
	})

	if calls != 1 {
		t.Fatalf("attempts = %d, want 1", calls)
	}
	if !errors.Is(err, core.ErrFileNotFound) {
		t.Fatalf("error = %v, want %v", err, core.ErrFileNotFound)
	}
}

// TestRetryOnConflictHonoursContextCancellation makes sure a cancelled caller
// never keeps writing, including while the helper backs off.
func TestRetryOnConflictHonoursContextCancellation(t *testing.T) {
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
			return badger.ErrConflict
		})

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want %v", err, context.Canceled)
		}
		if calls != 1 {
			t.Fatalf("attempts = %d, want 1: the helper must stop backing off once the context is done", calls)
		}
	})
}
