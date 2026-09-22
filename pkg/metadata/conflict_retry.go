package metadata

import (
	"context"
	"time"
)

const (
	// defaultConflictRetryAttempts is the total number of attempts a repository
	// write makes before it reports the conflict. A SQLite conflict comes from a
	// concurrent writer holding the write lock, so it usually clears after a
	// very short wait.
	defaultConflictRetryAttempts = 3

	// conflictRetryBackoff is the base delay between conflict retries. It grows
	// linearly (one base delay, then two) so a burst of claims does not stay in
	// lockstep.
	conflictRetryBackoff = 5 * time.Millisecond
)

// retryOnConflict runs fn until it succeeds, and returns the last error once the
// attempt budget is exhausted.
//
// attempts is the total number of attempts, not the number of retries; a
// non-positive value selects defaultConflictRetryAttempts. Only a transient
// SQLite lock conflict (SQLITE_BUSY or SQLITE_LOCKED, see
// isSQLiteConflictError) is retried: a domain error such as ErrFileNotFound or
// ErrFileNotClaimable is returned immediately, so contention classification
// stays in the caller. The wait between attempts honours ctx, and a cancelled
// context is reported without further attempts.
func retryOnConflict(ctx context.Context, attempts int, fn func() error) error {
	if attempts < 1 {
		attempts = defaultConflictRetryAttempts
	}

	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		err = fn()
		if err == nil {
			return nil
		}
		if !isSQLiteConflictError(err) {
			return err
		}
		if attempt == attempts-1 {
			break
		}

		timer := time.NewTimer(time.Duration(attempt+1) * conflictRetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return err
}
