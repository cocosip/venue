package metadata

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestConcurrentTenantOpenFailureReleasesEveryWaiter is the regression test for a
// reservation protocol that only closed the pending-open channel on the success
// path.
//
// Two callers reach the same tenant at once. The first reserves the open, the
// second blocks on the reservation. When the open fails, the reservation must be
// released: otherwise the second caller sits on that channel until its own
// context expires, so one failed open hangs every concurrent reader of the
// tenant instead of returning the same error to all of them.
func TestConcurrentTenantOpenFailureReleasesEveryWaiter(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)

	const tenantID = "concurrent-tenant"

	openErr := errors.New("injected open failure")
	var once sync.Once
	openStarted := make(chan struct{})

	// The first caller's open blocks until the test releases it, which pins the
	// second caller on the reservation channel and makes the ordering
	// deterministic instead of racing.
	release := make(chan struct{})
	repo.openTenantDatabase = func(context.Context, string) (*sql.DB, string, error) {
		once.Do(func() { close(openStarted) })
		<-release
		return nil, "", openErr
	}
	_ = openStarted

	winnerErr := make(chan error, 1)
	go func() {
		_, err := repo.begin(context.Background(), tenantID)
		winnerErr <- err
	}()

	// Wait until the winner holds the reservation before the second caller
	// arrives, so it genuinely takes the waiter branch.
	deadline := time.Now().Add(5 * time.Second)
	for {
		repo.mu.RLock()
		_, reserved := repo.openings[tenantID]
		repo.mu.RUnlock()
		if reserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first caller never reserved the open")
		}
		time.Sleep(time.Millisecond)
	}

	waiterDone := make(chan error, 1)
	go func() {
		waiterCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := repo.begin(waiterCtx, tenantID)
		waiterDone <- err
	}()

	// Let the winner's open fail and resolve the reservation.
	close(release)

	select {
	case err := <-winnerErr:
		if !errors.Is(err, openErr) {
			t.Fatalf("winner error = %v, want the injected open failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the winner never returned: its own open did not resolve")
	}

	select {
	case err := <-waiterDone:
		// The waiter retries the lookup after the reservation resolves, finds no
		// handle, and reaches the real open for this tenant. Any error is
		// acceptable as long as it returns promptly: the defect is the hang, not
		// the verdict.
		_ = err
	case <-time.After(10 * time.Second):
		t.Fatal("the waiter is still blocked after the reserved open failed: the reservation was never released")
	}
}

// TestFailedTenantOpenReleasesReservation keeps the narrower invariant explicit:
// after a failed open the repository holds no stale reservation, so the next
// caller starts a fresh open instead of inheriting a channel nobody will close.
func TestFailedTenantOpenReleasesReservation(t *testing.T) {
	repo := newSQLiteFixtureRepository(t, nil)

	const tenantID = "cleared-tenant"
	openErr := errors.New("injected open failure")
	repo.openTenantDatabase = func(context.Context, string) (*sql.DB, string, error) {
		return nil, "", openErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := repo.begin(ctx, tenantID); !errors.Is(err, openErr) {
		t.Fatalf("begin() error = %v, want the injected open failure", err)
	}

	repo.mu.RLock()
	_, reserved := repo.openings[tenantID]
	repo.mu.RUnlock()
	if reserved {
		t.Fatal("a failed open left its reservation behind, so the next caller would wait on it")
	}
}
