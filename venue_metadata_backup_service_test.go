package venue

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// stubBackupRepository is a metadata repository that supports per-tenant backups
// without touching a database, so a lifecycle test can drive the backup runner
// directly.
type stubBackupRepository struct {
	core.MetadataRepository

	backedUp sync.Map
}

func (r *stubBackupRepository) KnownTenantIDs(context.Context) ([]string, error) {
	return []string{"tenant-a"}, nil
}

func (r *stubBackupRepository) BackupTenant(_ context.Context, tenantID string, _ string) error {
	r.backedUp.Store(tenantID, struct{}{})
	return nil
}

// TestMetadataBackupServiceDoesNotLeakGoroutinesPerCycle is the regression test
// for the backup loop: every tick used to spawn a goroutine parked on stopCh that
// only returned when Stop closed it, so a long-lived runtime accumulated one
// leaked goroutine per backup cycle.
func TestMetadataBackupServiceDoesNotLeakGoroutinesPerCycle(t *testing.T) {
	service, err := newMetadataBackupService(
		&stubBackupRepository{},
		t.TempDir(),
		time.Millisecond,
		0,
		nil,
	)
	if err != nil {
		t.Fatalf("newMetadataBackupService() error = %v", err)
	}

	// Let the baseline settle before counting.
	runtime.GC()
	before := runtime.NumGoroutine()

	service.Start()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	during := runtime.NumGoroutine()

	service.Stop()

	// The loop goroutine itself is the only expected addition while running.
	if during-before > 2 {
		t.Fatalf("goroutines grew from %d to %d while the backup loop ran, want at most one loop goroutine", before, during)
	}

	// After Stop everything the service created must be gone.
	stopped := time.Now().Add(2 * time.Second)
	for {
		after := runtime.NumGoroutine()
		if after <= before {
			break
		}
		if time.Now().After(stopped) {
			t.Fatalf("goroutines after Stop = %d, want at most the %d baseline", after, before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMetadataBackupServiceRunOnceBacksUpEveryTenant keeps the cycle behaviour
// covered now that the loop no longer spawns a watcher goroutine per tick.
func TestMetadataBackupServiceRunOnceBacksUpEveryTenant(t *testing.T) {
	repo := &stubBackupRepository{}
	service, err := newMetadataBackupService(repo, t.TempDir(), time.Hour, 0, nil)
	if err != nil {
		t.Fatalf("newMetadataBackupService() error = %v", err)
	}

	if err := service.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if _, ok := repo.backedUp.Load("tenant-a"); !ok {
		t.Fatal("RunOnce() did not back up the only known tenant")
	}
}

// TestMetadataBackupServiceStopCancelsInFlightCycle proves a cycle started before
// Stop is interrupted instead of being waited out, which is why the cycle needs a
// cancellable context rather than a fresh background one.
func TestMetadataBackupServiceStopCancelsInFlightCycle(t *testing.T) {
	blocking := &blockingBackupRepository{started: make(chan struct{})}
	service, err := newMetadataBackupService(blocking, t.TempDir(), time.Millisecond, 0, nil)
	if err != nil {
		t.Fatalf("newMetadataBackupService() error = %v", err)
	}

	service.Start()
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the backup cycle never started")
	}

	stopped := make(chan struct{})
	go func() {
		service.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not interrupt the in-flight backup cycle")
	}
}

// blockingBackupRepository blocks inside BackupTenant until its context is
// cancelled, so Stop must cancel the cycle rather than wait for the tenant copy.
type blockingBackupRepository struct {
	core.MetadataRepository

	started chan struct{}
	once    sync.Once
}

func (r *blockingBackupRepository) KnownTenantIDs(context.Context) ([]string, error) {
	return []string{"tenant-a"}, nil
}

func (r *blockingBackupRepository) BackupTenant(ctx context.Context, _ string, _ string) error {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	return ctx.Err()
}
