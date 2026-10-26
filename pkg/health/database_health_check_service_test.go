package health

import (
	"context"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// idleHealthChecker is a DatabaseHealthChecker whose checks always succeed.
type idleHealthChecker struct{}

func (idleHealthChecker) CheckAllDatabases(context.Context) (*core.DatabaseHealthReport, error) {
	return &core.DatabaseHealthReport{AllHealthy: true}, nil
}

func (idleHealthChecker) CheckMetadataDatabase(context.Context, string) (*core.DatabaseHealthStatus, error) {
	return &core.DatabaseHealthStatus{IsHealthy: true}, nil
}

func (idleHealthChecker) CheckDirectoryQuotaDatabase(context.Context) (*core.DatabaseHealthStatus, error) {
	return &core.DatabaseHealthStatus{IsHealthy: true}, nil
}

func (idleHealthChecker) DetectOrphanedFiles(context.Context) ([]string, error) {
	return nil, nil
}

// TestStopAfterStartupOnlyCompletionReturnsNil pins the documented lifecycle of
// CheckOnStartupOnly: the run goroutine ends by itself after the startup check,
// so a caller that stops the service afterwards performed a legitimate stop and
// must not receive a "not running" error that Venue would log as a stop failure.
func TestStopAfterStartupOnlyCompletionReturnsNil(t *testing.T) {
	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: idleHealthChecker{},
		InitialDelay:          time.Millisecond,
		CheckOnStartupOnly:    true,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for service.IsRunning() {
		if time.Now().After(deadline) {
			t.Fatal("the startup-only service never finished its run loop")
		}
		time.Sleep(time.Millisecond)
	}

	if err := service.Stop(); err != nil {
		t.Errorf("Stop() after natural completion error = %v, want nil", err)
	}

	// A repeated stop stays a no-op rather than an error: the service is in the
	// same "finished" state it was after the first Stop.
	if err := service.Stop(); err != nil {
		t.Errorf("repeated Stop() error = %v, want nil", err)
	}
}

// TestStopWhileRunningReturnsNil keeps the regular lifecycle covered: a service
// that is still running stops without an error.
func TestStopWhileRunningReturnsNil(t *testing.T) {
	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: idleHealthChecker{},
		InitialDelay:          time.Hour,
		CheckOnStartupOnly:    true,
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := service.Stop(); err != nil {
		t.Errorf("Stop() error = %v, want nil", err)
	}
	if service.IsRunning() {
		t.Error("IsRunning() = true after Stop(), want false")
	}
}

// TestStopWithoutStartIsAnError keeps the misuse signal: stopping a service that
// was never started is a programming error and stays reported.
func TestStopWithoutStartIsAnError(t *testing.T) {
	service, err := NewDatabaseHealthCheckService(&DatabaseHealthCheckServiceOptions{
		DatabaseHealthChecker: idleHealthChecker{},
	})
	if err != nil {
		t.Fatalf("NewDatabaseHealthCheckService() error = %v", err)
	}
	if err := service.Stop(); err == nil {
		t.Error("Stop() without Start() = nil error, want an error")
	}
}
