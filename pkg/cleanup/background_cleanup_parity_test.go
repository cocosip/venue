package cleanup

import (
	"testing"
	"time"
)

// TestNewBackgroundCleanupService_JunkIntervalDefaultsTo20Minutes pins the
// documented default cadence for the junk-file sweep.
func TestNewBackgroundCleanupService_JunkIntervalDefaultsTo20Minutes(t *testing.T) {
	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService: &mockCleanupService{},
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}
	if svc.junkFileCleanupInterval != 20*time.Minute {
		t.Errorf("junkFileCleanupInterval = %v, want 20m", svc.junkFileCleanupInterval)
	}
	if !svc.lastJunkFileCleanup.IsZero() {
		t.Errorf("lastJunkFileCleanup = %v, want the zero time so the first cycle sweeps", svc.lastJunkFileCleanup)
	}
}

// TestBackgroundCleanupService_JunkSweepThrottledByInterval verifies the junk
// sweep runs on its own cadence instead of every cleanup cycle, exactly like the
// database-optimization throttle.
func TestBackgroundCleanupService_JunkSweepThrottledByInterval(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          mockSvc,
		InitialDelay:            5 * time.Millisecond,
		CleanupInterval:         15 * time.Millisecond,
		CleanupJunkFiles:        true,
		JunkFileCleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := mockSvc.cleanupJunkFilesCalled.Load(); got != 1 {
		t.Errorf("CleanupJunkFiles calls = %d, want 1 (throttled by a 1h interval)", got)
	}
	if svc.lastJunkFileCleanup.IsZero() {
		t.Error("lastJunkFileCleanup was not recorded after a successful sweep")
	}
}

// TestBackgroundCleanupService_JunkSweepRunsEveryCycleWithNonPositiveInterval
// verifies a non-positive interval disables the throttle.
func TestBackgroundCleanupService_JunkSweepRunsEveryCycleWithNonPositiveInterval(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          mockSvc,
		InitialDelay:            5 * time.Millisecond,
		CleanupInterval:         20 * time.Millisecond,
		CleanupJunkFiles:        true,
		JunkFileCleanupInterval: -time.Second,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := mockSvc.cleanupJunkFilesCalled.Load(); got < 2 {
		t.Errorf("CleanupJunkFiles calls = %d, want at least 2", got)
	}
}

// TestBackgroundCleanupService_NewSweepsHonourExplicitFalse verifies both new
// steps are skipped when they are disabled.
func TestBackgroundCleanupService_NewSweepsHonourExplicitFalse(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                mockSvc,
		InitialDelay:                  5 * time.Millisecond,
		CleanupInterval:               20 * time.Millisecond,
		CleanupJunkFiles:              false,
		CleanupInvalidDatabaseBackups: false,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := mockSvc.cleanupJunkFilesCalled.Load(); got != 0 {
		t.Errorf("CleanupJunkFiles calls = %d, want 0", got)
	}
	if got := mockSvc.invalidBackupsCalled.Load(); got != 0 {
		t.Errorf("CleanupInvalidDatabaseBackups calls = %d, want 0", got)
	}
}

// TestBackgroundCleanupService_InvalidBackupSweepRunsOnEveryCycle verifies the
// invalid-backup sweep follows its flag and is not throttled.
func TestBackgroundCleanupService_InvalidBackupSweepRunsOnEveryCycle(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                mockSvc,
		InitialDelay:                  5 * time.Millisecond,
		CleanupInterval:               20 * time.Millisecond,
		CleanupInvalidDatabaseBackups: true,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if got := mockSvc.invalidBackupsCalled.Load(); got < 2 {
		t.Errorf("CleanupInvalidDatabaseBackups calls = %d, want at least 2", got)
	}
}
