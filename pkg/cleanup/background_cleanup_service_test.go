package cleanup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
)

// mockCleanupService is a mock implementation of CleanupService for testing.
type mockCleanupService struct {
	cleanupEmptyDirsCalled        atomic.Int32
	cleanupTimedOutFilesCalled    atomic.Int32
	cleanupFailedFilesCalled      atomic.Int32
	failedRetentionNanos          atomic.Int64
	cleanupCompletedFilesCalled   atomic.Int32
	completedRetentionNanos       atomic.Int64
	cleanupOrphanedMetadataCalled atomic.Int32
	optimizeDatabasesCalled       atomic.Int32
	cleanupJunkFilesCalled        atomic.Int32
	invalidBackupsCalled          atomic.Int32
}

func (m *mockCleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupEmptyDirsCalled.Add(1)
	return &core.CleanupStatistics{EmptyDirectoriesRemoved: 5}, nil
}

func (m *mockCleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	m.cleanupTimedOutFilesCalled.Add(1)
	return &core.CleanupStatistics{TimedOutFilesReset: 3}, nil
}

func (m *mockCleanupService) CleanupPermanentlyFailedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	m.cleanupFailedFilesCalled.Add(1)
	m.failedRetentionNanos.Store(int64(retention))
	return &core.CleanupStatistics{PermanentlyFailedFilesRemoved: 2, SpaceFreed: 1024}, nil
}

func (m *mockCleanupService) CleanupCompletedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	m.cleanupCompletedFilesCalled.Add(1)
	m.completedRetentionNanos.Store(int64(retention))
	return &core.CleanupStatistics{CompletedRecordsRemoved: 4, SpaceFreed: 2048}, nil
}

func (m *mockCleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupOrphanedMetadataCalled.Add(1)
	return &core.CleanupStatistics{OrphanedMetadataRemoved: 1}, nil
}

func (m *mockCleanupService) OptimizeDatabases(ctx context.Context) (*core.CleanupStatistics, error) {
	m.optimizeDatabasesCalled.Add(1)
	return &core.CleanupStatistics{
		MetadataDatabasesOptimized: 1,
		QuotaDatabasesOptimized:    1,
	}, nil
}

func (m *mockCleanupService) CleanupJunkFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupJunkFilesCalled.Add(1)
	return &core.CleanupStatistics{JunkFilesRemoved: 7}, nil
}

func (m *mockCleanupService) CleanupInvalidDatabaseBackups(ctx context.Context) (*core.CleanupStatistics, error) {
	m.invalidBackupsCalled.Add(1)
	return &core.CleanupStatistics{InvalidDatabaseBackupsRemoved: 1}, nil
}

func (m *mockCleanupService) CumulativeStatistics() *core.CleanupStatistics {
	return &core.CleanupStatistics{}
}

func TestNewBackgroundCleanupService(t *testing.T) {
	mockSvc := &mockCleanupService{}

	tests := []struct {
		name      string
		opts      *BackgroundCleanupServiceOptions
		wantErr   bool
		errString string
	}{
		{
			name:      "nil options",
			opts:      nil,
			wantErr:   true,
			errString: "options cannot be nil",
		},
		{
			name: "nil cleanup service",
			opts: &BackgroundCleanupServiceOptions{
				CleanupService: nil,
			},
			wantErr:   true,
			errString: "cleanup service cannot be nil",
		},
		{
			name: "valid with defaults",
			opts: &BackgroundCleanupServiceOptions{
				CleanupService: mockSvc,
			},
			wantErr: false,
		},
		{
			name: "valid with custom settings",
			opts: &BackgroundCleanupServiceOptions{
				CleanupService:          mockSvc,
				CleanupInterval:         5 * time.Minute,
				InitialDelay:            30 * time.Second,
				CleanupEmptyDirectories: true,
				CleanupTimedOutFiles:    true,
				ProcessingTimeout:       10 * time.Minute,
				OptimizeDatabases:       false,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewBackgroundCleanupService(tt.opts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got nil")
				}
				if tt.errString != "" && err != nil && err.Error()[:len(tt.errString)] != tt.errString {
					t.Errorf("Expected error containing %q, got %q", tt.errString, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if svc == nil {
					t.Error("Expected service to be non-nil")
				}
				if tt.name == "valid with defaults" && svc.failedFileRetention != 3*24*time.Hour {
					t.Errorf("failed retention = %v, want 72h", svc.failedFileRetention)
				}
			}
		})
	}
}

func TestBackgroundCleanupService_StartStop(t *testing.T) {
	mockSvc := &mockCleanupService{}

	bgSvc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService: mockSvc,
		InitialDelay:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	// Test initial state
	if bgSvc.IsRunning() {
		t.Error("Service should not be running initially")
	}

	// Test start
	err = bgSvc.Start()
	if err != nil {
		t.Errorf("Failed to start service: %v", err)
	}

	if !bgSvc.IsRunning() {
		t.Error("Service should be running after start")
	}

	// Test double start
	err = bgSvc.Start()
	if err == nil {
		t.Error("Expected error when starting already running service")
	}

	// Test stop
	err = bgSvc.Stop()
	if err != nil {
		t.Errorf("Failed to stop service: %v", err)
	}

	if bgSvc.IsRunning() {
		t.Error("Service should not be running after stop")
	}

	// Test double stop
	err = bgSvc.Stop()
	if err == nil {
		t.Error("Expected error when stopping already stopped service")
	}
}

func TestBackgroundCleanupService_CleanupExecution(t *testing.T) {
	mockSvc := &mockCleanupService{}

	bgSvc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                 mockSvc,
		InitialDelay:                   50 * time.Millisecond,
		CleanupInterval:                200 * time.Millisecond,
		CleanupEmptyDirectories:        true,
		CleanupTimedOutFiles:           true,
		CleanupPermanentlyFailedFiles:  true,
		FailedFileRetentionPeriod:      7 * 24 * time.Hour,
		CleanupCompletedRecords:        true,
		CompletedRecordRetentionPeriod: 6 * time.Hour,
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	// Start service
	err = bgSvc.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer func() { _ = bgSvc.Stop() }()

	// Wait for initial delay + first execution
	time.Sleep(100 * time.Millisecond)

	// Check that cleanup methods were called at least once
	if mockSvc.cleanupEmptyDirsCalled.Load() < 1 {
		t.Error("CleanupEmptyDirectories should have been called at least once")
	}
	if mockSvc.cleanupTimedOutFilesCalled.Load() < 1 {
		t.Error("CleanupTimedOutProcessingFiles should have been called at least once")
	}
	if mockSvc.cleanupFailedFilesCalled.Load() < 1 {
		t.Error("CleanupPermanentlyFailedFiles should have been called at least once")
	}
	if got := time.Duration(mockSvc.failedRetentionNanos.Load()); got != 7*24*time.Hour {
		t.Errorf("CleanupPermanentlyFailedFiles retention = %v, want %v", got, 7*24*time.Hour)
	}
	if mockSvc.cleanupCompletedFilesCalled.Load() < 1 {
		t.Error("CleanupCompletedFiles should have been called at least once")
	}
	if got := time.Duration(mockSvc.completedRetentionNanos.Load()); got != 6*time.Hour {
		t.Errorf("CleanupCompletedFiles retention = %v, want %v", got, 6*time.Hour)
	}

	// Wait for another cycle
	time.Sleep(250 * time.Millisecond)

	// Check that cleanup methods were called multiple times
	if mockSvc.cleanupEmptyDirsCalled.Load() < 2 {
		t.Errorf("Expected at least 2 calls to CleanupEmptyDirectories, got %d",
			mockSvc.cleanupEmptyDirsCalled.Load())
	}
}

func TestBackgroundCleanupService_SelectiveCleanup(t *testing.T) {
	mockSvc := &mockCleanupService{}

	// Test with only empty directories cleanup enabled
	bgSvc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                mockSvc,
		InitialDelay:                  50 * time.Millisecond,
		CleanupInterval:               500 * time.Millisecond,
		CleanupEmptyDirectories:       true,
		CleanupTimedOutFiles:          false,
		CleanupPermanentlyFailedFiles: false,
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	err = bgSvc.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}

	// Wait for first execution
	time.Sleep(100 * time.Millisecond)

	_ = bgSvc.Stop()

	// Only empty directories cleanup should have been called
	if mockSvc.cleanupEmptyDirsCalled.Load() < 1 {
		t.Error("CleanupEmptyDirectories should have been called")
	}
	if mockSvc.cleanupTimedOutFilesCalled.Load() != 0 {
		t.Error("CleanupTimedOutProcessingFiles should not have been called")
	}
	if mockSvc.cleanupFailedFilesCalled.Load() != 0 {
		t.Error("CleanupPermanentlyFailedFiles should not have been called")
	}
}

func TestBackgroundCleanupService_GracefulShutdown(t *testing.T) {
	mockSvc := &mockCleanupService{}

	bgSvc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:  mockSvc,
		InitialDelay:    50 * time.Millisecond,
		CleanupInterval: 1 * time.Hour, // Long interval
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	err = bgSvc.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Stop should complete quickly
	stopStart := time.Now()
	err = bgSvc.Stop()
	stopDuration := time.Since(stopStart)

	if err != nil {
		t.Errorf("Failed to stop service: %v", err)
	}

	if stopDuration > 1*time.Second {
		t.Errorf("Stop took too long: %v", stopDuration)
	}

	if bgSvc.IsRunning() {
		t.Error("Service should not be running after stop")
	}
}

// blockingCleanupService blocks inside the first cleanup step until the test
// releases it, which pins the run goroutine inside an in-flight cycle.
type blockingCleanupService struct {
	entered        chan struct{}
	release        chan struct{}
	enteredOnce    sync.Once
	emptyDirsCalls atomic.Int32
}

func newBlockingCleanupService() *blockingCleanupService {
	return &blockingCleanupService{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (m *blockingCleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	m.emptyDirsCalls.Add(1)
	m.enteredOnce.Do(func() { close(m.entered) })
	select {
	case <-m.release:
	case <-ctx.Done():
	}
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupPermanentlyFailedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupCompletedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) OptimizeDatabases(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupJunkFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CleanupInvalidDatabaseBackups(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *blockingCleanupService) CumulativeStatistics() *core.CleanupStatistics {
	return &core.CleanupStatistics{}
}

// TestBackgroundCleanupService_StopDuringInFlightCycle is the regression test for
// the Stop()/run() lock-order deadlock: Stop() must not hold s.mu while it waits
// for the run goroutine, because the goroutine takes s.mu.RLock() through
// shouldOptimizeDatabases().
func TestBackgroundCleanupService_StopDuringInFlightCycle(t *testing.T) {
	blocking := newBlockingCleanupService()

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          blocking,
		InitialDelay:            time.Microsecond,
		CleanupInterval:         time.Hour,
		CleanupEmptyDirectories: true,
		OptimizeDatabases:       true,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Wait until the run goroutine is parked inside the cleanup cycle.
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		close(blocking.release)
		t.Fatal("cleanup cycle never started")
	}

	// Release the cycle only after Stop() has been called, which is exactly the
	// window where a lock held across wg.Wait() deadlocks against the cycle's
	// s.mu.RLock() in shouldOptimizeDatabases().
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(blocking.release)
	}()

	done := make(chan error, 1)
	go func() { done <- svc.Stop() }()

	var stopErr error
	select {
	case stopErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s while a cleanup cycle was in flight: deadlock")
	}

	if stopErr != nil {
		t.Fatalf("Stop() error = %v", stopErr)
	}
	if svc.IsRunning() {
		t.Error("service should not report running after Stop()")
	}
}

// TestNewBackgroundCleanupService_RespectsExplicitFalseFlags verifies that a
// caller can disable every cleanup category; the constructor must not rewrite an
// explicit all-false configuration into all-true.
func TestNewBackgroundCleanupService_RespectsExplicitFalseFlags(t *testing.T) {
	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                &mockCleanupService{},
		CleanupEmptyDirectories:       false,
		CleanupTimedOutFiles:          false,
		CleanupPermanentlyFailedFiles: false,
		CleanupCompletedRecords:       false,
		OptimizeDatabases:             false,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if svc.cleanupEmptyDirectories {
		t.Error("cleanupEmptyDirectories = true, want explicit false")
	}
	if svc.cleanupTimedOutFiles {
		t.Error("cleanupTimedOutFiles = true, want explicit false")
	}
	if svc.cleanupPermanentlyFailedFiles {
		t.Error("cleanupPermanentlyFailedFiles = true, want explicit false")
	}
	if svc.cleanupCompletedRecords {
		t.Error("cleanupCompletedRecords = true, want explicit false")
	}
	if svc.optimizeDatabases {
		t.Error("optimizeDatabases = true, want explicit false")
	}
}

// TestBackgroundCleanupService_AllFlagsFalseSkipsEveryStep verifies that an
// all-false configuration performs no cleanup work at all.
func TestBackgroundCleanupService_AllFlagsFalseSkipsEveryStep(t *testing.T) {
	mockSvc := &mockCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                mockSvc,
		InitialDelay:                  10 * time.Millisecond,
		CleanupInterval:               50 * time.Millisecond,
		CleanupEmptyDirectories:       false,
		CleanupTimedOutFiles:          false,
		CleanupPermanentlyFailedFiles: false,
		CleanupCompletedRecords:       false,
		OptimizeDatabases:             false,
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

	if got := mockSvc.cleanupEmptyDirsCalled.Load(); got != 0 {
		t.Errorf("CleanupEmptyDirectories calls = %d, want 0", got)
	}
	if got := mockSvc.cleanupTimedOutFilesCalled.Load(); got != 0 {
		t.Errorf("CleanupTimedOutProcessingFiles calls = %d, want 0", got)
	}
	if got := mockSvc.cleanupFailedFilesCalled.Load(); got != 0 {
		t.Errorf("CleanupPermanentlyFailedFiles calls = %d, want 0", got)
	}
	if got := mockSvc.cleanupCompletedFilesCalled.Load(); got != 0 {
		t.Errorf("CleanupCompletedFiles calls = %d, want 0", got)
	}
	if got := mockSvc.optimizeDatabasesCalled.Load(); got != 0 {
		t.Errorf("OptimizeDatabases calls = %d, want 0", got)
	}
	if got := mockSvc.cleanupOrphanedMetadataCalled.Load(); got != 0 {
		t.Errorf("CleanupOrphanedMetadata calls = %d, want 0", got)
	}
}

// TestBackgroundCleanupService_OrphanedMetadataCleanupIsOptIn verifies the
// orphaned-metadata step runs on the cycle only when it is enabled.
func TestBackgroundCleanupService_OrphanedMetadataCleanupIsOptIn(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		want    int32
	}{
		{name: "enabled", enabled: true, want: 1},
		{name: "disabled", enabled: false, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockSvc := &mockCleanupService{}
			svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
				CleanupService:          mockSvc,
				InitialDelay:            10 * time.Millisecond,
				CleanupInterval:         time.Hour, // exactly one cycle after the delay
				CleanupOrphanedMetadata: test.enabled,
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

			if got := mockSvc.cleanupOrphanedMetadataCalled.Load(); got != test.want {
				t.Errorf("CleanupOrphanedMetadata calls = %d, want %d", got, test.want)
			}
		})
	}
}

// panicCleanupService panics on the first cleanup step and then succeeds.
type panicCleanupService struct {
	panicOnce      sync.Once
	panicked       atomic.Bool
	emptyDirsCalls atomic.Int32
}

func (m *panicCleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	m.emptyDirsCalls.Add(1)
	shouldPanic := false
	m.panicOnce.Do(func() {
		shouldPanic = true
		m.panicked.Store(true)
	})
	if shouldPanic {
		panic("simulated cleanup failure")
	}
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupPermanentlyFailedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupCompletedFiles(ctx context.Context, retention time.Duration) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) OptimizeDatabases(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupJunkFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CleanupInvalidDatabaseBackups(ctx context.Context) (*core.CleanupStatistics, error) {
	return &core.CleanupStatistics{}, nil
}

func (m *panicCleanupService) CumulativeStatistics() *core.CleanupStatistics {
	return &core.CleanupStatistics{}
}

// TestBackgroundCleanupService_PanicIsContained verifies that a panic inside one
// cleanup pass is logged and contained instead of killing the process, and that
// later cycles still run.
func TestBackgroundCleanupService_PanicIsContained(t *testing.T) {
	panicking := &panicCleanupService{}

	svc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:          panicking,
		InitialDelay:            time.Microsecond,
		CleanupInterval:         20 * time.Millisecond,
		CleanupEmptyDirectories: true,
	})
	if err != nil {
		t.Fatalf("NewBackgroundCleanupService() error = %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// The loop must survive the panic and keep scheduling later cycles.
	deadline := time.Now().Add(3 * time.Second)
	for panicking.emptyDirsCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if !panicking.panicked.Load() {
		t.Fatal("the panicking cleanup pass never ran")
	}
	if got := panicking.emptyDirsCalls.Load(); got < 2 {
		t.Errorf("cleanup cycles after the panic = %d, want at least 2 (the loop must keep running)", got)
	}
}

func TestBackgroundCleanupService_DatabaseOptimization(t *testing.T) {
	mockSvc := &mockCleanupService{}

	bgSvc, err := NewBackgroundCleanupService(&BackgroundCleanupServiceOptions{
		CleanupService:                mockSvc,
		InitialDelay:                  50 * time.Millisecond,
		CleanupInterval:               100 * time.Millisecond,
		OptimizeDatabases:             true,
		DatabaseOptimizationInterval:  200 * time.Millisecond,
		CleanupEmptyDirectories:       false,
		CleanupTimedOutFiles:          false,
		CleanupPermanentlyFailedFiles: false,
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	err = bgSvc.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer func() { _ = bgSvc.Stop() }()

	// Wait for optimization to happen (should happen on first run and after interval)
	time.Sleep(400 * time.Millisecond)

	// Check that optimization was performed (shouldOptimizeDatabases should return false
	// because optimization was performed recently)
	if bgSvc.shouldOptimizeDatabases() {
		t.Error("shouldOptimizeDatabases should return false after recent optimization")
	}

	// Verify lastOptimizationTime is set
	if bgSvc.lastOptimizationTime.IsZero() {
		t.Error("lastOptimizationTime should be set after optimization")
	}

	if mockSvc.optimizeDatabasesCalled.Load() < 1 {
		t.Error("OptimizeDatabases should have been called at least once")
	}
}
