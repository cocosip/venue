package cleanup

import (
	"context"
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
	cleanupOrphanedMetadataCalled atomic.Int32
}

func (m *mockCleanupService) CleanupEmptyDirectories(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupEmptyDirsCalled.Add(1)
	return &core.CleanupStatistics{EmptyDirectoriesRemoved: 5}, nil
}

func (m *mockCleanupService) CleanupTimedOutProcessingFiles(ctx context.Context, timeout time.Duration) (*core.CleanupStatistics, error) {
	m.cleanupTimedOutFilesCalled.Add(1)
	return &core.CleanupStatistics{TimedOutFilesReset: 3}, nil
}

func (m *mockCleanupService) CleanupPermanentlyFailedFiles(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupFailedFilesCalled.Add(1)
	return &core.CleanupStatistics{PermanentlyFailedFilesRemoved: 2, SpaceFreed: 1024}, nil
}

func (m *mockCleanupService) CleanupOrphanedMetadata(ctx context.Context) (*core.CleanupStatistics, error) {
	m.cleanupOrphanedMetadataCalled.Add(1)
	return &core.CleanupStatistics{OrphanedMetadataRemoved: 1}, nil
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
		CleanupService:                mockSvc,
		InitialDelay:                  50 * time.Millisecond,
		CleanupInterval:               200 * time.Millisecond,
		CleanupEmptyDirectories:       true,
		CleanupTimedOutFiles:          true,
		CleanupPermanentlyFailedFiles: true,
	})
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	// Start service
	err = bgSvc.Start()
	if err != nil {
		t.Fatalf("Failed to start service: %v", err)
	}
	defer bgSvc.Stop()

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

	bgSvc.Stop()

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
	defer bgSvc.Stop()

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
}
