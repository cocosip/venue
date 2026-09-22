package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// The concrete manager must expose both the base quota interface and the
// optional administration capability so a caller that holds a
// core.TenantQuotaManager can type-assert the latter.
var (
	_ core.TenantQuotaManager       = (*TenantQuotaManager)(nil)
	_ core.TenantQuotaAdministrator = (*TenantQuotaManager)(nil)
)

// TestNewTenantQuotaManagerGlobalLimit covers construction: a positive first
// argument becomes the global limit, anything else keeps it unlimited.
func TestNewTenantQuotaManagerGlobalLimit(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		args       []int
		wantGlobal int
	}{
		{name: "no argument is unlimited", args: nil, wantGlobal: 0},
		{name: "zero is unlimited", args: []int{0}, wantGlobal: 0},
		{name: "negative is ignored", args: []int{-5}, wantGlobal: 0},
		{name: "positive becomes the global limit", args: []int{4}, wantGlobal: 4},
		{name: "first positive argument wins", args: []int{4, 9}, wantGlobal: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewTenantQuotaManager(tt.args...)
			if manager == nil {
				t.Fatal("NewTenantQuotaManager() = nil, want usable manager")
			}

			got, err := manager.GetGlobalLimit(ctx)
			if err != nil {
				t.Fatalf("GetGlobalLimit() error = %v", err)
			}
			if got != tt.wantGlobal {
				t.Fatalf("GetGlobalLimit() = %d, want %d", got, tt.wantGlobal)
			}

			// A manager built with no argument must still enforce the global
			// limit dynamically.
			if err := manager.SetGlobalLimit(ctx, 1); err != nil {
				t.Fatalf("SetGlobalLimit() error = %v", err)
			}
			if err := manager.IncrementFileCount(ctx, "tenant-a"); err != nil {
				t.Fatalf("first IncrementFileCount() error = %v", err)
			}
			if err := manager.IncrementFileCount(ctx, "tenant-a"); !errors.Is(err, core.ErrTenantQuotaExceeded) {
				t.Fatalf("second IncrementFileCount() error = %v, want %v", err, core.ErrTenantQuotaExceeded)
			}
		})
	}
}

// TestTenantQuotaManagerGetEffectiveLimit resolves override vs global vs none.
func TestTenantQuotaManagerGetEffectiveLimit(t *testing.T) {
	ctx := context.Background()
	const tenantID = "tenant-a"

	override := func(v int) *int { return &v }

	tests := []struct {
		name         string
		global       int
		override     *int
		queryUnknown bool
		want         int
	}{
		{name: "no override uses the global limit", global: 7, want: 7},
		{name: "no override and no global limit is unlimited", global: 0, want: 0},
		{name: "override wins over the global limit", global: 7, override: override(3), want: 3},
		{name: "override of zero means unlimited", global: 7, override: override(0), want: 0},
		{name: "unknown tenant falls back to the global limit", global: 9, queryUnknown: true, want: 9},
		{name: "unknown tenant with no global limit is unlimited", global: 0, queryUnknown: true, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewTenantQuotaManager(tt.global)
			if tt.override != nil {
				if err := manager.SetTenantLimit(ctx, tenantID, *tt.override); err != nil {
					t.Fatalf("SetTenantLimit() error = %v", err)
				}
			}

			query := tenantID
			if tt.queryUnknown {
				query = "tenant-unknown"
			}

			got, err := manager.GetEffectiveLimit(ctx, query)
			if err != nil {
				t.Fatalf("GetEffectiveLimit() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("GetEffectiveLimit() = %d, want %d", got, tt.want)
			}

			// A lookup must not materialize quota state for an unknown tenant.
			if tt.queryUnknown {
				if _, loaded := manager.quotas.Load(query); loaded {
					t.Fatal("GetEffectiveLimit() created a quota entry for an unknown tenant")
				}
			}
		})
	}
}

// TestTenantQuotaManagerSetQuotaCreatesOverride guards the legacy SetQuota
// meaning: it installs a per-tenant override, so it must keep winning over the
// global limit.
func TestTenantQuotaManagerSetQuotaCreatesOverride(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager(10)

	if err := manager.SetQuota(ctx, "tenant-a", 2); err != nil {
		t.Fatalf("SetQuota() error = %v", err)
	}

	limit, err := manager.GetEffectiveLimit(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("GetEffectiveLimit() error = %v", err)
	}
	if limit != 2 {
		t.Fatalf("GetEffectiveLimit() = %d, want 2", limit)
	}

	if err := manager.SetGlobalLimit(ctx, 1); err != nil {
		t.Fatalf("SetGlobalLimit() error = %v", err)
	}

	limit, err = manager.GetEffectiveLimit(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("GetEffectiveLimit() error = %v", err)
	}
	if limit != 2 {
		t.Fatalf("GetEffectiveLimit() after SetGlobalLimit = %d, want 2", limit)
	}
}

// TestTenantQuotaManagerGlobalLimitTakesEffectImmediately is the regression
// test for the previous behaviour: a tenant whose entry already existed kept the
// limit captured when the entry was created, so a runtime global-limit change
// was invisible to it.
func TestTenantQuotaManagerGlobalLimitTakesEffectImmediately(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager()
	const tenantID = "tenant-existing"

	for i := 0; i < 3; i++ {
		if err := manager.IncrementFileCount(ctx, tenantID); err != nil {
			t.Fatalf("IncrementFileCount() #%d error = %v", i+1, err)
		}
	}

	// The tenant has a count but no override: the global limit must apply at
	// once.
	if err := manager.SetGlobalLimit(ctx, 3); err != nil {
		t.Fatalf("SetGlobalLimit() error = %v", err)
	}

	limit, err := manager.GetEffectiveLimit(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetEffectiveLimit() error = %v", err)
	}
	if limit != 3 {
		t.Fatalf("GetEffectiveLimit() = %d, want 3", limit)
	}

	canAdd, err := manager.CanAddFile(ctx, tenantID)
	if err != nil {
		t.Fatalf("CanAddFile() error = %v", err)
	}
	if canAdd {
		t.Fatal("CanAddFile() = true at the global limit, want false")
	}
	if err := manager.IncrementFileCount(ctx, tenantID); !errors.Is(err, core.ErrTenantQuotaExceeded) {
		t.Fatalf("IncrementFileCount() error = %v, want %v", err, core.ErrTenantQuotaExceeded)
	}

	// Raising the limit re-opens the tenant without touching its count.
	if err := manager.SetGlobalLimit(ctx, 5); err != nil {
		t.Fatalf("SetGlobalLimit() error = %v", err)
	}
	if canAdd, err = manager.CanAddFile(ctx, tenantID); err != nil {
		t.Fatalf("CanAddFile() error = %v", err)
	}
	if !canAdd {
		t.Fatal("CanAddFile() = false below the raised global limit, want true")
	}
	if err := manager.IncrementFileCount(ctx, tenantID); err != nil {
		t.Fatalf("IncrementFileCount() after raise error = %v", err)
	}

	count, err := manager.GetFileCount(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 4 {
		t.Fatalf("GetFileCount() = %d, want 4", count)
	}

	// Zero means unlimited again.
	if err := manager.SetGlobalLimit(ctx, 0); err != nil {
		t.Fatalf("SetGlobalLimit() error = %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := manager.IncrementFileCount(ctx, tenantID); err != nil {
			t.Fatalf("IncrementFileCount() under unlimited error = %v", err)
		}
	}
}

// TestTenantQuotaManagerRemoveTenantLimit covers override removal: the file
// count survives and the tenant falls back to the current global limit.
func TestTenantQuotaManagerRemoveTenantLimit(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager(10)
	const tenantID = "tenant-a"

	if err := manager.SetTenantLimit(ctx, tenantID, 2); err != nil {
		t.Fatalf("SetTenantLimit() error = %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := manager.IncrementFileCount(ctx, tenantID); err != nil {
			t.Fatalf("IncrementFileCount() #%d error = %v", i+1, err)
		}
	}
	if err := manager.IncrementFileCount(ctx, tenantID); !errors.Is(err, core.ErrTenantQuotaExceeded) {
		t.Fatalf("IncrementFileCount() error = %v, want %v", err, core.ErrTenantQuotaExceeded)
	}

	if err := manager.RemoveTenantLimit(ctx, tenantID); err != nil {
		t.Fatalf("RemoveTenantLimit() error = %v", err)
	}

	limit, err := manager.GetEffectiveLimit(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetEffectiveLimit() error = %v", err)
	}
	if limit != 10 {
		t.Fatalf("GetEffectiveLimit() after removal = %d, want 10", limit)
	}

	count, err := manager.GetFileCount(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("GetFileCount() after removal = %d, want 2 (count must be preserved)", count)
	}

	// The tenant can grow again up to the global limit.
	for i := count; i < 10; i++ {
		if err := manager.IncrementFileCount(ctx, tenantID); err != nil {
			t.Fatalf("IncrementFileCount() #%d error = %v", i+1, err)
		}
	}
	if err := manager.IncrementFileCount(ctx, tenantID); !errors.Is(err, core.ErrTenantQuotaExceeded) {
		t.Fatalf("IncrementFileCount() at global limit error = %v, want %v", err, core.ErrTenantQuotaExceeded)
	}

	// Removing a limit twice, or for a tenant that never had one, is a no-op.
	if err := manager.RemoveTenantLimit(ctx, tenantID); err != nil {
		t.Fatalf("second RemoveTenantLimit() error = %v", err)
	}
	if err := manager.RemoveTenantLimit(ctx, "tenant-unknown"); err != nil {
		t.Fatalf("RemoveTenantLimit() for unknown tenant error = %v", err)
	}
	if _, loaded := manager.quotas.Load("tenant-unknown"); loaded {
		t.Fatal("RemoveTenantLimit() created a quota entry for an unknown tenant")
	}
}

// TestTenantQuotaManagerImplicitEntriesAreNotOverrides proves that entries
// created by IncrementFileCount/SetFileCount follow the global limit instead of
// freezing it, and that removing a limit does not reset their count.
func TestTenantQuotaManagerImplicitEntriesAreNotOverrides(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager(3)

	// SetFileCount is the startup-reconciliation path and must not create an
	// override.
	if err := manager.SetFileCount(ctx, "tenant-reconciled", 5); err != nil {
		t.Fatalf("SetFileCount() error = %v", err)
	}
	if canAdd, err := manager.CanAddFile(ctx, "tenant-reconciled"); err != nil {
		t.Fatalf("CanAddFile() error = %v", err)
	} else if canAdd {
		t.Fatal("CanAddFile() = true above the global limit, want false")
	}

	if err := manager.SetGlobalLimit(ctx, 6); err != nil {
		t.Fatalf("SetGlobalLimit() error = %v", err)
	}
	if canAdd, err := manager.CanAddFile(ctx, "tenant-reconciled"); err != nil {
		t.Fatalf("CanAddFile() error = %v", err)
	} else if !canAdd {
		t.Fatal("CanAddFile() = false below the raised global limit, want true")
	}

	// Removing a limit on an implicit entry keeps the count and the global
	// limit.
	if err := manager.RemoveTenantLimit(ctx, "tenant-reconciled"); err != nil {
		t.Fatalf("RemoveTenantLimit() error = %v", err)
	}
	count, err := manager.GetFileCount(ctx, "tenant-reconciled")
	if err != nil {
		t.Fatalf("GetFileCount() error = %v", err)
	}
	if count != 5 {
		t.Fatalf("GetFileCount() = %d, want 5", count)
	}
	limit, err := manager.GetEffectiveLimit(ctx, "tenant-reconciled")
	if err != nil {
		t.Fatalf("GetEffectiveLimit() error = %v", err)
	}
	if limit != 6 {
		t.Fatalf("GetEffectiveLimit() = %d, want 6", limit)
	}
}

// TestTenantQuotaManagerUnlimitedBehavior covers the 0 = unlimited contract for
// the global limit and for per-tenant overrides.
func TestTenantQuotaManagerUnlimitedBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("global zero is unlimited", func(t *testing.T) {
		manager := NewTenantQuotaManager(0)
		for i := 0; i < 50; i++ {
			if err := manager.IncrementFileCount(ctx, "tenant-a"); err != nil {
				t.Fatalf("IncrementFileCount() #%d error = %v", i+1, err)
			}
		}
		if canAdd, err := manager.CanAddFile(ctx, "tenant-a"); err != nil {
			t.Fatalf("CanAddFile() error = %v", err)
		} else if !canAdd {
			t.Fatal("CanAddFile() = false under unlimited quota, want true")
		}
	})

	t.Run("per-tenant zero overrides a finite global limit", func(t *testing.T) {
		manager := NewTenantQuotaManager(1)
		if err := manager.SetTenantLimit(ctx, "tenant-a", 0); err != nil {
			t.Fatalf("SetTenantLimit() error = %v", err)
		}
		for i := 0; i < 20; i++ {
			if err := manager.IncrementFileCount(ctx, "tenant-a"); err != nil {
				t.Fatalf("IncrementFileCount() #%d error = %v", i+1, err)
			}
		}
		if canAdd, err := manager.CanAddFile(ctx, "tenant-a"); err != nil {
			t.Fatalf("CanAddFile() error = %v", err)
		} else if !canAdd {
			t.Fatal("CanAddFile() = false for an overridden unlimited tenant, want true")
		}
	})

	t.Run("unknown tenant is allowed and stays unmaterialized", func(t *testing.T) {
		manager := NewTenantQuotaManager(2)
		if canAdd, err := manager.CanAddFile(ctx, "tenant-unknown"); err != nil {
			t.Fatalf("CanAddFile() error = %v", err)
		} else if !canAdd {
			t.Fatal("CanAddFile() = false for an unknown tenant, want true")
		}
		if _, loaded := manager.quotas.Load("tenant-unknown"); loaded {
			t.Fatal("CanAddFile() created a quota entry for an unknown tenant")
		}
	})
}

// TestTenantQuotaManagerAdminValidation covers identifier and limit validation
// for the administration capability.
func TestTenantQuotaManagerAdminValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		call func(*TenantQuotaManager) error
	}{
		{
			name: "SetTenantLimit rejects a negative limit",
			call: func(m *TenantQuotaManager) error { return m.SetTenantLimit(ctx, "tenant-a", -1) },
		},
		{
			name: "SetTenantLimit rejects an empty tenant ID",
			call: func(m *TenantQuotaManager) error { return m.SetTenantLimit(ctx, "", 1) },
		},
		{
			name: "RemoveTenantLimit rejects an empty tenant ID",
			call: func(m *TenantQuotaManager) error { return m.RemoveTenantLimit(ctx, "") },
		},
		{
			name: "GetEffectiveLimit rejects an empty tenant ID",
			call: func(m *TenantQuotaManager) error {
				_, err := m.GetEffectiveLimit(ctx, "")
				return err
			},
		},
		{
			name: "SetGlobalLimit rejects a negative limit",
			call: func(m *TenantQuotaManager) error { return m.SetGlobalLimit(ctx, -1) },
		},
		{
			name: "SetQuota rejects a negative limit",
			call: func(m *TenantQuotaManager) error { return m.SetQuota(ctx, "tenant-a", -1) },
		},
		{
			name: "SetQuota rejects an empty tenant ID",
			call: func(m *TenantQuotaManager) error { return m.SetQuota(ctx, "", 1) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewTenantQuotaManager(5)

			if err := tt.call(manager); !errors.Is(err, core.ErrInvalidArgument) {
				t.Fatalf("error = %v, want %v", err, core.ErrInvalidArgument)
			}

			// A rejected call must not mutate any limit.
			global, err := manager.GetGlobalLimit(ctx)
			if err != nil {
				t.Fatalf("GetGlobalLimit() error = %v", err)
			}
			if global != 5 {
				t.Fatalf("GetGlobalLimit() = %d after rejected call, want 5", global)
			}
			limit, err := manager.GetEffectiveLimit(ctx, "tenant-a")
			if err != nil {
				t.Fatalf("GetEffectiveLimit() error = %v", err)
			}
			if limit != 5 {
				t.Fatalf("GetEffectiveLimit() = %d after rejected call, want 5", limit)
			}
		})
	}
}

// TestTenantQuotaManagerConcurrentAdminAndIncrement is the race regression test:
// global-limit mutation, effective-limit reads, override removal and quota
// increments all run against the same manager.
func TestTenantQuotaManagerConcurrentAdminAndIncrement(t *testing.T) {
	ctx := context.Background()
	manager := NewTenantQuotaManager(0)

	// This tenant keeps a fixed override for the whole test, so its count must
	// stop exactly at the override.
	const fixedTenant = "tenant-fixed"
	const fixedLimit = 5
	if err := manager.SetTenantLimit(ctx, fixedTenant, fixedLimit); err != nil {
		t.Fatalf("SetTenantLimit() error = %v", err)
	}

	const (
		incrementers = 8
		iterations   = 200
	)

	var (
		mu       sync.Mutex
		failures []error
	)
	record := func(err error) {
		mu.Lock()
		failures = append(failures, err)
		mu.Unlock()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < incrementers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				tenantID := fmt.Sprintf("tenant-%d", (worker+j)%3)
				if j%3 == 0 {
					tenantID = fixedTenant
				}
				if err := manager.IncrementFileCount(ctx, tenantID); err != nil && !errors.Is(err, core.ErrTenantQuotaExceeded) {
					record(fmt.Errorf("IncrementFileCount(%q) error = %w", tenantID, err))
					return
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for j := 0; j < iterations*2; j++ {
			limit := 0
			if j%2 == 0 {
				limit = 40
			}
			if err := manager.SetGlobalLimit(ctx, limit); err != nil {
				record(fmt.Errorf("SetGlobalLimit(%d) error = %w", limit, err))
				return
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				tenantID := fmt.Sprintf("tenant-%d", (worker+j)%3)
				if _, err := manager.GetEffectiveLimit(ctx, tenantID); err != nil {
					record(fmt.Errorf("GetEffectiveLimit(%q) error = %w", tenantID, err))
					return
				}
				if _, err := manager.CanAddFile(ctx, tenantID); err != nil {
					record(fmt.Errorf("CanAddFile(%q) error = %w", tenantID, err))
					return
				}
				if _, err := manager.GetFileCount(ctx, tenantID); err != nil {
					record(fmt.Errorf("GetFileCount(%q) error = %w", tenantID, err))
					return
				}
			}
		}(i)
	}

	// Override churn on a tenant that is concurrently incremented.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for j := 0; j < iterations; j++ {
			if j%2 == 0 {
				if err := manager.SetTenantLimit(ctx, "tenant-0", 8); err != nil {
					record(fmt.Errorf("SetTenantLimit() error = %w", err))
					return
				}
				continue
			}
			if err := manager.RemoveTenantLimit(ctx, "tenant-0"); err != nil {
				record(fmt.Errorf("RemoveTenantLimit() error = %w", err))
				return
			}
		}
	}()

	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(failures) > 0 {
		t.Fatalf("concurrent operations failed: %v", failures)
	}

	// The fixed-override tenant must have stopped exactly at its override and
	// kept it.
	count, err := manager.GetFileCount(ctx, fixedTenant)
	if err != nil {
		t.Fatalf("GetFileCount(%q) error = %v", fixedTenant, err)
	}
	if count != fixedLimit {
		t.Fatalf("GetFileCount(%q) = %d, want %d", fixedTenant, count, fixedLimit)
	}
	limit, err := manager.GetEffectiveLimit(ctx, fixedTenant)
	if err != nil {
		t.Fatalf("GetEffectiveLimit(%q) error = %v", fixedTenant, err)
	}
	if limit != fixedLimit {
		t.Fatalf("GetEffectiveLimit(%q) = %d, want %d", fixedTenant, limit, fixedLimit)
	}

	// Every count stays non-negative whatever interleaving happened.
	for i := 0; i < 3; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		count, err := manager.GetFileCount(ctx, tenantID)
		if err != nil {
			t.Fatalf("GetFileCount(%q) error = %v", tenantID, err)
		}
		if count < 0 {
			t.Fatalf("GetFileCount(%q) = %d, want >= 0", tenantID, count)
		}
	}
}
