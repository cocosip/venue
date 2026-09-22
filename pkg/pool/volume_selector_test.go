package pool

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

func selectorVolumes(volumes ...*fakeVolume) map[string]core.StorageVolume {
	result := make(map[string]core.StorageVolume, len(volumes))
	for _, volume := range volumes {
		result[volume.VolumeID()] = volume
	}
	return result
}

// TestMostAvailableSpaceSelector_SelectsExpectedVolume is the table-driven
// behavior matrix for the default selector.
func TestMostAvailableSpaceSelector_SelectsExpectedVolume(t *testing.T) {
	unavailable := newFakeVolume("unavailable", true, 0)
	unavailable.availableErr = errors.New("capacity probe failed")
	unhealthy := newFakeVolume("unhealthy", false, 1000)
	full := newFakeVolume("full", true, 0)
	roomy := newFakeVolume("roomy", true, 100)
	small := newFakeVolume("small", true, 10)
	large := newFakeVolume("large", true, 100)
	ok := newFakeVolume("ok", true, 50)

	tests := []struct {
		name       string
		volumes    map[string]core.StorageVolume
		wantVolume string
		wantErr    bool
	}{
		{
			name:    "no volumes",
			volumes: map[string]core.StorageVolume{},
			wantErr: true,
		},
		{
			name:    "only unhealthy volumes",
			volumes: selectorVolumes(unhealthy),
			wantErr: true,
		},
		{
			name:    "zero free space never wins",
			volumes: selectorVolumes(full),
			wantErr: true,
		},
		{
			name:       "zero free space volume is skipped",
			volumes:    selectorVolumes(full, roomy),
			wantVolume: "roomy",
		},
		{
			name:       "picks the largest available space",
			volumes:    selectorVolumes(small, large),
			wantVolume: "large",
		},
		{
			name:       "skips volumes whose space probe failed",
			volumes:    selectorVolumes(unavailable, ok),
			wantVolume: "ok",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected, err := (&MostAvailableSpaceSelector{}).SelectVolume(context.Background(), tc.volumes)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got volume %v", selected)
				}
				if !errors.Is(err, core.ErrInsufficientStorage) {
					t.Fatalf("expected ErrInsufficientStorage, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if selected == nil {
				t.Fatal("expected a volume, got nil")
			}
			if selected.VolumeID() != tc.wantVolume {
				t.Errorf("selected volume = %q, want %q", selected.VolumeID(), tc.wantVolume)
			}
		})
	}
}

// TestMostAvailableSpaceSelector_RespectsContextCancellation verifies that the
// selector aborts when its context is already cancelled.
func TestMostAvailableSpaceSelector_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := (&MostAvailableSpaceSelector{}).SelectVolume(ctx, selectorVolumes(newFakeVolume("v1", true, 100)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestMostAvailableSpaceSelector_SizeConstraint verifies that candidates can be
// filtered by the payload size, without ever returning a zero-space volume.
func TestMostAvailableSpaceSelector_SizeConstraint(t *testing.T) {
	ctx := context.Background()
	selector := &MostAvailableSpaceSelector{}
	volumes := selectorVolumes(
		newFakeVolume("tiny", true, 100),
		newFakeVolume("medium", true, 500),
		newFakeVolume("large", true, 2000),
	)

	tests := []struct {
		name          string
		requiredBytes int64
		wantVolumes   []string
		wantErr       bool
	}{
		{
			name:          "no constraint keeps every writable volume",
			requiredBytes: 0,
			wantVolumes:   []string{"large", "medium", "tiny"},
		},
		{
			name:          "filters volumes that cannot hold the payload",
			requiredBytes: 500,
			wantVolumes:   []string{"large", "medium"},
		},
		{
			name:          "exact fit is accepted",
			requiredBytes: 2000,
			wantVolumes:   []string{"large"},
		},
		{
			name:          "no volume fits",
			requiredBytes: 2001,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidates, err := selector.SelectVolumeCandidates(ctx, volumes, tc.requiredBytes)
			if tc.wantErr {
				if !errors.Is(err, core.ErrInsufficientStorage) {
					t.Fatalf("expected ErrInsufficientStorage, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			got := make([]string, 0, len(candidates))
			for _, candidate := range candidates {
				got = append(got, candidate.VolumeID())
			}
			if len(got) != len(tc.wantVolumes) {
				t.Fatalf("candidates = %v, want %v", got, tc.wantVolumes)
			}
			for i := range got {
				if got[i] != tc.wantVolumes[i] {
					t.Fatalf("candidates = %v, want %v", got, tc.wantVolumes)
				}
			}
		})
	}
}

// TestRoundRobinSelector_Errors verifies the empty and unhealthy error cases.
func TestRoundRobinSelector_Errors(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		volumes map[string]core.StorageVolume
	}{
		{name: "no volumes", volumes: map[string]core.StorageVolume{}},
		{name: "only unhealthy volumes", volumes: selectorVolumes(newFakeVolume("v1", false, 100))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := (&RoundRobinSelector{}).SelectVolume(ctx, tc.volumes)
			if !errors.Is(err, core.ErrInsufficientStorage) {
				t.Fatalf("expected ErrInsufficientStorage, got %v", err)
			}
		})
	}
}

// TestRoundRobinSelector_RotatesThroughHealthyVolumes verifies that consecutive
// selections rotate over the healthy volumes in a stable order.
func TestRoundRobinSelector_RotatesThroughHealthyVolumes(t *testing.T) {
	ctx := context.Background()
	unhealthy := newFakeVolume("v9", false, 100)

	selector := &RoundRobinSelector{}
	volumes := selectorVolumes(
		newFakeVolume("v3", true, 100),
		newFakeVolume("v1", true, 100),
		newFakeVolume("v2", true, 100),
		unhealthy,
	)

	counts := map[string]int{}
	const picks = 6
	for i := 0; i < picks; i++ {
		selected, err := selector.SelectVolume(ctx, volumes)
		if err != nil {
			t.Fatalf("selection %d failed: %v", i, err)
		}
		counts[selected.VolumeID()]++
	}

	for _, id := range []string{"v1", "v2", "v3"} {
		if counts[id] != picks/3 {
			t.Errorf("volume %s selected %d times, want %d (selections=%v)", id, counts[id], picks/3, counts)
		}
	}
	if counts["v9"] != 0 {
		t.Errorf("unhealthy volume selected %d times, want 0", counts["v9"])
	}
}

// TestRoundRobinSelector_ConcurrentSelection verifies that concurrent callers
// can share one selector without a data race.
func TestRoundRobinSelector_ConcurrentSelection(t *testing.T) {
	ctx := context.Background()
	volumes := selectorVolumes(
		newFakeVolume("v1", true, 100),
		newFakeVolume("v2", true, 100),
		newFakeVolume("v3", true, 100),
	)

	selector := &RoundRobinSelector{}
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				selected, err := selector.SelectVolume(ctx, volumes)
				if err != nil {
					t.Errorf("concurrent selection failed: %v", err)
					return
				}
				if selected == nil {
					t.Error("concurrent selection returned a nil volume")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestFirstHealthySelector_SelectsDeterministicVolume verifies that the "first"
// healthy volume is stable across calls.
func TestFirstHealthySelector_SelectsDeterministicVolume(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		volumes    map[string]core.StorageVolume
		wantVolume string
		wantErr    bool
	}{
		{
			name:    "no volumes",
			volumes: map[string]core.StorageVolume{},
			wantErr: true,
		},
		{
			name:    "only unhealthy volumes",
			volumes: selectorVolumes(newFakeVolume("v2", false, 100)),
			wantErr: true,
		},
		{
			name: "skips unhealthy volumes",
			volumes: selectorVolumes(
				newFakeVolume("v3", true, 100),
				newFakeVolume("v1", false, 100),
				newFakeVolume("v2", true, 100),
			),
			wantVolume: "v2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 5; i++ {
				selected, err := (&FirstHealthySelector{}).SelectVolume(ctx, tc.volumes)
				if tc.wantErr {
					if err == nil {
						t.Fatalf("expected an error, got volume %v", selected)
					}
					continue
				}
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if selected.VolumeID() != tc.wantVolume {
					t.Fatalf("selected volume = %q, want %q", selected.VolumeID(), tc.wantVolume)
				}
			}
		})
	}
}

// TestPathGenerators_GenerateExpectedLayouts covers the three shipped path
// generators, which previously had no tests.
func TestPathGenerators_GenerateExpectedLayouts(t *testing.T) {
	tests := []struct {
		name    string
		gen     PathGenerator
		pattern *regexp.Regexp
	}{
		{
			name:    "flat",
			gen:     &FlatPathGenerator{},
			pattern: regexp.MustCompile(`^tenant-1/abc123\.pdf$`),
		},
		{
			name:    "date based",
			gen:     &DateBasedPathGenerator{},
			pattern: regexp.MustCompile(`^tenant-1/\d{4}/\d{2}/\d{2}/abc123\.pdf$`),
		},
		{
			name:    "hour based",
			gen:     &HourBasedPathGenerator{},
			pattern: regexp.MustCompile(`^tenant-1/\d{4}/\d{2}/\d{2}/\d{2}/abc123\.pdf$`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			generated := filepath.ToSlash(tc.gen.GeneratePath("tenant-1", "abc123", ".pdf"))
			if !tc.pattern.MatchString(generated) {
				t.Errorf("generated path %q does not match %v", generated, tc.pattern)
			}
		})
	}
}

// TestPathGenerators_EmptyExtension verifies extension-less file keys.
func TestPathGenerators_EmptyExtension(t *testing.T) {
	generators := map[string]PathGenerator{
		"flat":       &FlatPathGenerator{},
		"date based": &DateBasedPathGenerator{},
		"hour based": &HourBasedPathGenerator{},
	}

	for name, generator := range generators {
		t.Run(name, func(t *testing.T) {
			generated := filepath.ToSlash(generator.GeneratePath("tenant-1", "abc123", ""))
			base := filepath.Base(generated)
			if base != "abc123" {
				t.Errorf("base name = %q, want %q (full path %q)", base, "abc123", generated)
			}
			if filepath.ToSlash(filepath.Dir(generated)) == "." {
				t.Errorf("expected a tenant-scoped path, got %q", generated)
			}
		})
	}
}
