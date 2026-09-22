package venue

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/cocosip/venue/config"
	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// scriptedVolume is a StorageVolume with scripted health answers. It also
// implements the forced-probe and warm-up capabilities, so a test can prove
// which of the two paths the runtime takes.
type scriptedVolume struct {
	volumeID string

	// probeAnswers is consumed one answer per ProbeHealth call; the last answer
	// repeats once the script is exhausted.
	probeAnswers []bool
	probes       int

	warmups   int
	warmupErr error
}

func (v *scriptedVolume) VolumeID() string { return v.volumeID }

func (v *scriptedVolume) MountPath() string { return "" }

// IsHealthy must never be consulted while the forced-probe capability exists.
func (v *scriptedVolume) IsHealthy(context.Context) bool { return false }

func (v *scriptedVolume) TotalCapacity(context.Context) (int64, error) { return 0, nil }

func (v *scriptedVolume) AvailableSpace(context.Context) (int64, error) { return 0, nil }

func (v *scriptedVolume) WriteFile(context.Context, string, io.Reader) (int64, error) {
	return 0, nil
}

func (v *scriptedVolume) ReadFile(context.Context, string) (io.ReadCloser, error) {
	return nil, core.ErrFileNotFound
}

func (v *scriptedVolume) DeleteFile(context.Context, string) error { return nil }

func (v *scriptedVolume) FileExists(context.Context, string) (bool, error) { return false, nil }

func (v *scriptedVolume) ProbeHealth(context.Context) bool {
	index := v.probes
	v.probes++

	if len(v.probeAnswers) == 0 {
		return true
	}
	if index >= len(v.probeAnswers) {
		index = len(v.probeAnswers) - 1
	}
	return v.probeAnswers[index]
}

func (v *scriptedVolume) WarmWritePathCache(context.Context) error {
	v.warmups++
	return v.warmupErr
}

func newStartupTestVenue(volume *scriptedVolume, mutate func(*config.VolumeConfig)) *Venue {
	volumeConfig := config.NewVolumeConfig().
		WithVolumeID(volume.volumeID).
		WithMountPath("unused").
		WithStartupHealthChecks(0, 0)
	if mutate != nil {
		mutate(volumeConfig)
	}

	return &Venue{
		config:  &config.Config{Volumes: []config.VolumeConfig{*volumeConfig}},
		logger:  logging.Disabled(),
		volumes: map[string]core.StorageVolume{volume.volumeID: volume},
	}
}

// TestPrepareVolumesFailsWhenAVolumeNeverBecomesHealthy covers the startup
// contract: a volume that never answers the probe fails construction with a
// stable error instead of letting the pool pick an unusable volume.
func TestPrepareVolumesFailsWhenAVolumeNeverBecomesHealthy(t *testing.T) {
	volume := &scriptedVolume{volumeID: "vh", probeAnswers: []bool{false}}
	runtime := newStartupTestVenue(volume, nil)

	err := runtime.prepareVolumes(context.Background())
	if !errors.Is(err, core.ErrStorageVolumeUnavailable) {
		t.Fatalf("prepareVolumes() error = %v, want core.ErrStorageVolumeUnavailable", err)
	}
	if volume.probes != 1+maxVolumeHealthCheckAttempts {
		t.Errorf("probe attempts = %d, want one immediate probe plus %d retries",
			volume.probes, maxVolumeHealthCheckAttempts)
	}
}

// TestPrepareVolumesDoesNotDelayAHealthyVolume guards the deliberate divergence
// from Locus: the configured initial delay applies before a retry, never before
// a volume that answers immediately.
func TestPrepareVolumesDoesNotDelayAHealthyVolume(t *testing.T) {
	volume := &scriptedVolume{volumeID: "vh", probeAnswers: []bool{true}}
	runtime := newStartupTestVenue(volume, func(volumeConfig *config.VolumeConfig) {
		volumeConfig.WithStartupHealthChecks(30*time.Second, 30*time.Second)
	})

	started := time.Now()
	if err := runtime.prepareVolumes(context.Background()); err != nil {
		t.Fatalf("prepareVolumes() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("prepareVolumes() took %s for a healthy volume, want no startup delay", elapsed)
	}
	if volume.probes != 1 {
		t.Errorf("probe attempts = %d, want exactly one immediate probe", volume.probes)
	}
}

// TestPrepareVolumesRequiresTwoConsecutiveHealthyProbes covers the retry rule: a
// flapping volume is retried until it answers twice in a row.
func TestPrepareVolumesRequiresTwoConsecutiveHealthyProbes(t *testing.T) {
	volume := &scriptedVolume{
		volumeID:     "vh",
		probeAnswers: []bool{false, true, false, true, true},
	}
	runtime := newStartupTestVenue(volume, nil)

	if err := runtime.prepareVolumes(context.Background()); err != nil {
		t.Fatalf("prepareVolumes() error = %v", err)
	}
	if volume.probes != 5 {
		t.Errorf("probe attempts = %d, want 5 (the script that reaches two consecutive successes)", volume.probes)
	}
}

// TestPrepareVolumesWarmsUpOnceAndContainsFailures covers the warm-up wiring: the
// optional write happens once for a warm-up capable volume, and a failure is
// reported rather than turning into a startup error.
func TestPrepareVolumesWarmsUpOnceAndContainsFailures(t *testing.T) {
	volume := &scriptedVolume{
		volumeID:     "vh",
		probeAnswers: []bool{true},
		warmupErr:    errors.New("warmup failed"),
	}
	runtime := newStartupTestVenue(volume, func(volumeConfig *config.VolumeConfig) {
		volumeConfig.WarmupOnStartup = true
	})

	if err := runtime.prepareVolumes(context.Background()); err != nil {
		t.Fatalf("prepareVolumes() error = %v, want the warm-up failure to stay advisory", err)
	}
	if volume.warmups != 1 {
		t.Errorf("warm-up calls = %d, want 1", volume.warmups)
	}
}

// TestPrepareVolumesHonorsCancellation covers the reserved shutdown path: a
// cancelled startup stops waiting and reports the context error.
func TestPrepareVolumesHonorsCancellation(t *testing.T) {
	volume := &scriptedVolume{volumeID: "vh", probeAnswers: []bool{false}}
	runtime := newStartupTestVenue(volume, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runtime.prepareVolumes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("prepareVolumes(cancelled) error = %v, want context.Canceled", err)
	}
}
