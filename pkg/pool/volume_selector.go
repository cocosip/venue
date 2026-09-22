package pool

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cocosip/venue/pkg/core"
)

// VolumeSelector selects a storage volume for writing files.
type VolumeSelector interface {
	// SelectVolume selects a volume from the available volumes.
	SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error)
}

// VolumeCandidateSelector is an optional VolumeSelector extension that returns
// every writable volume in preference order so the storage pool can retry a
// failed write on the next candidate. requiredBytes <= 0 means "no size
// constraint"; otherwise only volumes reporting at least that much free space
// are returned.
type VolumeCandidateSelector interface {
	// SelectVolumeCandidates returns writable volumes ordered by preference.
	SelectVolumeCandidates(
		ctx context.Context,
		volumes map[string]core.StorageVolume,
		requiredBytes int64,
	) ([]core.StorageVolume, error)
}

// MostAvailableSpaceSelector selects the volume with the most available space.
// Volumes that are unhealthy, report an error, or have no free space left are
// never selected.
type MostAvailableSpaceSelector struct{}

// SelectVolume selects the volume with the most available space.
func (s *MostAvailableSpaceSelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	candidates, err := s.SelectVolumeCandidates(ctx, volumes, 0)
	if err != nil {
		return nil, err
	}
	return candidates[0], nil
}

// SelectVolumeCandidates returns the writable volumes ordered from the most to
// the least available space. Ties are broken by volume ID so the order is
// deterministic.
func (s *MostAvailableSpaceSelector) SelectVolumeCandidates(
	ctx context.Context,
	volumes map[string]core.StorageVolume,
	requiredBytes int64,
) ([]core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	type candidate struct {
		volume core.StorageVolume
		space  int64
	}

	candidates := make([]candidate, 0, len(volumes))
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !volume.IsHealthy(ctx) {
			continue
		}
		space, err := volume.AvailableSpace(ctx)
		if err != nil {
			continue
		}
		// A full volume must never be selected: "0 free bytes" is still full.
		if space <= 0 {
			continue
		}
		if requiredBytes > 0 && space < requiredBytes {
			continue
		}
		candidates = append(candidates, candidate{volume: volume, space: space})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no healthy volumes with available space: %w", core.ErrInsufficientStorage)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].space != candidates[j].space {
			return candidates[i].space > candidates[j].space
		}
		return candidates[i].volume.VolumeID() < candidates[j].volume.VolumeID()
	})

	result := make([]core.StorageVolume, len(candidates))
	for i := range candidates {
		result[i] = candidates[i].volume
	}
	return result, nil
}

// RoundRobinSelector selects volumes in a round-robin fashion. It is safe for
// concurrent use.
type RoundRobinSelector struct {
	mu        sync.Mutex
	lastIndex int
}

// SelectVolume selects the next volume in round-robin order.
//
// Healthy volumes are ordered by volume ID so rotation is deterministic even
// though the input is a map.
func (s *RoundRobinSelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	// Convert map to slice for indexing
	volumeSlice := make([]core.StorageVolume, 0, len(volumes))
	for _, vol := range volumes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if vol.IsHealthy(ctx) {
			volumeSlice = append(volumeSlice, vol)
		}
	}

	if len(volumeSlice) == 0 {
		return nil, fmt.Errorf("no healthy volumes available: %w", core.ErrInsufficientStorage)
	}

	sort.SliceStable(volumeSlice, func(i, j int) bool {
		return volumeSlice[i].VolumeID() < volumeSlice[j].VolumeID()
	})

	s.mu.Lock()
	index := s.lastIndex % len(volumeSlice)
	selected := volumeSlice[index]
	s.lastIndex = (index + 1) % len(volumeSlice)
	s.mu.Unlock()

	return selected, nil
}

// FirstHealthySelector always selects the first healthy volume.
type FirstHealthySelector struct{}

// SelectVolume selects the first healthy volume, ordered by volume ID.
func (s *FirstHealthySelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	healthy := make([]core.StorageVolume, 0, len(volumes))
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if volume.IsHealthy(ctx) {
			healthy = append(healthy, volume)
		}
	}

	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy volumes available: %w", core.ErrInsufficientStorage)
	}

	sort.SliceStable(healthy, func(i, j int) bool {
		return healthy[i].VolumeID() < healthy[j].VolumeID()
	})

	return healthy[0], nil
}
