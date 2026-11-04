package pool

import (
	"context"
	"fmt"
	"sort"

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
//
// The storage pool uses this selector exclusively. A different selection policy
// would need a new option on StoragePool, so no other selector is shipped.
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
