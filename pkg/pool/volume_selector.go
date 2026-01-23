package pool

import (
	"context"
	"fmt"

	"github.com/cocosip/venue/pkg/core"
)

// VolumeSelector selects a storage volume for writing files.
type VolumeSelector interface {
	// SelectVolume selects a volume from the available volumes.
	SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error)
}

// MostAvailableSpaceSelector selects the volume with the most available space.
type MostAvailableSpaceSelector struct{}

// SelectVolume selects the volume with the most available space.
func (s *MostAvailableSpaceSelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	var selectedVolume core.StorageVolume
	var maxAvailableSpace int64 = -1

	for _, volume := range volumes {
		// Check if volume is healthy
		if !volume.IsHealthy(ctx) {
			continue
		}

		// Get available space
		space, err := volume.AvailableSpace(ctx)
		if err != nil {
			continue
		}

		// Select volume with most space
		if space > maxAvailableSpace {
			maxAvailableSpace = space
			selectedVolume = volume
		}
	}

	if selectedVolume == nil {
		return nil, fmt.Errorf("no healthy volumes available: %w", core.ErrInsufficientStorage)
	}

	return selectedVolume, nil
}

// RoundRobinSelector selects volumes in a round-robin fashion.
type RoundRobinSelector struct {
	lastIndex int
}

// SelectVolume selects the next volume in round-robin order.
func (s *RoundRobinSelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	// Convert map to slice for indexing
	volumeSlice := make([]core.StorageVolume, 0, len(volumes))
	for _, vol := range volumes {
		if vol.IsHealthy(ctx) {
			volumeSlice = append(volumeSlice, vol)
		}
	}

	if len(volumeSlice) == 0 {
		return nil, fmt.Errorf("no healthy volumes available: %w", core.ErrInsufficientStorage)
	}

	// Select next volume
	s.lastIndex = (s.lastIndex + 1) % len(volumeSlice)
	return volumeSlice[s.lastIndex], nil
}

// FirstHealthySelector always selects the first healthy volume.
type FirstHealthySelector struct{}

// SelectVolume selects the first healthy volume.
func (s *FirstHealthySelector) SelectVolume(ctx context.Context, volumes map[string]core.StorageVolume) (core.StorageVolume, error) {
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available: %w", core.ErrInsufficientStorage)
	}

	for _, volume := range volumes {
		if volume.IsHealthy(ctx) {
			return volume, nil
		}
	}

	return nil, fmt.Errorf("no healthy volumes available: %w", core.ErrInsufficientStorage)
}
