package pool

import (
	"context"
	"fmt"
	"testing"

	"github.com/cocosip/venue/pkg/core"
)

// BenchmarkMostAvailableSpaceSelector_SelectVolumeCandidates measures the
// per-write selection cost against a realistic volume count.
func BenchmarkMostAvailableSpaceSelector_SelectVolumeCandidates(b *testing.B) {
	ctx := context.Background()

	const volumeCount = 8

	volumes := make(map[string]core.StorageVolume, volumeCount)
	for i := 0; i < volumeCount; i++ {
		id := fmt.Sprintf("volume-%d", i)
		volumes[id] = newFakeVolume(id, true, int64(i+1)*1024)
	}

	selector := &MostAvailableSpaceSelector{}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := selector.SelectVolumeCandidates(ctx, volumes, 0); err != nil {
			b.Fatalf("SelectVolumeCandidates() error = %v", err)
		}
	}
}
