package pool

import (
	"path/filepath"
	"strconv"
	"time"
)

// DateBasedPathGenerator generates paths based on the current date.
// Format: {tenantID}/{YYYY}/{MM}/{DD}/{fileKey}{ext}
// Example: tenant1/2024/01/22/abc123.pdf
//
// It is only a fallback for a volume that cannot build a path itself (see
// StoragePoolOptions.PathGenerator); a local volume's own builder is preferred.
//
// Caveat: the generator is not deterministic. It reads the wall clock on every
// call, so the same (tenantID, fileKey, extension) produces a different path
// after a date boundary, which violates the PathGenerator contract. That
// matters because the pool rebuilds a canonical path to correct a drifted
// physical path (tryCorrectPhysicalPath): for a file written just before
// midnight the rebuilt path points into the next day's directory, the correction
// finds no bytes there, and the read reports the original failure. A deployment
// that relies on physical-path correction must configure a deterministic
// PathGenerator (or a volume that implements core.StorageVolumePathBuilder).
type DateBasedPathGenerator struct{}

// GeneratePath generates a date-based storage path.
func (g *DateBasedPathGenerator) GeneratePath(tenantID string, fileKey string, fileExtension string) string {
	now := time.Now()
	year := itoa4(now.Year())
	month := itoa2(int(now.Month()))
	day := itoa2(now.Day())

	// Build path: tenantID/YYYY/MM/DD/fileKey.ext
	fileName := fileKey + fileExtension
	path := filepath.Join(tenantID, year, month, day, fileName)

	return path
}

// itoa2 converts an integer to a 2-digit string (with leading zero if needed)
func itoa2(i int) string {
	if i < 10 {
		return "0" + strconv.Itoa(i)
	}
	return strconv.Itoa(i)
}

// itoa4 converts an integer to a 4-digit string (with leading zeros if needed)
func itoa4(i int) string {
	switch {
	case i < 10:
		return "000" + strconv.Itoa(i)
	case i < 100:
		return "00" + strconv.Itoa(i)
	case i < 1000:
		return "0" + strconv.Itoa(i)
	default:
		return strconv.Itoa(i)
	}
}
