package pool

import (
	"fmt"
	"path/filepath"
	"time"
)

// DateBasedPathGenerator generates paths based on the current date.
// Format: {tenantID}/{YYYY}/{MM}/{DD}/{fileKey}{ext}
// Example: tenant1/2024/01/22/abc123.pdf
type DateBasedPathGenerator struct{}

// GeneratePath generates a date-based storage path.
func (g *DateBasedPathGenerator) GeneratePath(tenantID string, fileKey string, fileExtension string) string {
	now := time.Now()
	year := fmt.Sprintf("%04d", now.Year())
	month := fmt.Sprintf("%02d", int(now.Month()))
	day := fmt.Sprintf("%02d", now.Day())

	// Build path: tenantID/YYYY/MM/DD/fileKey.ext
	fileName := fileKey + fileExtension
	path := filepath.Join(tenantID, year, month, day, fileName)

	return path
}

// FlatPathGenerator generates flat paths without date hierarchy.
// Format: {tenantID}/{fileKey}{ext}
// Example: tenant1/abc123.pdf
type FlatPathGenerator struct{}

// GeneratePath generates a flat storage path.
func (g *FlatPathGenerator) GeneratePath(tenantID string, fileKey string, fileExtension string) string {
	fileName := fileKey + fileExtension
	path := filepath.Join(tenantID, fileName)
	return path
}

// HourBasedPathGenerator generates paths based on date and hour.
// Format: {tenantID}/{YYYY}/{MM}/{DD}/{HH}/{fileKey}{ext}
// Example: tenant1/2024/01/22/14/abc123.pdf
type HourBasedPathGenerator struct{}

// GeneratePath generates an hour-based storage path.
func (g *HourBasedPathGenerator) GeneratePath(tenantID string, fileKey string, fileExtension string) string {
	now := time.Now()
	year := fmt.Sprintf("%04d", now.Year())
	month := fmt.Sprintf("%02d", int(now.Month()))
	day := fmt.Sprintf("%02d", now.Day())
	hour := fmt.Sprintf("%02d", now.Hour())

	// Build path: tenantID/YYYY/MM/DD/HH/fileKey.ext
	fileName := fileKey + fileExtension
	path := filepath.Join(tenantID, year, month, day, hour, fileName)

	return path
}
