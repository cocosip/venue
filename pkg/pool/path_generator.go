package pool

import (
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// stringBuilderPool reduces GC pressure by reusing string builders for path generation
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &stringBuilder{}
	},
}

type stringBuilder struct {
	buf [256]byte
	len int
}

func (sb *stringBuilder) reset() {
	sb.len = 0
}

func (sb *stringBuilder) appendString(s string) {
	n := copy(sb.buf[sb.len:], s)
	sb.len += n
}

func (sb *stringBuilder) appendInt(i int, width int) {
	n := strconv.Itoa(i)
	if width > len(n) {
		for i := 0; i < width-len(n); i++ {
			sb.buf[sb.len] = '0'
			sb.len++
		}
	}
	sb.appendString(n)
}

func (sb *stringBuilder) string() string {
	return string(sb.buf[:sb.len])
}

// DateBasedPathGenerator generates paths based on the current date.
// Format: {tenantID}/{YYYY}/{MM}/{DD}/{fileKey}{ext}
// Example: tenant1/2024/01/22/abc123.pdf
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
	year := itoa4(now.Year())
	month := itoa2(int(now.Month()))
	day := itoa2(now.Day())
	hour := itoa2(now.Hour())

	// Build path: tenantID/YYYY/MM/DD/HH/fileKey.ext
	fileName := fileKey + fileExtension
	path := filepath.Join(tenantID, year, month, day, hour, fileName)

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
