package core

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxTenantIDLength bounds a tenant identifier so that it always stays a valid
// single path segment on every supported platform.
const MaxTenantIDLength = 128

// reservedTenantDeviceNames are Windows device names that cannot be used as a
// file or directory name. They are rejected case-insensitively, with or without
// a file extension ("CON", "con.json", "LPT1").
var reservedTenantDeviceNames = map[string]bool{
	"CON": true, "PRN": true, "AUX": true, "NUL": true,
	"COM0": true, "COM1": true, "COM2": true, "COM3": true, "COM4": true,
	"COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true,
	"LPT0": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true,
	"LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true,
}

// ValidateTenantID reports whether tenantID is safe to use as a tenant
// identifier and as a single filesystem path segment.
//
// Tenant identifiers reach the runtime from callers and configuration, and are
// used to build tenant metadata paths and physical storage directories. An
// unvalidated identifier such as "../../evil" would resolve outside the
// configured root, so every path derivation must reject it up front.
//
// The returned error always wraps ErrInvalidArgument. Identifiers containing a
// path separator or a "."/".." segment additionally wrap ErrPathTraversalAttempt.
//
// Rejected inputs: empty, invalid UTF-8, longer than MaxTenantIDLength bytes,
// control characters, the characters / \ : * ? " < > |, leading or trailing
// whitespace, a trailing dot, a leading dot (the runtime reserves "."-prefixed
// names such as ".locus" for internal state), and Windows device names.
func ValidateTenantID(tenantID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty: %w", ErrInvalidArgument)
	}
	if !utf8.ValidString(tenantID) {
		return fmt.Errorf("tenant ID is not valid UTF-8: %w", ErrInvalidArgument)
	}
	if len(tenantID) > MaxTenantIDLength {
		return fmt.Errorf("tenant ID exceeds %d bytes: %w", MaxTenantIDLength, ErrInvalidArgument)
	}

	traversal := tenantID == "." || tenantID == ".."

	for _, r := range tenantID {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("tenant ID contains a control character: %w", ErrInvalidArgument)
		}
		switch r {
		case '/', '\\':
			traversal = true
			return tenantIDError(tenantID, "contains a path separator", traversal)
		case ':', '*', '?', '"', '<', '>', '|':
			return tenantIDError(tenantID, "contains a reserved path character", traversal)
		}
	}

	if strings.TrimSpace(tenantID) != tenantID {
		return fmt.Errorf("tenant ID has leading or trailing whitespace: %w", ErrInvalidArgument)
	}
	if strings.HasPrefix(tenantID, ".") {
		return tenantIDError(tenantID, "starts with a reserved dot prefix", traversal)
	}
	if strings.HasSuffix(tenantID, ".") {
		return tenantIDError(tenantID, "ends with a dot", traversal)
	}
	if reservedTenantDeviceNames[reservedDeviceName(tenantID)] {
		return fmt.Errorf("tenant ID is a reserved device name: %w", ErrInvalidArgument)
	}

	return nil
}

// tenantIDError builds a validation error, adding ErrPathTraversalAttempt for
// inputs that could resolve outside the configured root.
func tenantIDError(tenantID, reason string, traversal bool) error {
	base := ErrInvalidArgument
	if traversal {
		base = errors.Join(ErrInvalidArgument, ErrPathTraversalAttempt)
	}
	return fmt.Errorf("tenant ID %q %s: %w", tenantID, reason, base)
}

// reservedDeviceName returns the device-name key for tenantID: the part before
// the first dot, upper-cased. Windows resolves "CON.json" to the CON device.
func reservedDeviceName(tenantID string) string {
	if index := strings.IndexByte(tenantID, '.'); index >= 0 {
		tenantID = tenantID[:index]
	}
	return strings.ToUpper(tenantID)
}
