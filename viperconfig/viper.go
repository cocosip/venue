// Package viperconfig adapts Viper sources and configuration files to Config.
// Applications that do not import this package do not compile Viper.
package viperconfig

import (
	"errors"
	"fmt"

	"github.com/cocosip/venue/config"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// ErrSectionNotFound is returned when a named configuration section is absent.
var ErrSectionNotFound = errors.New("configuration section not found")

// Load decodes a Viper source into an independent Venue configuration.
//
// The result is seeded from config.DefaultConfig() and unmarshalled with
// ZeroFields disabled, so a source only has to carry the keys it wants to
// override: the defaults do not need to be registered on the source first.
func Load(source *viper.Viper) (*config.Config, error) {
	if source == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}

	result := config.DefaultConfig()
	if err := source.Unmarshal(result, func(decoder *mapstructure.DecoderConfig) {
		decoder.ZeroFields = false
	}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	// Logging contains a runtime slog.Handler and is never file-bound.
	result.Logging = nil
	result.ApplyDefaults()
	if err := result.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return result, nil
}

// LoadFromFile reads a Viper-supported file, preferring the "venue" section.
func LoadFromFile(filePath string) (*config.Config, error) {
	return LoadFromFileSection(filePath, "venue")
}

// LoadFromFileSection reads a Viper-supported file from sectionName.
//
// An empty sectionName reads the root. A non-empty sectionName that the document
// does not contain is an error wrapping ErrSectionNotFound: silently binding the
// whole document instead would turn an unrelated application config into Venue
// configuration and surface as a confusing validation failure.
func LoadFromFileSection(filePath, sectionName string) (*config.Config, error) {
	source := viper.New()
	source.SetConfigFile(filePath)
	if err := source.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	if sectionName == "" {
		return Load(source)
	}

	section := source.Sub(sectionName)
	if section == nil {
		return nil, fmt.Errorf("section %q not found in %s: %w", sectionName, filePath, ErrSectionNotFound)
	}
	return Load(section)
}

// LoadSection decodes a named section from an application-owned Viper source.
func LoadSection(source *viper.Viper, key string) (*config.Config, error) {
	if source == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}
	section := source.Sub(key)
	if section == nil {
		return nil, fmt.Errorf("section %q not found in configuration: %w", key, ErrSectionNotFound)
	}
	return Load(section)
}
