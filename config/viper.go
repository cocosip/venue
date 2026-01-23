package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// LoadWithViper loads Venue configuration using viper.
// This allows binding to environment variables, remote config, etc.
func LoadWithViper(v *viper.Viper) (*Config, error) {
	if v == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Apply defaults for missing values
	config.ApplyDefaults()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// LoadFromFileWithViper loads configuration from a file using viper.
// It attempts to load from the "venue" node first, and falls back to root level if not found.
// Supports JSON, YAML, TOML, HCL, envfile and Java properties config files.
func LoadFromFileWithViper(filePath string) (*Config, error) {
	return LoadFromFileWithViperNode(filePath, "venue")
}

// LoadFromFileWithViperNode loads configuration from a file using viper with a specific root node.
// If nodeName is empty, loads from root level directly.
// If nodeName is specified but not found, falls back to root level.
// Supports JSON, YAML, TOML, HCL, envfile and Java properties config files.
func LoadFromFileWithViperNode(filePath string, nodeName string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(filePath)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// If nodeName is specified, try to load from that node first
	if nodeName != "" {
		subV := v.Sub(nodeName)
		if subV != nil {
			// Found the node, use it
			return LoadWithViper(subV)
		}
		// Node not found, fall back to root level
	}

	// Load from root level
	return LoadWithViper(v)
}

// LoadVenueSectionWithViper loads Venue configuration from a section in viper.
// This is useful when Venue config is embedded in a larger application config.
// Example: If your config has a "venue" section, use "venue" as the key.
func LoadVenueSectionWithViper(v *viper.Viper, key string) (*Config, error) {
	if v == nil {
		return nil, fmt.Errorf("viper instance cannot be nil")
	}

	// Create a sub-viper for the venue section
	venueViper := v.Sub(key)
	if venueViper == nil {
		return nil, fmt.Errorf("section '%s' not found in configuration", key)
	}

	return LoadWithViper(venueViper)
}

// BindToViper binds a Config instance to viper for runtime updates.
// This enables hot-reload of configuration when viper watches for changes.
func BindToViper(v *viper.Viper, config *Config) error {
	// Set up automatic environment variable binding
	v.SetEnvPrefix("VENUE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	return nil
}

// NewViperWithDefaults creates a new viper instance with Venue defaults.
// This is useful when you want to create a configuration from scratch.
func NewViperWithDefaults() *viper.Viper {
	v := viper.New()

	// Set defaults from DefaultConfig
	defaults := DefaultConfig()

	// Top-level settings
	v.SetDefault("metadataDirectory", defaults.MetadataDirectory)
	v.SetDefault("quotaDirectory", defaults.QuotaDirectory)
	v.SetDefault("fileWatcherConfigurationDirectory", defaults.FileWatcherConfigurationDirectory)
	v.SetDefault("autoCreateTenants", defaults.AutoCreateTenants)
	v.SetDefault("defaultTenantQuota", defaults.DefaultTenantQuota)
	v.SetDefault("enableDatabaseHealthCheck", defaults.EnableDatabaseHealthCheck)
	v.SetDefault("enableBackgroundCleanup", defaults.EnableBackgroundCleanup)

	// RetryPolicy
	v.SetDefault("retryPolicy.maxRetryCount", defaults.RetryPolicy.MaxRetryCount)
	v.SetDefault("retryPolicy.initialRetryDelay", defaults.RetryPolicy.InitialRetryDelay)
	v.SetDefault("retryPolicy.useExponentialBackoff", defaults.RetryPolicy.UseExponentialBackoff)
	v.SetDefault("retryPolicy.maxRetryDelay", defaults.RetryPolicy.MaxRetryDelay)

	// CleanupOptions
	v.SetDefault("cleanupOptions.cleanupInterval", defaults.CleanupOptions.CleanupInterval)
	v.SetDefault("cleanupOptions.initialDelay", defaults.CleanupOptions.InitialDelay)
	v.SetDefault("cleanupOptions.cleanupEmptyDirectories", defaults.CleanupOptions.CleanupEmptyDirectories)
	v.SetDefault("cleanupOptions.cleanupTimedOutFiles", defaults.CleanupOptions.CleanupTimedOutFiles)
	v.SetDefault("cleanupOptions.processingTimeout", defaults.CleanupOptions.ProcessingTimeout)
	v.SetDefault("cleanupOptions.cleanupPermanentlyFailedFiles", defaults.CleanupOptions.CleanupPermanentlyFailedFiles)
	v.SetDefault("cleanupOptions.failedFileRetentionPeriod", defaults.CleanupOptions.FailedFileRetentionPeriod)
	v.SetDefault("cleanupOptions.cleanupCompletedRecords", defaults.CleanupOptions.CleanupCompletedRecords)
	v.SetDefault("cleanupOptions.completedRecordRetentionPeriod", defaults.CleanupOptions.CompletedRecordRetentionPeriod)
	v.SetDefault("cleanupOptions.optimizeDatabases", defaults.CleanupOptions.OptimizeDatabases)
	v.SetDefault("cleanupOptions.databaseOptimizationInterval", defaults.CleanupOptions.DatabaseOptimizationInterval)

	// DatabaseHealthCheckOptions
	v.SetDefault("databaseHealthCheckOptions.initialDelay", defaults.DatabaseHealthCheckOptions.InitialDelay)
	v.SetDefault("databaseHealthCheckOptions.maxRetries", defaults.DatabaseHealthCheckOptions.MaxRetries)
	v.SetDefault("databaseHealthCheckOptions.retryDelay", defaults.DatabaseHealthCheckOptions.RetryDelay)
	v.SetDefault("databaseHealthCheckOptions.checkOnStartupOnly", defaults.DatabaseHealthCheckOptions.CheckOnStartupOnly)
	v.SetDefault("databaseHealthCheckOptions.periodicCheckInterval", defaults.DatabaseHealthCheckOptions.PeriodicCheckInterval)

	return v
}
