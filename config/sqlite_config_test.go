package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/cocosip/venue/config"
)

// TestSqliteDefaults pins the documented SQLite engine defaults so an engine
// swap cannot silently change durability or cache behaviour.
func TestSqliteDefaults(t *testing.T) {
	sqlite := config.DefaultConfig().Sqlite

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"JournalMode", sqlite.JournalMode, "WAL"},
		{"SynchronousMode", sqlite.SynchronousMode, "NORMAL"},
		{"CacheSizeKb", sqlite.CacheSizeKb, -4000},
		{"BusyTimeoutMs", sqlite.BusyTimeoutMs, 5000},
		{"CheckpointAfterBatch", sqlite.CheckpointAfterBatch, false},
		{"MaxOpenConns", sqlite.MaxOpenConns, 1},
		{"MaxOpenDatabases", sqlite.MaxOpenDatabases, 0},
		{"OpenDatabaseIdleTimeout", sqlite.OpenDatabaseIdleTimeout, time.Duration(0)},
		{"RecoverCorruptedDatabase", sqlite.RecoverCorruptedDatabase, false},
		{"CorruptedDatabaseRetention", sqlite.CorruptedDatabaseRetention, 72 * time.Hour},
		{"BackupDirectory", sqlite.BackupDirectory, ""},
		{"BackupInterval", sqlite.BackupInterval, time.Hour},
		{"BackupRetention", sqlite.BackupRetention, 7 * 24 * time.Hour},
		{"AutoRestoreFromBackup", sqlite.AutoRestoreFromBackup, false},
		{"SkipBackupVerification", sqlite.SkipBackupVerification, false},
		{"OptimizeIdleTenantDatabases", sqlite.OptimizeIdleTenantDatabases, false},
	}

	for _, test := range tests {
		if test.got != test.want {
			t.Errorf("Sqlite.%s = %v, want %v", test.name, test.got, test.want)
		}
	}
}

// TestValidateSqlitePragmaWhitelist covers the Locus pragma value whitelist: a
// value outside it must fail construction instead of reaching a PRAGMA statement.
func TestValidateSqlitePragmaWhitelist(t *testing.T) {
	tests := []struct {
		name        string
		journalMode string
		synchronous string
		wantError   bool
		wantText    string
	}{
		{name: "defaults", journalMode: "WAL", synchronous: "NORMAL"},
		{name: "lowercase is accepted", journalMode: "wal", synchronous: "normal"},
		{name: "memory journal", journalMode: "MEMORY", synchronous: "FULL"},
		{name: "numeric synchronous", journalMode: "DELETE", synchronous: "1"},
		{name: "extra synchronous", journalMode: "PERSIST", synchronous: "EXTRA"},
		{name: "unknown journal mode", journalMode: "SIDEWAYS", synchronous: "NORMAL", wantError: true, wantText: "JournalMode"},
		{name: "injection attempt", journalMode: "WAL; DROP TABLE files", synchronous: "NORMAL", wantError: true, wantText: "JournalMode"},
		{name: "unknown synchronous mode", journalMode: "WAL", synchronous: "EVENTUAL", wantError: true, wantText: "SynchronousMode"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Sqlite.JournalMode = test.journalMode
			cfg.Sqlite.SynchronousMode = test.synchronous

			err := cfg.Validate()
			if !test.wantError {
				if err != nil {
					t.Fatalf("Validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() error = nil, want an error mentioning %s", test.wantText)
			}
			if !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("Validate() error = %v, want it to mention %s", err, test.wantText)
			}
		})
	}
}

// TestValidateSqliteNumericBounds covers the remaining scalar rules.
func TestValidateSqliteNumericBounds(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*config.SqliteConfig)
		wantError bool
	}{
		{name: "zero cache size is rejected", mutate: func(s *config.SqliteConfig) { s.CacheSizeKb = 0 }, wantError: true},
		{name: "negative busy timeout", mutate: func(s *config.SqliteConfig) { s.BusyTimeoutMs = -1 }, wantError: true},
		{name: "zero max open conns", mutate: func(s *config.SqliteConfig) { s.MaxOpenConns = 0 }, wantError: true},
		{name: "negative max open databases", mutate: func(s *config.SqliteConfig) { s.MaxOpenDatabases = -1 }, wantError: true},
		{name: "negative idle timeout", mutate: func(s *config.SqliteConfig) { s.OpenDatabaseIdleTimeout = -time.Second }, wantError: true},
		{name: "negative corrupted retention", mutate: func(s *config.SqliteConfig) { s.CorruptedDatabaseRetention = -time.Second }, wantError: true},
		{name: "negative backup interval", mutate: func(s *config.SqliteConfig) { s.BackupInterval = -time.Second }, wantError: true},
		{name: "negative backup retention", mutate: func(s *config.SqliteConfig) { s.BackupRetention = -time.Second }, wantError: true},
		{
			name:      "auto restore requires a backup directory",
			mutate:    func(s *config.SqliteConfig) { s.AutoRestoreFromBackup = true; s.BackupDirectory = "" },
			wantError: true,
		},
		{
			name: "auto restore with a backup directory",
			mutate: func(s *config.SqliteConfig) {
				s.AutoRestoreFromBackup = true
				s.BackupDirectory = "backups"
				s.RecoverCorruptedDatabase = true
			},
		},
		{
			name: "the default empty backup directory is valid",
			// An empty directory disables periodic backups; the default interval
			// must therefore not make DefaultConfig invalid.
			mutate: func(s *config.SqliteConfig) {},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			test.mutate(&cfg.Sqlite)

			err := cfg.Validate()
			if test.wantError && err == nil {
				t.Fatal("Validate() error = nil, want an error")
			}
			if !test.wantError && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

// TestApplyDefaultsRestoresSqliteDefaults covers the zero-value fallbacks.
func TestApplyDefaultsRestoresSqliteDefaults(t *testing.T) {
	cfg := &config.Config{}
	cfg.ApplyDefaults()

	defaults := config.DefaultConfig().Sqlite
	switch {
	case cfg.Sqlite.JournalMode != defaults.JournalMode:
		t.Errorf("JournalMode = %q, want %q", cfg.Sqlite.JournalMode, defaults.JournalMode)
	case cfg.Sqlite.SynchronousMode != defaults.SynchronousMode:
		t.Errorf("SynchronousMode = %q, want %q", cfg.Sqlite.SynchronousMode, defaults.SynchronousMode)
	case cfg.Sqlite.CacheSizeKb != defaults.CacheSizeKb:
		t.Errorf("CacheSizeKb = %d, want %d", cfg.Sqlite.CacheSizeKb, defaults.CacheSizeKb)
	case cfg.Sqlite.BusyTimeoutMs != defaults.BusyTimeoutMs:
		t.Errorf("BusyTimeoutMs = %d, want %d", cfg.Sqlite.BusyTimeoutMs, defaults.BusyTimeoutMs)
	case cfg.Sqlite.MaxOpenConns != defaults.MaxOpenConns:
		t.Errorf("MaxOpenConns = %d, want %d", cfg.Sqlite.MaxOpenConns, defaults.MaxOpenConns)
	case cfg.Sqlite.CorruptedDatabaseRetention != defaults.CorruptedDatabaseRetention:
		t.Errorf("CorruptedDatabaseRetention = %s, want %s", cfg.Sqlite.CorruptedDatabaseRetention, defaults.CorruptedDatabaseRetention)
	case cfg.Sqlite.BackupInterval != defaults.BackupInterval:
		t.Errorf("BackupInterval = %s, want %s", cfg.Sqlite.BackupInterval, defaults.BackupInterval)
	case cfg.Sqlite.BackupRetention != defaults.BackupRetention:
		t.Errorf("BackupRetention = %s, want %s", cfg.Sqlite.BackupRetention, defaults.BackupRetention)
	}
}

// TestSqliteConfigFluentConstruction covers the documented fluent surface.
func TestSqliteConfigFluentConstruction(t *testing.T) {
	cfg := config.New().WithSqlite(
		config.NewSqliteConfig().
			WithJournalMode("DELETE").
			WithSynchronousMode("FULL").
			WithCacheSizeKb(-8000).
			WithBusyTimeout(2*time.Second).
			WithCheckpointAfterBatch(true).
			WithMaxOpenConns(2).
			WithMaxOpenDatabases(64).
			WithOpenDatabaseIdleTimeout(5*time.Minute).
			WithCorruptedDatabaseRecovery(true).
			WithCorruptedDatabaseRetention(24*time.Hour).
			WithBackup("backups", 30*time.Minute, 48*time.Hour).
			WithAutoRestoreFromBackup(true).
			WithBackupVerificationSkipped(true).
			WithOptimizeIdleTenantDatabases(true),
	)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	sqlite := cfg.Sqlite
	switch {
	case sqlite.JournalMode != "DELETE":
		t.Errorf("JournalMode = %q, want DELETE", sqlite.JournalMode)
	case sqlite.SynchronousMode != "FULL":
		t.Errorf("SynchronousMode = %q, want FULL", sqlite.SynchronousMode)
	case sqlite.CacheSizeKb != -8000:
		t.Errorf("CacheSizeKb = %d, want -8000", sqlite.CacheSizeKb)
	case sqlite.BusyTimeoutMs != 2000:
		t.Errorf("BusyTimeoutMs = %d, want 2000", sqlite.BusyTimeoutMs)
	case !sqlite.CheckpointAfterBatch:
		t.Error("CheckpointAfterBatch = false, want true")
	case sqlite.MaxOpenConns != 2:
		t.Errorf("MaxOpenConns = %d, want 2", sqlite.MaxOpenConns)
	case sqlite.MaxOpenDatabases != 64:
		t.Errorf("MaxOpenDatabases = %d, want 64", sqlite.MaxOpenDatabases)
	case sqlite.OpenDatabaseIdleTimeout != 5*time.Minute:
		t.Errorf("OpenDatabaseIdleTimeout = %s, want 5m", sqlite.OpenDatabaseIdleTimeout)
	case !sqlite.RecoverCorruptedDatabase:
		t.Error("RecoverCorruptedDatabase = false, want true")
	case sqlite.CorruptedDatabaseRetention != 24*time.Hour:
		t.Errorf("CorruptedDatabaseRetention = %s, want 24h", sqlite.CorruptedDatabaseRetention)
	case sqlite.BackupDirectory != "backups":
		t.Errorf("BackupDirectory = %q, want backups", sqlite.BackupDirectory)
	case sqlite.BackupInterval != 30*time.Minute:
		t.Errorf("BackupInterval = %s, want 30m", sqlite.BackupInterval)
	case sqlite.BackupRetention != 48*time.Hour:
		t.Errorf("BackupRetention = %s, want 48h", sqlite.BackupRetention)
	case !sqlite.AutoRestoreFromBackup:
		t.Error("AutoRestoreFromBackup = false, want true")
	case !sqlite.SkipBackupVerification:
		t.Error("SkipBackupVerification = false, want true")
	case !sqlite.OptimizeIdleTenantDatabases:
		t.Error("OptimizeIdleTenantDatabases = false, want true")
	}
}

// TestCloneIsolatesSqliteConfig proves a clone owns its own engine settings.
func TestCloneIsolatesSqliteConfig(t *testing.T) {
	original := config.New()
	clone := original.Clone()

	clone.Sqlite.JournalMode = "DELETE"
	clone.Sqlite.BackupDirectory = "elsewhere"

	if original.Sqlite.JournalMode == "DELETE" {
		t.Error("mutating the clone changed the original journal mode")
	}
	if original.Sqlite.BackupDirectory == "elsewhere" {
		t.Error("mutating the clone changed the original backup directory")
	}
}
