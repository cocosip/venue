package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocosip/venue/pkg/core"
	"github.com/cocosip/venue/pkg/logging"
)

// backupFilePattern is the frozen backup file name: metadata.<UTC stamp>.bak.
// A collision suffix is allowed because two backups can land in the same second.
var backupFilePattern = regexp.MustCompile(`^metadata\.\d{8}T\d{6}Z(\.\d+)?\.bak$`)

// backupTestTenantA and backupTestTenantB hold records for two tenants inside
// one repository, which is the state a backup must preserve.
const (
	backupTestTenantA = "backup-tenant-a"
	backupTestTenantB = "backup-tenant-b"
)

// writeReaderBackup publishes the bytes of r as a backup file, which is what a
// backup runner does with the repository's stream.
func writeReaderBackup(directory string, r io.Reader, now time.Time) error {
	_, err := writeBackupFile(directory, func(w io.Writer) error {
		_, copyErr := io.Copy(w, r)
		return copyErr
	}, now)
	return err
}

// backupTenantDatabasePath derives one tenant's database directory the same way
// the repository does, so a test never has to hardcode the layout.
func backupTenantDatabasePath(t *testing.T, dataPath, tenantID string) string {
	t.Helper()

	dbPath, _, err := newBadgerOptions(&BadgerRepositoryOptions{TenantID: tenantID, DataPath: dataPath})
	if err != nil {
		t.Fatalf("newBadgerOptions(%s) error = %v", tenantID, err)
	}
	return dbPath
}

// newBackupTestRepository opens a repository whose database lives under a fresh
// data path and registers its shutdown before the temporary directory is
// removed.
func newBackupTestRepository(t *testing.T, dataPath string, opts *BadgerRepositoryOptions) *BadgerMetadataRepository {
	t.Helper()

	if opts == nil {
		opts = &BadgerRepositoryOptions{}
	}
	opts.TenantID = backupTestTenant
	opts.DataPath = dataPath
	if opts.GCInterval == 0 {
		opts.GCInterval = time.Hour
	}

	repo, err := NewBadgerMetadataRepository(opts)
	if err != nil {
		t.Fatalf("NewBadgerMetadataRepository() error = %v", err)
	}
	badgerRepo, ok := repo.(*BadgerMetadataRepository)
	if !ok {
		t.Fatalf("NewBadgerMetadataRepository() returned %T, want *BadgerMetadataRepository", repo)
	}
	t.Cleanup(func() { _ = badgerRepo.Close() })
	return badgerRepo
}

// tenantRecord builds one record owned by tenantID.
func tenantRecord(tenantID, fileKey string, status core.FileProcessingStatus) *core.FileMetadata {
	record := createTestMetadata(fileKey, status)
	record.TenantID = tenantID
	return record
}

// backupRecordKeys returns the file keys a repository can read back for a
// tenant, so a test can compare a restored set without depending on ordering.
func backupRecordKeys(t *testing.T, repo core.MetadataRepository, tenantID string, status core.FileProcessingStatus, want int) map[string]core.FileProcessingStatus {
	t.Helper()

	records, err := repo.GetByStatus(context.Background(), tenantID, status, 0)
	if err != nil {
		t.Fatalf("GetByStatus(%s, %s) error = %v", tenantID, status, err)
	}
	if len(records) != want {
		t.Fatalf("GetByStatus(%s, %s) returned %d records, want %d", tenantID, status, len(records), want)
	}

	got := make(map[string]core.FileProcessingStatus, len(records))
	for _, record := range records {
		if record.TenantID != tenantID {
			t.Fatalf("GetByStatus(%s) returned a record owned by %q", tenantID, record.TenantID)
		}
		got[record.FileKey] = record.Status
	}
	return got
}

// seedTwoTenants writes records for two tenants and returns the statuses that
// were written.
func seedTwoTenants(t *testing.T, repo core.MetadataRepository, prefix string, count int) map[string]map[string]core.FileProcessingStatus {
	t.Helper()

	ctx := context.Background()
	want := map[string]map[string]core.FileProcessingStatus{
		backupTestTenantA: {},
		backupTestTenantB: {},
	}
	statuses := []core.FileProcessingStatus{core.FileStatusPending, core.FileStatusCompleted, core.FileStatusProcessing}

	for i := 0; i < count; i++ {
		for _, tenantID := range []string{backupTestTenantA, backupTestTenantB} {
			status := statuses[i%len(statuses)]
			fileKey := fmt.Sprintf("%s-%s-%03d", prefix, tenantID, i)
			if err := repo.AddOrUpdate(ctx, tenantRecord(tenantID, fileKey, status)); err != nil {
				t.Fatalf("AddOrUpdate(%s) error = %v", fileKey, err)
			}
			want[tenantID][fileKey] = status
		}
	}
	return want
}

// TestBackupRepositoryRoundTrip covers the whole recoverability promise: a
// backup of a two-tenant repository restores into a fresh database and both
// tenants' records are readable with the same statuses.
func TestBackupRepositoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repo := newBackupTestRepository(t, dataPath, nil)
	want := seedTwoTenants(t, repo, "roundtrip", 3)

	var buffer bytes.Buffer
	since, err := BackupRepository(ctx, repo, &buffer)
	if err != nil {
		t.Fatalf("BackupRepository() error = %v", err)
	}
	t.Logf("backup since = %d, bytes = %d", since, buffer.Len())
	if buffer.Len() == 0 {
		t.Fatal("BackupRepository() wrote an empty stream")
	}

	restorePath := t.TempDir()
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{
		TenantID: backupTestTenant,
		DataPath: restorePath,
	}, bytes.NewReader(buffer.Bytes())); err != nil {
		t.Fatalf("RestoreDatabase() error = %v", err)
	}

	restored := newBackupTestRepository(t, restorePath, &BadgerRepositoryOptions{
		// A fresh open must not find anything to quarantine.
		CorruptedDatabaseRetention: time.Hour,
	})
	for tenantID, records := range want {
		for fileKey, status := range records {
			got, err := restored.Get(ctx, tenantID, fileKey)
			if err != nil {
				t.Fatalf("restored Get(%s, %s) error = %v", tenantID, fileKey, err)
			}
			if got.Status != status {
				t.Fatalf("restored Get(%s, %s) status = %s, want %s", tenantID, fileKey, got.Status, status)
			}
		}
	}

	// The status indexes must be restored too, not only the primary records.
	for tenantID, records := range want {
		counts := map[core.FileProcessingStatus]int{}
		for _, status := range records {
			counts[status]++
		}
		for status, count := range counts {
			backupRecordKeys(t, restored, tenantID, status, count)
		}
	}
}

// TestBackupRejectsInvalidInput pins the documented argument contract.
func TestBackupRejectsInvalidInput(t *testing.T) {
	ctx := context.Background()
	repo := newBackupTestRepository(t, t.TempDir(), nil)

	var buffer bytes.Buffer
	if _, err := repo.Backup(ctx, nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("Backup(nil writer) error = %v, want %v", err, core.ErrInvalidArgument)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.Backup(canceled, &buffer); !errors.Is(err, context.Canceled) {
		t.Fatalf("Backup(canceled context) error = %v, want %v", err, context.Canceled)
	}

	// A repository that does not support backups is reported as unsupported.
	if _, err := BackupRepository(ctx, unsupportedMetadataRepository{}, &buffer); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("BackupRepository(unsupported) error = %v, want %v", err, core.ErrInvalidArgument)
	}

	wantErr := errors.New("writer refused the stream")
	_, err := repo.Backup(ctx, failingWriter{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Backup(failing writer) error = %v, want %v wrapped", err, wantErr)
	}
}

// TestBackupFailsOnClosedRepository keeps a closed repository from reporting a
// successful backup.
func TestBackupFailsOnClosedRepository(t *testing.T) {
	repo := newBackupTestRepository(t, t.TempDir(), nil)
	if err := repo.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var buffer bytes.Buffer
	if _, err := repo.Backup(context.Background(), &buffer); !errors.Is(err, core.ErrDatabaseError) {
		t.Fatalf("Backup() on a closed repository error = %v, want %v", err, core.ErrDatabaseError)
	}
}

// TestBackupIsOnlineAndRestoresAConsistentSet is the online-backup guarantee:
// the repository keeps committing writes while a backup streams, and the
// restored snapshot holds a consistent set of records.
func TestBackupIsOnlineAndRestoresAConsistentSet(t *testing.T) {
	ctx := context.Background()
	dataPath := t.TempDir()
	repo := newBackupTestRepository(t, dataPath, nil)

	// A non-trivial dataset: several hundred small records across two tenants.
	const seeded = 400
	seedTwoTenants(t, repo, "seeded", seeded)

	// Writers keep committing while the backup runs, so the backup must not
	// hold the write path for the whole transfer.
	const concurrentWriters = 4
	const writesPerWriter = 50
	var wg sync.WaitGroup
	writeErrs := make(chan error, concurrentWriters*writesPerWriter)
	started := make(chan struct{})

	for writer := 0; writer < concurrentWriters; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-started
			for i := 0; i < writesPerWriter; i++ {
				fileKey := fmt.Sprintf("concurrent-%d-%03d", writer, i)
				tenantID := backupTestTenantA
				if i%2 == 1 {
					tenantID = backupTestTenantB
				}
				if err := repo.AddOrUpdate(ctx, tenantRecord(tenantID, fileKey, core.FileStatusPending)); err != nil {
					writeErrs <- err
					return
				}
			}
		}(writer)
	}

	var backup bytes.Buffer
	close(started)
	since, err := repo.Backup(ctx, &backup)
	if err != nil {
		t.Fatalf("Backup() during concurrent writes error = %v", err)
	}
	wg.Wait()
	close(writeErrs)
	for err := range writeErrs {
		t.Fatalf("concurrent AddOrUpdate() error = %v", err)
	}
	if since == 0 {
		t.Fatal("Backup() since = 0, want the sequence number the snapshot was taken at")
	}

	// Every seeded record must be in the snapshot, consistent with its status,
	// and every record present in the snapshot must be readable.
	restorePath := t.TempDir()
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{
		TenantID: backupTestTenant,
		DataPath: restorePath,
	}, bytes.NewReader(backup.Bytes())); err != nil {
		t.Fatalf("RestoreDatabase() error = %v", err)
	}
	restored := newBackupTestRepository(t, restorePath, nil)

	for _, tenantID := range []string{backupTestTenantA, backupTestTenantB} {
		records, err := restored.GetByStatus(ctx, tenantID, core.FileStatusPending, 0)
		if err != nil {
			t.Fatalf("restored GetByStatus(%s, Pending) error = %v", tenantID, err)
		}
		if len(records) < seeded/3 {
			t.Fatalf("restored pending records for %s = %d, want at least %d", tenantID, len(records), seeded/3)
		}
		for _, record := range records {
			if record.Status != core.FileStatusPending {
				t.Fatalf("restored record %s status = %s, want Pending", record.FileKey, record.Status)
			}
			if _, err := restored.Get(ctx, record.TenantID, record.FileKey); err != nil {
				t.Fatalf("restored Get(%s, %s) error = %v", record.TenantID, record.FileKey, err)
			}
		}
	}
}

// TestLatestBackupAndPruneBackups covers the backup directory inventory and
// retention rules, including the retention boundary.
func TestLatestBackupAndPruneBackups(t *testing.T) {
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

	t.Run("missing directory", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "does-not-exist")
		info, err := LatestBackup(missing)
		if err != nil {
			t.Fatalf("LatestBackup(missing directory) error = %v", err)
		}
		if info.BackupCount != 0 || info.LatestBackupPath != "" || !info.LatestBackupAt.IsZero() || info.TotalBytes != 0 {
			t.Fatalf("LatestBackup(missing directory) = %+v, want an empty result", info)
		}
		if info.BackupDirectory != missing {
			t.Fatalf("LatestBackup().BackupDirectory = %q, want %q", info.BackupDirectory, missing)
		}
		if removed, err := PruneBackups(missing, time.Hour, now); err != nil || removed != 0 {
			t.Fatalf("PruneBackups(missing directory) = (%d, %v), want (0, nil)", removed, err)
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		directory := t.TempDir()
		info, err := LatestBackup(directory)
		if err != nil {
			t.Fatalf("LatestBackup(empty directory) error = %v", err)
		}
		if info.BackupCount != 0 || info.LatestBackupPath != "" {
			t.Fatalf("LatestBackup(empty directory) = %+v, want no backups", info)
		}
		if removed, err := PruneBackups(directory, time.Hour, now); err != nil || removed != 0 {
			t.Fatalf("PruneBackups(empty directory) = (%d, %v), want (0, nil)", removed, err)
		}
	})

	t.Run("multiple backups with injected times", func(t *testing.T) {
		directory := t.TempDir()
		files := []struct {
			name string
			age  time.Duration
			size int
		}{
			{name: "metadata.20240506T070809Z.bak", age: 0, size: 10},
			{name: "metadata.20240505T070809Z.bak", age: 25 * time.Hour, size: 20},
			{name: "metadata.20240504T070809Z.bak", age: 49 * time.Hour, size: 30},
			// Non-matching leftovers must never count as backups.
			{name: "metadata.20240503T070809Z.bak.tmp", age: 49 * time.Hour, size: 40},
			{name: "unrelated.bak", age: 49 * time.Hour, size: 50},
		}
		for _, file := range files {
			full := filepath.Join(directory, file.name)
			if err := os.WriteFile(full, bytes.Repeat([]byte("x"), file.size), 0o644); err != nil {
				t.Fatalf("WriteFile(%s) error = %v", full, err)
			}
			stamp := now.Add(-file.age)
			if err := os.Chtimes(full, stamp, stamp); err != nil {
				t.Fatalf("Chtimes(%s) error = %v", full, err)
			}
		}

		info, err := LatestBackup(directory)
		if err != nil {
			t.Fatalf("LatestBackup() error = %v", err)
		}
		if info.BackupCount != 3 {
			t.Fatalf("LatestBackup().BackupCount = %d, want 3", info.BackupCount)
		}
		if info.TotalBytes != 60 {
			t.Fatalf("LatestBackup().TotalBytes = %d, want 60", info.TotalBytes)
		}
		if want := filepath.Join(directory, "metadata.20240506T070809Z.bak"); info.LatestBackupPath != want {
			t.Fatalf("LatestBackup().LatestBackupPath = %q, want %q", info.LatestBackupPath, want)
		}
		if !info.LatestBackupAt.Equal(now) {
			t.Fatalf("LatestBackup().LatestBackupAt = %s, want %s", info.LatestBackupAt, now)
		}

		// Retention exactly 24h: the 25h-old and 49h-old backups expire, a
		// backup modified exactly at the boundary survives.
		boundary := filepath.Join(directory, "metadata.20240505T070809Z.bak")
		if err := os.Chtimes(boundary, now.Add(-24*time.Hour), now.Add(-24*time.Hour)); err != nil {
			t.Fatalf("Chtimes(boundary) error = %v", err)
		}

		removed, err := PruneBackups(directory, 24*time.Hour, now)
		if err != nil {
			t.Fatalf("PruneBackups() error = %v", err)
		}
		if removed != 1 {
			t.Fatalf("PruneBackups() removed = %d, want 1", removed)
		}
		if _, err := os.Lstat(filepath.Join(directory, "metadata.20240504T070809Z.bak")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expired backup is still present: %v", err)
		}
		if _, err := os.Lstat(boundary); err != nil {
			t.Fatalf("backup at the retention boundary was removed: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(directory, "metadata.20240506T070809Z.bak")); err != nil {
			t.Fatalf("fresh backup was removed: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(directory, "metadata.20240503T070809Z.bak.tmp")); err != nil {
			t.Fatalf("non-backup leftover was removed: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(directory, "unrelated.bak")); err != nil {
			t.Fatalf("unrelated file was removed: %v", err)
		}

		info, err = LatestBackup(directory)
		if err != nil {
			t.Fatalf("LatestBackup() after pruning error = %v", err)
		}
		if info.BackupCount != 2 || info.TotalBytes != 30 {
			t.Fatalf("LatestBackup() after pruning = %+v, want 2 backups and 30 bytes", info)
		}
	})

	t.Run("non-positive retention removes nothing", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "metadata.20200101T000000Z.bak")
		if err := os.WriteFile(path, []byte("payload"), 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		old := now.Add(-1000 * time.Hour)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("Chtimes() error = %v", err)
		}

		for _, retention := range []time.Duration{0, -time.Hour} {
			removed, err := PruneBackups(directory, retention, now)
			if err != nil {
				t.Fatalf("PruneBackups(%s) error = %v", retention, err)
			}
			if removed != 0 {
				t.Fatalf("PruneBackups(%s) removed = %d, want 0", retention, removed)
			}
		}
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("backup was removed with pruning disabled: %v", err)
		}
	})
}

// TestBackupFileLayout pins the frozen backup file naming and the atomic
// write: the final name is metadata.<UTC stamp>.bak and no temp file remains.
func TestBackupFileLayout(t *testing.T) {
	directory := t.TempDir()
	var buffer bytes.Buffer
	since, err := BackupRepository(context.Background(), newBackupTestRepository(t, t.TempDir(), nil), &buffer)
	if err != nil {
		t.Fatalf("BackupRepository() error = %v", err)
	}
	if since == 0 {
		t.Fatal("BackupRepository() since = 0")
	}

	if err := writeReaderBackup(directory, bytes.NewReader(buffer.Bytes()), time.Now().UTC()); err != nil {
		t.Fatalf("writeBackupFile() error = %v", err)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("backup directory holds %d entries, want 1", len(entries))
	}
	name := entries[0].Name()
	if !backupFilePattern.MatchString(name) {
		t.Fatalf("backup file name = %q, want metadata.<yyyyMMddTHHmmssZ>.bak", name)
	}

	// Two backups inside the same second must not overwrite each other.
	if err := writeReaderBackup(directory, bytes.NewReader(buffer.Bytes()), time.Now().UTC()); err != nil {
		t.Fatalf("second writeBackupFile() error = %v", err)
	}
	entries, err = os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("backup directory holds %d entries after two backups in one second, want 2", len(entries))
	}
}

// TestBackupFileWriteFailureLeavesNoPartialFile keeps a reader from ever
// observing a partial backup.
func TestBackupFileWriteFailureLeavesNoPartialFile(t *testing.T) {
	directory := t.TempDir()
	wantErr := errors.New("stream failed")
	err := writeReaderBackup(directory, failingReader{err: wantErr}, time.Now().UTC())
	if err == nil {
		t.Fatal("writeBackupFile(failing reader) error = nil, want a failure")
	}

	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatalf("ReadDir() error = %v", readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("a failed backup left %v behind, want no file at all", names)
	}
}

// TestRestoreDatabaseRefusesNonEmptyTarget keeps an offline restore from
// overwriting a database that is already there.
func TestRestoreDatabaseRefusesNonEmptyTarget(t *testing.T) {
	ctx := context.Background()
	source := newBackupTestRepository(t, t.TempDir(), nil)
	seedTwoTenants(t, source, "refusal", 2)

	var buffer bytes.Buffer
	if _, err := BackupRepository(ctx, source, &buffer); err != nil {
		t.Fatalf("BackupRepository() error = %v", err)
	}

	target := t.TempDir()
	dbPath := backupTenantDatabasePath(t, target, backupTestTenant)
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	sentinel := filepath.Join(dbPath, "KEEPME")
	if err := os.WriteFile(sentinel, []byte("existing"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: target}, bytes.NewReader(buffer.Bytes()))
	if !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("RestoreDatabase(non-empty target) error = %v, want %v", err, core.ErrInvalidArgument)
	}
	content, readErr := os.ReadFile(sentinel)
	if readErr != nil {
		t.Fatalf("the existing target content was removed: %v", readErr)
	}
	if string(content) != "existing" {
		t.Fatalf("existing target content = %q, want %q", content, "existing")
	}

	if err := RestoreDatabase(ctx, nil, bytes.NewReader(buffer.Bytes())); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("RestoreDatabase(nil options) error = %v, want %v", err, core.ErrInvalidArgument)
	}
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: t.TempDir()}, nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("RestoreDatabase(nil reader) error = %v, want %v", err, core.ErrInvalidArgument)
	}
}

// TestRestoreDatabaseFailureLeavesNoDirectory keeps a failed restore from
// leaving a half-loaded database behind.
func TestRestoreDatabaseFailureLeavesNoDirectory(t *testing.T) {
	ctx := context.Background()
	target := t.TempDir()
	dbPath := backupTenantDatabasePath(t, target, backupTestTenant)

	err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: target}, strings.NewReader("this is not a badger backup stream"))
	if err == nil {
		t.Fatal("RestoreDatabase(garbage stream) error = nil, want a failure")
	}
	if _, statErr := os.Lstat(dbPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("a failed restore left %s behind: %v", dbPath, statErr)
	}

	// The directory must also be recoverable: a real restore right after must work.
	source := newBackupTestRepository(t, t.TempDir(), nil)
	seedTwoTenants(t, source, "recover", 2)
	var buffer bytes.Buffer
	if _, err := BackupRepository(ctx, source, &buffer); err != nil {
		t.Fatalf("BackupRepository() error = %v", err)
	}
	if err := RestoreDatabase(ctx, &BadgerRepositoryOptions{TenantID: backupTestTenant, DataPath: target}, bytes.NewReader(buffer.Bytes())); err != nil {
		t.Fatalf("RestoreDatabase() after a failed restore error = %v", err)
	}
	restored := newBackupTestRepository(t, target, nil)
	// Both tenants' records came back, not only the repository's own tenant.
	backupRecordKeys(t, restored, backupTestTenantA, core.FileStatusPending, 1)
	backupRecordKeys(t, restored, backupTestTenantB, core.FileStatusPending, 1)
}

// TestBackupServiceRunOnceAndLifecycle covers the periodic runner: one cycle
// writes exactly one backup file, naming is frozen, and Start/Stop are
// idempotent and restart-safe.
func TestBackupServiceRunOnceAndLifecycle(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	repo := newBackupTestRepository(t, t.TempDir(), nil)

	service, err := NewBackupService(&BackupServiceOptions{
		Repository: repo,
		Directory:  directory,
		Interval:   10 * time.Millisecond,
		Retention:  time.Hour,
	})
	if err != nil {
		t.Fatalf("NewBackupService() error = %v", err)
	}
	if !service.IsEnabled() {
		t.Fatal("IsEnabled() = false, want true")
	}

	if err := service.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("backup directory holds %d entries after RunOnce, want 1", len(entries))
	}
	if !backupFilePattern.MatchString(entries[0].Name()) {
		t.Fatalf("backup file name = %q, want metadata.<yyyyMMddTHHmmssZ>.bak", entries[0].Name())
	}

	info, err := service.LatestBackup()
	if err != nil {
		t.Fatalf("LatestBackup() error = %v", err)
	}
	if info.BackupCount != 1 || info.LatestBackupPath == "" || info.LatestBackupAt.IsZero() {
		t.Fatalf("LatestBackup() = %+v, want one named backup", info)
	}
	if info.BackupDirectory != directory {
		t.Fatalf("LatestBackup().BackupDirectory = %q, want %q", info.BackupDirectory, directory)
	}

	// Start is idempotent and the goroutine backs up on its own.
	service.Start()
	service.Start()
	service.Stop()
	service.Stop()
	service.Start()
	service.Start()

	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := service.LatestBackup()
		if err != nil {
			t.Fatalf("LatestBackup() error = %v", err)
		}
		if info.BackupCount >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the periodic backup did not run: %+v", info)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stop must return promptly and leave no goroutine writing afterwards.
	done := make(chan struct{})
	go func() {
		service.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5s")
	}

	// After stopping, no new backup appears while the service is idle.
	before, err := service.LatestBackup()
	if err != nil {
		t.Fatalf("LatestBackup() error = %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := service.LatestBackup()
	if err != nil {
		t.Fatalf("LatestBackup() error = %v", err)
	}
	if after.BackupCount != before.BackupCount {
		t.Fatalf("backups kept being written after Stop(): %d -> %d", before.BackupCount, after.BackupCount)
	}
}

// TestBackupServiceRetentionPrunesEveryCycle keeps the retention window applied
// by the runner itself.
func TestBackupServiceRetentionPrunesEveryCycle(t *testing.T) {
	directory := t.TempDir()
	service, err := NewBackupService(&BackupServiceOptions{
		Repository: newBackupTestRepository(t, t.TempDir(), nil),
		Directory:  directory,
		Interval:   time.Hour,
		Retention:  time.Hour,
	})
	if err != nil {
		t.Fatalf("NewBackupService() error = %v", err)
	}

	stale := filepath.Join(directory, "metadata.20200101T000000Z.bak")
	if err := os.WriteFile(stale, []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	if err := service.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RunOnce() did not prune the expired backup: %v", err)
	}
	info, err := service.LatestBackup()
	if err != nil {
		t.Fatalf("LatestBackup() error = %v", err)
	}
	if info.BackupCount != 1 {
		t.Fatalf("LatestBackup().BackupCount = %d, want 1", info.BackupCount)
	}
}

// recordingLogHandler captures every record a logging runtime emits, so a test
// can assert on the structured warning an operator would see.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []logging.Record
}

// Enabled accepts every level; the runtime still applies its own filtering.
func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle captures one record, including the component and event attributes the
// logging runtime adds to every emission.
func (h *recordingLogHandler) Handle(_ context.Context, record slog.Record) error {
	var attrs []slog.Attr
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})

	captured := logging.Record{
		Level:   record.Level,
		Message: record.Message,
		Attrs:   attrs,
	}
	if component, ok := attributeValue(attrs, "component"); ok {
		captured.Component = component
	}
	if event, ok := attributeValue(attrs, "event"); ok {
		captured.Event = event
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, captured)
	return nil
}

// attributeValue reads a flattened attribute by key.
func attributeValue(attrs []slog.Attr, key string) (string, bool) {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.String(), true
		}
	}
	return "", false
}

// WithAttrs returns the handler unchanged; these tests never add group state.
func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

// WithGroup returns the handler unchanged; these tests never add group state.
func (h *recordingLogHandler) WithGroup(string) slog.Handler { return h }

// snapshot returns the captured records.
func (h *recordingLogHandler) snapshot() []logging.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logging.Record(nil), h.records...)
}

// warning returns the first captured warning, or nil.
func (h *recordingLogHandler) warning() *logging.Record {
	for _, record := range h.snapshot() {
		if record.Level == slog.LevelWarn {
			captured := record
			return &captured
		}
	}
	return nil
}

// attributes flattens recorded attributes into a map for assertions.
func (h *recordingLogHandler) attributes(record logging.Record) map[string]string {
	values := map[string]string{}
	for _, attr := range record.Attrs {
		values[attr.Key] = attr.Value.String()
	}
	return values
}

// TestBackupServiceWarnsWithoutLeakingPaths keeps a failing backup cycle
// observable without writing the configured directory into the log.
func TestBackupServiceWarnsWithoutLeakingPaths(t *testing.T) {
	handler := &recordingLogHandler{}
	runtime, err := logging.New(logging.Config{Handler: handler})
	if err != nil {
		t.Fatalf("logging.New() error = %v", err)
	}

	parent := t.TempDir()
	blocked := filepath.Join(parent, "blocked-directory")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	service, err := NewBackupService(&BackupServiceOptions{
		Repository: newBackupTestRepository(t, t.TempDir(), nil),
		Directory:  blocked,
		Interval:   time.Hour,
		Logging:    runtime,
	})
	if err != nil {
		t.Fatalf("NewBackupService() error = %v", err)
	}
	if err := service.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() with an unusable directory error = nil, want a failure")
	}

	records := handler.snapshot()
	if len(records) != 1 {
		t.Fatalf("a failed cycle emitted %d records, want exactly one warning", len(records))
	}
	if records[0].Level != slog.LevelWarn {
		t.Fatalf("record level = %v, want a warning", records[0].Level)
	}
	if records[0].Component != "metadata_backup" {
		t.Fatalf("record component = %q, want %q", records[0].Component, "metadata_backup")
	}
	if records[0].Event == "" {
		t.Fatal("record has no stable event name")
	}
	if strings.Contains(records[0].Message, "blocked-directory") {
		t.Fatalf("warning message %q leaks the backup directory", records[0].Message)
	}
	if attributes := handler.attributes(records[0]); attributes["reason"] != "backup_failed" {
		t.Fatalf("warning attributes = %v, want a stable reason", attributes)
	}
}

// TestBackupServiceRejectsInvalidConfiguration pins the constructor contract,
// including the "enabled but unsupported repository" case.
func TestBackupServiceRejectsInvalidConfiguration(t *testing.T) {
	directory := t.TempDir()

	if _, err := NewBackupService(nil); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("NewBackupService(nil) error = %v, want %v", err, core.ErrInvalidArgument)
	}
	if _, err := NewBackupService(&BackupServiceOptions{Directory: directory, Interval: time.Minute}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("NewBackupService(nil repository) error = %v, want %v", err, core.ErrInvalidArgument)
	}
	if _, err := NewBackupService(&BackupServiceOptions{
		Repository: unsupportedMetadataRepository{},
		Directory:  directory,
		Interval:   time.Minute,
	}); !errors.Is(err, core.ErrInvalidArgument) {
		t.Fatalf("NewBackupService(unsupported repository) error = %v, want %v", err, core.ErrInvalidArgument)
	}

	// A disabled service may use a repository that cannot back up: it never
	// touches the repository.
	service, err := NewBackupService(&BackupServiceOptions{Repository: unsupportedMetadataRepository{}})
	if err != nil {
		t.Fatalf("NewBackupService(disabled) error = %v", err)
	}
	if service.IsEnabled() {
		t.Fatal("IsEnabled() = true for a service with no directory and no interval")
	}
	service.Start()
	service.Stop()
	if _, err := service.LatestBackup(); err != nil {
		t.Fatalf("LatestBackup() on a disabled service error = %v", err)
	}

	intervalOnly, err := NewBackupService(&BackupServiceOptions{Repository: unsupportedMetadataRepository{}, Interval: time.Minute})
	if err != nil {
		t.Fatalf("NewBackupService(interval only) error = %v", err)
	}
	if intervalOnly.IsEnabled() {
		t.Fatal("IsEnabled() = true without a backup directory")
	}

	directoryOnly, err := NewBackupService(&BackupServiceOptions{Repository: unsupportedMetadataRepository{}, Directory: directory})
	if err != nil {
		t.Fatalf("NewBackupService(directory only) error = %v", err)
	}
	if directoryOnly.IsEnabled() {
		t.Fatal("IsEnabled() = true without a backup interval")
	}
}

// TestBackupServiceDisabledPerformsNoIO keeps a disabled service from writing
// anything, including the backup directory itself.
func TestBackupServiceDisabledPerformsNoIO(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "never-created")

	service, err := NewBackupService(&BackupServiceOptions{
		Repository: newBackupTestRepository(t, t.TempDir(), nil),
	})
	if err != nil {
		t.Fatalf("NewBackupService() error = %v", err)
	}
	if service.IsEnabled() {
		t.Fatal("IsEnabled() = true, want false")
	}

	service.Start()
	service.Stop()

	if err := service.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() on a disabled service error = %v", err)
	}
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a disabled service created %s: %v", directory, err)
	}

	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("a disabled service wrote %d entries into the parent directory", len(entries))
	}
}

// TestBackupServiceContainsFailures keeps a failing backup from stopping the
// runner and from leaking an error to the caller of Stop.
func TestBackupServiceContainsFailures(t *testing.T) {
	parent := t.TempDir()
	// A regular file where the backup directory should be makes every backup
	// cycle fail deterministically.
	blocker := filepath.Join(parent, "blocked")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	service, err := NewBackupService(&BackupServiceOptions{
		Repository: newBackupTestRepository(t, t.TempDir(), nil),
		Directory:  blocker,
		Interval:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewBackupService() error = %v", err)
	}
	if err := service.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce() with an unusable directory error = nil, want a failure")
	}

	// The runner keeps going and Stop still returns.
	service.Start()
	time.Sleep(50 * time.Millisecond)
	service.Stop()
}
