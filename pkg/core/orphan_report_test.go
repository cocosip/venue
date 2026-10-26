package core

import (
	"fmt"
	"testing"
)

// TestOrphanRecoveryReportCapsErrorDetail is the regression test for a detail
// list that grew with the number of failures.
//
// A scan can fail once per physical file, so a volume with a persistent fault
// accumulates one string per file for the whole run. The counter must stay exact
// while the detail list is bounded, otherwise a large volume turns a reported
// failure into unbounded memory growth.
func TestOrphanRecoveryReportCapsErrorDetail(t *testing.T) {
	const attempts = OrphanRecoveryReportErrorLimit * 3

	report := NewOrphanRecoveryReport()
	for i := 0; i < attempts; i++ {
		report.FilesFailed++
		report.AddError("recover file %d", i)
	}

	if len(report.Errors) != OrphanRecoveryReportErrorLimit {
		t.Fatalf("len(Errors) = %d, want the cap of %d", len(report.Errors), OrphanRecoveryReportErrorLimit)
	}

	// The retained descriptions must be the first ones: a later failure is
	// dropped, never allowed to replace an earlier record.
	if report.Errors[0] != "recover file 0" {
		t.Errorf("Errors[0] = %q, want the first failure", report.Errors[0])
	}
	wantLast := fmt.Sprintf("recover file %d", OrphanRecoveryReportErrorLimit-1)
	if got := report.Errors[len(report.Errors)-1]; got != wantLast {
		t.Errorf("Errors[last] = %q, want %q", got, wantLast)
	}

	// The counter is the exact figure and is not capped.
	if report.FilesFailed != attempts {
		t.Errorf("FilesFailed = %d, want the exact count %d", report.FilesFailed, attempts)
	}
}
