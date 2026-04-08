package state

import (
	"context"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

func newFileBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := blob.OpenBucket(context.Background(), "file://"+dir+"?create_dir=1")
	if err != nil {
		t.Fatalf("opening fileblob: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// TestLoad_MissingReturnsZero: brand-new tables have no state file;
// Load must return a zero-value state with the UUID set, NOT an error.
// This is the contract the circuit breaker depends on so the first
// failure on a new table doesn't crash trying to read a missing file.
func TestLoad_MissingReturnsZero(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	const uuid = "00000000-0000-0000-0000-000000000001"
	s, err := Load(ctx, bucket, uuid)
	if err != nil {
		t.Fatalf("Load on missing: %v", err)
	}
	if s == nil {
		t.Fatal("Load on missing: nil state")
	}
	if s.TableUUID != uuid {
		t.Errorf("Load on missing: TableUUID=%q want %q", s.TableUUID, uuid)
	}
	if s.ConsecutiveFailedRuns != 0 {
		t.Errorf("Load on missing: counter=%d want 0", s.ConsecutiveFailedRuns)
	}
}

// TestSaveLoad_RoundTrip: a saved state must come back byte-equal on
// load. The MaxRetainedErrors-truncation behavior is exercised by
// TestRecordFailure_TruncatesErrors below.
func TestSaveLoad_RoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	now := time.Date(2026, 4, 8, 9, 30, 0, 0, time.UTC)

	original := &TableState{
		TableUUID:             "abc",
		UpdatedAt:             now,
		ConsecutiveFailedRuns: 2,
		LastErrors: []ErrorRecord{
			{At: now.Add(-2 * time.Minute), Message: "first"},
			{At: now.Add(-1 * time.Minute), Message: "second"},
		},
		LastOutcome:   OutcomeFailure,
		LastSuccessAt: now.Add(-1 * time.Hour),
	}
	if err := Save(ctx, bucket, original); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := Load(ctx, bucket, "abc")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ConsecutiveFailedRuns != 2 {
		t.Errorf("counter=%d want 2", got.ConsecutiveFailedRuns)
	}
	if len(got.LastErrors) != 2 {
		t.Errorf("LastErrors len=%d want 2", len(got.LastErrors))
	}
	if got.LastOutcome != OutcomeFailure {
		t.Errorf("LastOutcome=%q want %q", got.LastOutcome, OutcomeFailure)
	}
	if !got.LastSuccessAt.Equal(now.Add(-1 * time.Hour)) {
		t.Errorf("LastSuccessAt=%v want %v", got.LastSuccessAt, now.Add(-1*time.Hour))
	}
}

// TestRecordFailure_TruncatesErrors: only the most recent
// MaxRetainedErrors entries should survive — older ones get dropped.
// This bounds the state file size under a long failure streak.
func TestRecordFailure_TruncatesErrors(t *testing.T) {
	s := &TableState{TableUUID: "x"}
	now := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	for i := 0; i < MaxRetainedErrors+5; i++ {
		s.RecordFailure(now.Add(time.Duration(i)*time.Second), "err"+string(rune('A'+i)))
	}
	if len(s.LastErrors) != MaxRetainedErrors {
		t.Fatalf("LastErrors len=%d want %d", len(s.LastErrors), MaxRetainedErrors)
	}
	// The retained errors must be the LAST N, not the first.
	// 8 inserts (A..H), keep last 3 → errF, errG, errH.
	if s.LastErrors[0].Message != "errF" {
		t.Errorf("first retained error = %q, want %q", s.LastErrors[0].Message, "errF")
	}
	if s.LastErrors[len(s.LastErrors)-1].Message != "errH" {
		t.Errorf("last retained error = %q, want %q", s.LastErrors[len(s.LastErrors)-1].Message, "errH")
	}
}

// TestRecordSuccess_ResetsCounter: a success on a state with prior
// failures must zero the counter and clear the LastErrors slice.
func TestRecordSuccess_ResetsCounter(t *testing.T) {
	s := &TableState{TableUUID: "x", ConsecutiveFailedRuns: 5}
	s.LastErrors = []ErrorRecord{{Message: "old"}}
	now := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	s.RecordSuccess(now)
	if s.ConsecutiveFailedRuns != 0 {
		t.Errorf("counter=%d want 0", s.ConsecutiveFailedRuns)
	}
	if len(s.LastErrors) != 0 {
		t.Errorf("LastErrors len=%d want 0", len(s.LastErrors))
	}
	if !s.LastSuccessAt.Equal(now) {
		t.Errorf("LastSuccessAt=%v want %v", s.LastSuccessAt, now)
	}
}

// TestPause_DeleteIdempotent: deleting a pause file that does not
// exist must NOT error. The operator's `resume` action should be
// safe to invoke whether or not the table is currently paused.
func TestPause_DeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	if err := DeletePause(ctx, bucket, "never-existed"); err != nil {
		t.Errorf("DeletePause on missing: %v", err)
	}
}

// TestPause_SaveLoadDelete exercises the full pause file lifecycle:
// not present → save → load → delete → not present.
func TestPause_SaveLoadDelete(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	const uuid = "p"
	if pause, _ := LoadPause(ctx, bucket, uuid); pause != nil {
		t.Fatalf("expected no pause initially, got %+v", pause)
	}
	now := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC)
	original := &PauseFile{
		TableUUID:   uuid,
		Reason:      "cb8_consecutive_failure: 3 consecutive failed runs",
		TriggeredAt: now,
		TriggeredBy: "test",
		LastErrors:  []ErrorRecord{{At: now, Message: "boom"}},
	}
	if err := SavePause(ctx, bucket, original); err != nil {
		t.Fatalf("SavePause: %v", err)
	}
	got, err := LoadPause(ctx, bucket, uuid)
	if err != nil || got == nil {
		t.Fatalf("LoadPause: pause=%v err=%v", got, err)
	}
	if got.Reason != original.Reason {
		t.Errorf("Reason=%q want %q", got.Reason, original.Reason)
	}
	if err := DeletePause(ctx, bucket, uuid); err != nil {
		t.Fatalf("DeletePause: %v", err)
	}
	if pause, _ := LoadPause(ctx, bucket, uuid); pause != nil {
		t.Errorf("expected no pause after delete, got %+v", pause)
	}
}
