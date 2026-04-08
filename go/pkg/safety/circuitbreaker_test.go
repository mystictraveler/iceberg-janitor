package safety

import (
	"context"
	"errors"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/state"
)

// newFileBucket opens a fresh fileblob bucket rooted at t.TempDir().
// fileblob is the in-process backend that lets us exercise every code
// path in the state and circuit breaker packages without spinning up
// MinIO. The CAS-correctness path is exercised by pkg/catalog/cas_test.go;
// here we only care about read/write/delete and the breaker's logic.
func newFileBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	dir := t.TempDir()
	bucket, err := blob.OpenBucket(context.Background(), "file://"+dir+"?create_dir=1")
	if err != nil {
		t.Fatalf("opening fileblob: %v", err)
	}
	t.Cleanup(func() { bucket.Close() })
	return bucket
}

// stubClock pins timeNow to a fixed value and returns a restore func.
// Used so failure timestamps in tests are deterministic and so the
// LastSuccessAt-zero check has a known reference.
func stubClock(t *testing.T, fixed time.Time) {
	t.Helper()
	saved := timeNow
	timeNow = func() time.Time { return fixed }
	t.Cleanup(func() { timeNow = saved })
}

// TestCB8_ConsecutiveFailureTrips is the headline test for issue #2.
// Two failures must NOT pause the table; the third one must. The state
// file must reflect the counter after each call, and the pause file
// must materialize exactly once on the trip.
func TestCB8_ConsecutiveFailureTrips(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "11111111-2222-3333-4444-555555555555"
	cb := New(bucket)

	// First failure: counter goes to 1, no pause file yet.
	if err := cb.RecordOutcome(ctx, uuid, errors.New("boom 1")); err != nil {
		t.Fatalf("RecordOutcome 1: %v", err)
	}
	s, err := state.Load(ctx, bucket, uuid)
	if err != nil {
		t.Fatalf("loading state after 1 failure: %v", err)
	}
	if s.ConsecutiveFailedRuns != 1 {
		t.Errorf("after 1 failure: counter=%d want 1", s.ConsecutiveFailedRuns)
	}
	if pause, _ := state.LoadPause(ctx, bucket, uuid); pause != nil {
		t.Errorf("pause file should not exist after 1 failure, got %+v", pause)
	}

	// Second failure: counter goes to 2, still no pause file.
	if err := cb.RecordOutcome(ctx, uuid, errors.New("boom 2")); err != nil {
		t.Fatalf("RecordOutcome 2: %v", err)
	}
	s, _ = state.Load(ctx, bucket, uuid)
	if s.ConsecutiveFailedRuns != 2 {
		t.Errorf("after 2 failures: counter=%d want 2", s.ConsecutiveFailedRuns)
	}
	if pause, _ := state.LoadPause(ctx, bucket, uuid); pause != nil {
		t.Errorf("pause file should not exist after 2 failures, got %+v", pause)
	}

	// Third failure: trips CB8.
	if err := cb.RecordOutcome(ctx, uuid, errors.New("boom 3")); err != nil {
		t.Fatalf("RecordOutcome 3: %v", err)
	}
	s, _ = state.Load(ctx, bucket, uuid)
	if s.ConsecutiveFailedRuns != 3 {
		t.Errorf("after 3 failures: counter=%d want 3", s.ConsecutiveFailedRuns)
	}
	pause, err := state.LoadPause(ctx, bucket, uuid)
	if err != nil {
		t.Fatalf("loading pause: %v", err)
	}
	if pause == nil {
		t.Fatalf("pause file should exist after 3 failures")
	}
	if pause.TableUUID != uuid {
		t.Errorf("pause.TableUUID = %q want %q", pause.TableUUID, uuid)
	}
	if len(pause.LastErrors) != 3 {
		t.Errorf("pause.LastErrors len=%d want 3", len(pause.LastErrors))
	}
	if pause.LastErrors[0].Message != "boom 1" || pause.LastErrors[2].Message != "boom 3" {
		t.Errorf("pause.LastErrors messages wrong: %+v", pause.LastErrors)
	}
}

// TestCB8_PreflightSkipsPaused: once a pause file exists, Preflight
// must return *PausedError without touching the table.
func TestCB8_PreflightSkipsPaused(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	cb := New(bucket)

	// Healthy table: Preflight is a no-op.
	if err := cb.Preflight(ctx, uuid); err != nil {
		t.Fatalf("Preflight on healthy table: %v", err)
	}

	// Trip the breaker.
	for i := 0; i < CB8DefaultThreshold; i++ {
		if err := cb.RecordOutcome(ctx, uuid, errors.New("boom")); err != nil {
			t.Fatalf("RecordOutcome %d: %v", i, err)
		}
	}

	// Now Preflight must refuse with *PausedError.
	err := cb.Preflight(ctx, uuid)
	if err == nil {
		t.Fatal("Preflight after trip: want PausedError, got nil")
	}
	if !IsPaused(err) {
		t.Errorf("Preflight after trip: want PausedError, got %T: %v", err, err)
	}
	var pe *PausedError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As to *PausedError failed for %v", err)
	}
	if pe.TableUUID != uuid {
		t.Errorf("PausedError.TableUUID = %q want %q", pe.TableUUID, uuid)
	}
}

// TestCB8_SuccessResetsCounter: a successful run after some failures
// must clear the counter and the LastErrors list. This is the
// "transient hiccup absorbed by retry" path that justifies CB8's
// "consecutive" qualifier — the counter must NOT be a lifetime total.
func TestCB8_SuccessResetsCounter(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "deadbeef-dead-beef-dead-beefdeadbeef"
	cb := New(bucket)

	// Two failures.
	for i := 0; i < 2; i++ {
		if err := cb.RecordOutcome(ctx, uuid, errors.New("transient")); err != nil {
			t.Fatalf("RecordOutcome failure %d: %v", i, err)
		}
	}
	// Success.
	if err := cb.RecordOutcome(ctx, uuid, nil); err != nil {
		t.Fatalf("RecordOutcome success: %v", err)
	}
	s, _ := state.Load(ctx, bucket, uuid)
	if s.ConsecutiveFailedRuns != 0 {
		t.Errorf("after success: counter=%d want 0", s.ConsecutiveFailedRuns)
	}
	if len(s.LastErrors) != 0 {
		t.Errorf("after success: LastErrors len=%d want 0", len(s.LastErrors))
	}
	if s.LastSuccessAt.IsZero() {
		t.Errorf("after success: LastSuccessAt should be set")
	}
	// Two more failures: should NOT trip (because the counter was
	// reset by the success above).
	for i := 0; i < 2; i++ {
		if err := cb.RecordOutcome(ctx, uuid, errors.New("post-success")); err != nil {
			t.Fatalf("RecordOutcome post-success %d: %v", i, err)
		}
	}
	if pause, _ := state.LoadPause(ctx, bucket, uuid); pause != nil {
		t.Errorf("pause file should NOT exist after success+2 failures, got %+v", pause)
	}
}

// TestCB8_Resume clears the pause file and resets the counter so the
// very next failure does not immediately re-trip.
func TestCB8_Resume(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "abcdef00-0000-0000-0000-000000000000"
	cb := New(bucket)

	for i := 0; i < CB8DefaultThreshold; i++ {
		_ = cb.RecordOutcome(ctx, uuid, errors.New("boom"))
	}
	if err := cb.Preflight(ctx, uuid); !IsPaused(err) {
		t.Fatalf("expected paused before resume, got %v", err)
	}

	if err := cb.Resume(ctx, uuid); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := cb.Preflight(ctx, uuid); err != nil {
		t.Errorf("Preflight after resume: want nil, got %v", err)
	}
	s, _ := state.Load(ctx, bucket, uuid)
	if s.ConsecutiveFailedRuns != 0 {
		t.Errorf("after resume: counter=%d want 0", s.ConsecutiveFailedRuns)
	}

	// One more failure must NOT immediately re-trip (the counter
	// was reset). This is the bug Resume specifically prevents.
	if err := cb.RecordOutcome(ctx, uuid, errors.New("first after resume")); err != nil {
		t.Fatalf("RecordOutcome after resume: %v", err)
	}
	if pause, _ := state.LoadPause(ctx, bucket, uuid); pause != nil {
		t.Errorf("pause file should NOT exist after resume + 1 failure, got %+v", pause)
	}
}

// TestCB8_ManualPause exercises the operator's "stop touching this
// table" path. Reason is required; the resulting pause file shows the
// reason prefixed with "manual:".
func TestCB8_ManualPause(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	cb := New(bucket)

	if err := cb.ManualPause(ctx, uuid, ""); err == nil {
		t.Error("ManualPause with empty reason should error")
	}
	if err := cb.ManualPause(ctx, uuid, "investigating broken partition spec"); err != nil {
		t.Fatalf("ManualPause: %v", err)
	}
	pause, err := state.LoadPause(ctx, bucket, uuid)
	if err != nil || pause == nil {
		t.Fatalf("pause file missing after ManualPause: pause=%v err=%v", pause, err)
	}
	if pause.Reason != "manual: investigating broken partition spec" {
		t.Errorf("pause.Reason = %q", pause.Reason)
	}
}

// TestCB1_FreshTableNotBlocked: a brand-new table (no state file) must
// pass Preflight even though its LastRunAt is the zero value. The
// cooldown applies to "touched recently", not "never touched".
func TestCB1_FreshTableNotBlocked(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "11111111-0000-0000-0000-000000000001"
	cb := New(bucket)

	if err := cb.Preflight(ctx, uuid); err != nil {
		t.Errorf("Preflight on fresh table: want nil, got %v", err)
	}
}

// TestCB1_RecentRunBlocks: once a successful run has stamped LastRunAt,
// the very next Preflight must return *CooldownError.
func TestCB1_RecentRunBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "22222222-0000-0000-0000-000000000002"
	cb := New(bucket)

	// Record a success — this stamps LastRunAt = stub clock.
	if err := cb.RecordOutcome(ctx, uuid, nil); err != nil {
		t.Fatalf("RecordOutcome success: %v", err)
	}

	err := cb.Preflight(ctx, uuid)
	if err == nil {
		t.Fatal("Preflight right after success: want CooldownError, got nil")
	}
	if !IsCooldown(err) {
		t.Errorf("Preflight right after success: want CooldownError, got %T: %v", err, err)
	}
	var ce *CooldownError
	if !errors.As(err, &ce) {
		t.Fatalf("errors.As to *CooldownError failed for %v", err)
	}
	if ce.TableUUID != uuid {
		t.Errorf("CooldownError.TableUUID = %q want %q", ce.TableUUID, uuid)
	}
	if ce.RemainingDuration <= 0 || ce.RemainingDuration > CB1DefaultCooldown {
		t.Errorf("CooldownError.RemainingDuration = %v; want (0, %v]", ce.RemainingDuration, CB1DefaultCooldown)
	}

	// A failure is also "a recent run" and must also block cooldown.
	// Advance the clock past cooldown first, record failure, then check.
	bucket2 := newFileBucket(t)
	cb2 := New(bucket2)
	const uuid2 = "22222222-0000-0000-0000-000000000003"
	if err := cb2.RecordOutcome(ctx, uuid2, errors.New("boom")); err != nil {
		t.Fatalf("RecordOutcome failure: %v", err)
	}
	if err := cb2.Preflight(ctx, uuid2); !IsCooldown(err) {
		t.Errorf("Preflight after failure: want CooldownError, got %v", err)
	}
}

// TestCB1_AfterCooldownPasses: advance the stub clock past the
// cooldown duration and Preflight must pass.
func TestCB1_AfterCooldownPasses(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	t0 := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)

	saved := timeNow
	current := t0
	timeNow = func() time.Time { return current }
	t.Cleanup(func() { timeNow = saved })

	const uuid = "33333333-0000-0000-0000-000000000004"
	cb := New(bucket)

	if err := cb.RecordOutcome(ctx, uuid, nil); err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}

	// Immediately: blocked.
	if err := cb.Preflight(ctx, uuid); !IsCooldown(err) {
		t.Fatalf("Preflight immediate: want CooldownError, got %v", err)
	}

	// Advance past the cooldown: allowed.
	current = t0.Add(CB1DefaultCooldown + time.Second)
	if err := cb.Preflight(ctx, uuid); err != nil {
		t.Errorf("Preflight after cooldown: want nil, got %v", err)
	}
}

// TestCB1_PauseTakesPrecedence: when both a pause file and a recent
// run exist, Preflight must return *PausedError (not CooldownError).
// Rationale: pause is a stronger signal — the operator has explicitly
// said "stop", and surfacing a cooldown message instead would be
// misleading (the cooldown will clear on its own; the pause won't).
func TestCB1_PauseTakesPrecedence(t *testing.T) {
	ctx := context.Background()
	bucket := newFileBucket(t)
	stubClock(t, time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC))

	const uuid = "44444444-0000-0000-0000-000000000005"
	cb := New(bucket)

	// Recent run (stamps LastRunAt).
	if err := cb.RecordOutcome(ctx, uuid, nil); err != nil {
		t.Fatalf("RecordOutcome: %v", err)
	}
	// Manual pause.
	if err := cb.ManualPause(ctx, uuid, "investigating"); err != nil {
		t.Fatalf("ManualPause: %v", err)
	}

	err := cb.Preflight(ctx, uuid)
	if err == nil {
		t.Fatal("Preflight with pause+recent: want error, got nil")
	}
	if !IsPaused(err) {
		t.Errorf("Preflight with pause+recent: want PausedError, got %T: %v", err, err)
	}
	if IsCooldown(err) {
		t.Errorf("Preflight with pause+recent: should NOT be CooldownError, got %v", err)
	}
}

// TestCB8_NilBreaker: a nil receiver must be safe so callers can
// idiomatically gate features without explicit nil checks at the
// call site.
func TestCB8_NilBreaker(t *testing.T) {
	var cb *CircuitBreaker
	if err := cb.Preflight(context.Background(), "anything"); err != nil {
		t.Errorf("nil Preflight: %v", err)
	}
	if err := cb.RecordOutcome(context.Background(), "anything", errors.New("x")); err != nil {
		t.Errorf("nil RecordOutcome: %v", err)
	}
}
