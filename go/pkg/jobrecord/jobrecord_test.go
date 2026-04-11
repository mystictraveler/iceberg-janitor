package jobrecord

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

func newBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := blob.OpenBucket(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestPath(t *testing.T) {
	got := Path("abc-123")
	want := "_janitor/state/jobs/abc-123.json"
	if got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
}

func TestWrite_RejectsEmptyID(t *testing.T) {
	bucket := newBucket(t)
	_, err := Write(context.Background(), bucket, &Record{})
	if err == nil {
		t.Error("expected error for empty ID")
	}
}

func TestWrite_RejectsNil(t *testing.T) {
	bucket := newBucket(t)
	_, err := Write(context.Background(), bucket, nil)
	if err == nil {
		t.Error("expected error for nil record")
	}
}

func TestWrite_AndRead_RoundTrip(t *testing.T) {
	bucket := newBucket(t)
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{
		ID:          "abc-123",
		Table:       "tpcds.store_sales",
		Operation:   "maintain",
		Status:      "running",
		CreatedAt:   now,
		HeartbeatAt: now.Add(2 * time.Minute),
		LeaseNonce:  "nonce-xyz",
		Owner:       "host:42:uuid",
	}
	path, err := Write(context.Background(), bucket, rec)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if path != Path(rec.ID) {
		t.Errorf("Write returned %q, want %q", path, Path(rec.ID))
	}

	got, err := Read(context.Background(), bucket, "abc-123")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.ID != rec.ID || got.Table != rec.Table || got.Operation != rec.Operation {
		t.Errorf("ID/Table/Operation mismatch: got %+v", got)
	}
	if got.Status != rec.Status {
		t.Errorf("Status: got %q, want %q", got.Status, rec.Status)
	}
	if !got.CreatedAt.Equal(rec.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", got.CreatedAt, rec.CreatedAt)
	}
	if !got.HeartbeatAt.Equal(rec.HeartbeatAt) {
		t.Errorf("HeartbeatAt: got %v, want %v", got.HeartbeatAt, rec.HeartbeatAt)
	}
	if got.LeaseNonce != rec.LeaseNonce {
		t.Errorf("LeaseNonce: got %q, want %q", got.LeaseNonce, rec.LeaseNonce)
	}
	if got.Owner != rec.Owner {
		t.Errorf("Owner: got %q, want %q", got.Owner, rec.Owner)
	}
}

func TestWrite_OverwritesExisting(t *testing.T) {
	bucket := newBucket(t)
	first := &Record{ID: "abc", Status: "pending", Table: "ns.t", Operation: "maintain", CreatedAt: time.Now()}
	if _, err := Write(context.Background(), bucket, first); err != nil {
		t.Fatalf("first write: %v", err)
	}
	second := &Record{ID: "abc", Status: "completed", Table: "ns.t", Operation: "maintain", CreatedAt: time.Now()}
	if _, err := Write(context.Background(), bucket, second); err != nil {
		t.Fatalf("second write: %v", err)
	}
	got, err := Read(context.Background(), bucket, "abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.Status != "completed" {
		t.Errorf("after overwrite: Status = %q, want completed", got.Status)
	}
}

func TestRead_NotFound(t *testing.T) {
	bucket := newBucket(t)
	_, err := Read(context.Background(), bucket, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRead_RejectsEmptyID(t *testing.T) {
	bucket := newBucket(t)
	_, err := Read(context.Background(), bucket, "")
	if err == nil {
		t.Error("expected error for empty jobID")
	}
}

func TestRead_PreservesResultJSON(t *testing.T) {
	bucket := newBucket(t)
	resultPayload, _ := json.Marshal(map[string]any{
		"files":   42,
		"bytes":   1024,
		"removed": []int{1, 2, 3},
	})
	rec := &Record{
		ID:        "abc",
		Table:     "ns.t",
		Operation: "maintain",
		Status:    "completed",
		CreatedAt: time.Now(),
		Result:    resultPayload,
	}
	if _, err := Write(context.Background(), bucket, rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := Read(context.Background(), bucket, "abc")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got.Result, &decoded); err != nil {
		t.Fatalf("Result roundtrip: %v", err)
	}
	if decoded["files"].(float64) != 42 {
		t.Errorf("Result.files = %v, want 42", decoded["files"])
	}
}

func TestDelete_DeletesAndIdempotent(t *testing.T) {
	bucket := newBucket(t)
	rec := &Record{ID: "abc", Table: "ns.t", Operation: "maintain", Status: "completed", CreatedAt: time.Now()}
	if _, err := Write(context.Background(), bucket, rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := Delete(context.Background(), bucket, "abc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := Read(context.Background(), bucket, "abc"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after Delete: expected ErrNotFound, got %v", err)
	}
	// Idempotent: second Delete on missing record is fine.
	if err := Delete(context.Background(), bucket, "abc"); err != nil {
		t.Errorf("second Delete on missing: got %v, want nil", err)
	}
}

func TestDelete_RejectsEmptyID(t *testing.T) {
	bucket := newBucket(t)
	if err := Delete(context.Background(), bucket, ""); err == nil {
		t.Error("expected error for empty jobID")
	}
}

// === Heartbeat staleness ===

func TestIsHeartbeatStale_PendingNeverStale(t *testing.T) {
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{Status: "pending", CreatedAt: now.Add(-1 * time.Hour)}
	if rec.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("pending status: should not be stale")
	}
}

func TestIsHeartbeatStale_CompletedNeverStale(t *testing.T) {
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{Status: "completed", HeartbeatAt: now.Add(-2 * time.Hour)}
	if rec.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("completed status: should not be stale even with old heartbeat")
	}
}

func TestIsHeartbeatStale_FailedNeverStale(t *testing.T) {
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{Status: "failed", HeartbeatAt: now.Add(-2 * time.Hour)}
	if rec.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("failed status: should not be stale even with old heartbeat")
	}
}

func TestIsHeartbeatStale_RunningWithFreshHeartbeat(t *testing.T) {
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{Status: "running", HeartbeatAt: now.Add(-2 * time.Minute)}
	if rec.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("fresh heartbeat: should not be stale")
	}
}

func TestIsHeartbeatStale_RunningWithStaleHeartbeat(t *testing.T) {
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	rec := &Record{Status: "running", HeartbeatAt: now.Add(-15 * time.Minute)}
	if !rec.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("15-min-old heartbeat with maxAge=10m: should be stale")
	}
}

func TestIsHeartbeatStale_RunningWithNoHeartbeatGracePeriod(t *testing.T) {
	// Running, no heartbeat yet (just created). Should give it
	// one maxAge before considering stale.
	now := time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC)
	freshlyCreated := &Record{Status: "running", CreatedAt: now.Add(-2 * time.Minute)}
	if freshlyCreated.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("just-created running job: should get grace period")
	}
	wayOld := &Record{Status: "running", CreatedAt: now.Add(-30 * time.Minute)}
	if !wayOld.IsHeartbeatStale(now, 10*time.Minute) {
		t.Errorf("30-min-old running job with no heartbeat: should be stale")
	}
}

func TestIsHeartbeatStale_NilRecord(t *testing.T) {
	var rec *Record
	if !rec.IsHeartbeatStale(time.Now(), 10*time.Minute) {
		t.Errorf("nil record: should be stale")
	}
}
