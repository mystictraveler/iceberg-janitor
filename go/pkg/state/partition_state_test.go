package state

import (
	"context"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

// TestPartitionState_SaveLoadRoundTrip verifies the fileblob round-trip
// preserves all record fields. Uses t.TempDir for an in-process
// fileblob with no external dependencies.
func TestPartitionState_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Close()

	const uuid = "00000000-0000-0000-0000-000000000001"
	in := &PartitionState{
		TableUUID: uuid,
		Partitions: map[string]PartitionRecord{
			"ss_store_sk=5": {
				PartitionKey:     "ss_store_sk=5",
				LastRewriteAt:    time.Now().UTC().Truncate(time.Millisecond),
				LastRewriteFiles: 1,
				LastRewriteBytes: 1024 * 1024,
				LastRewriteOp:    "compact",
			},
			"ss_store_sk=7": {
				PartitionKey:     "ss_store_sk=7",
				LastRewriteAt:    time.Now().UTC().Truncate(time.Millisecond),
				LastRewriteFiles: 3,
				LastRewriteBytes: 4096 * 1024,
				LastRewriteOp:    "stitch",
			},
		},
	}

	if err := SavePartitionState(ctx, bucket, in); err != nil {
		t.Fatalf("SavePartitionState: %v", err)
	}

	out, err := LoadPartitionState(ctx, bucket, uuid)
	if err != nil {
		t.Fatalf("LoadPartitionState: %v", err)
	}
	if out.TableUUID != uuid {
		t.Errorf("TableUUID mismatch: got %s, want %s", out.TableUUID, uuid)
	}
	if len(out.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(out.Partitions))
	}
	for k, inRec := range in.Partitions {
		outRec, ok := out.Partitions[k]
		if !ok {
			t.Errorf("partition %s missing after round-trip", k)
			continue
		}
		if outRec.LastRewriteFiles != inRec.LastRewriteFiles {
			t.Errorf("%s: LastRewriteFiles mismatch: got %d, want %d", k, outRec.LastRewriteFiles, inRec.LastRewriteFiles)
		}
		if outRec.LastRewriteBytes != inRec.LastRewriteBytes {
			t.Errorf("%s: LastRewriteBytes mismatch: got %d, want %d", k, outRec.LastRewriteBytes, inRec.LastRewriteBytes)
		}
		if outRec.LastRewriteOp != inRec.LastRewriteOp {
			t.Errorf("%s: LastRewriteOp mismatch: got %s, want %s", k, outRec.LastRewriteOp, inRec.LastRewriteOp)
		}
	}
}

// TestPartitionState_LoadMissing returns an empty state when the file
// doesn't exist — first-run semantics.
func TestPartitionState_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Close()

	s, err := LoadPartitionState(ctx, bucket, "does-not-exist")
	if err != nil {
		t.Fatalf("expected nil err on missing state, got %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil state on missing file")
	}
	if len(s.Partitions) != 0 {
		t.Errorf("expected empty Partitions, got %d", len(s.Partitions))
	}
	if s.TableUUID != "does-not-exist" {
		t.Errorf("expected TableUUID to be set, got %q", s.TableUUID)
	}
}

// TestPartitionState_Record updates a record and verifies LastRewriteAt
// is set to roughly now.
func TestPartitionState_Record(t *testing.T) {
	s := &PartitionState{TableUUID: "u", Partitions: map[string]PartitionRecord{}}

	before := time.Now()
	s.Record("ss_store_sk=5", "compact", 1, 1024)
	after := time.Now()

	rec, ok := s.Partitions["ss_store_sk=5"]
	if !ok {
		t.Fatal("record not stored")
	}
	if rec.PartitionKey != "ss_store_sk=5" {
		t.Errorf("PartitionKey mismatch: %s", rec.PartitionKey)
	}
	if rec.LastRewriteFiles != 1 {
		t.Errorf("LastRewriteFiles: %d", rec.LastRewriteFiles)
	}
	if rec.LastRewriteBytes != 1024 {
		t.Errorf("LastRewriteBytes: %d", rec.LastRewriteBytes)
	}
	if rec.LastRewriteOp != "compact" {
		t.Errorf("LastRewriteOp: %s", rec.LastRewriteOp)
	}
	if rec.LastRewriteAt.Before(before) || rec.LastRewriteAt.After(after) {
		t.Errorf("LastRewriteAt out of range: %v (want between %v and %v)", rec.LastRewriteAt, before, after)
	}
}

// TestPartitionState_LastRewriteAges computes ages relative to a given
// "now" and skips zero-value records.
func TestPartitionState_LastRewriteAges(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	s := &PartitionState{
		Partitions: map[string]PartitionRecord{
			"recent": {
				PartitionKey:  "recent",
				LastRewriteAt: now.Add(-30 * time.Minute),
			},
			"old": {
				PartitionKey:  "old",
				LastRewriteAt: now.Add(-48 * time.Hour),
			},
			"never": {
				PartitionKey: "never",
				// LastRewriteAt is zero value; should be skipped
			},
		},
	}
	ages := s.LastRewriteAges(now)
	if len(ages) != 2 {
		t.Errorf("expected 2 non-zero ages, got %d: %v", len(ages), ages)
	}
	if ages["recent"] != 30*time.Minute {
		t.Errorf("recent age: got %v, want 30m", ages["recent"])
	}
	if ages["old"] != 48*time.Hour {
		t.Errorf("old age: got %v, want 48h", ages["old"])
	}
	if _, ok := ages["never"]; ok {
		t.Error("zero-value record should be skipped from ages map")
	}
}
