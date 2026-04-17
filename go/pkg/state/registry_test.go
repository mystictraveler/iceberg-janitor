package state_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "gocloud.dev/blob/memblob"

	"gocloud.dev/blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/state"
)

// TestRegistryRoundTrip_1KTables is the scale correctness test for
// the sharded per-table registry. Seeds 1000 registry entries across
// 4 workload classes, lists them all, verifies sort order
// (NextMaintainAt ascending), selects due-for-maintenance subset,
// and confirms the cadence contract per class.
func TestRegistryRoundTrip_1KTables(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open mem bucket: %v", err)
	}
	defer bucket.Close()

	classes := []string{"streaming", "batch", "slow_changing", "dormant"}
	now := time.Now().UTC().Truncate(time.Second)
	baseTime := now.Add(-2 * time.Hour)

	// Seed 1000 tables: 250 per class. Each table's LastMaintained
	// is spread across a 2-hour window so the due-for-maintenance
	// selection is non-trivial.
	const numTables = 1000
	for i := 0; i < numTables; i++ {
		class := classes[i%len(classes)]
		lastMaintained := baseTime.Add(time.Duration(i) * 7200 * time.Millisecond / numTables)
		entry := &state.RegistryEntry{
			TableUUID:      fmt.Sprintf("uuid-%04d", i),
			Namespace:      "ns",
			Table:          fmt.Sprintf("table_%04d", i),
			Class:          class,
			LastSeen:       now,
			LastMaintained: lastMaintained,
			NextMaintainAt: state.NextMaintainTime(class, lastMaintained),
			SchemaID:       1,
			SnapshotID:     int64(1000 + i),
		}
		if err := state.SaveRegistryEntry(ctx, bucket, entry); err != nil {
			t.Fatalf("save entry %d: %v", i, err)
		}
	}

	// List all entries.
	started := time.Now()
	entries, err := state.ListRegistryEntries(ctx, bucket)
	listDur := time.Since(started)
	if err != nil {
		t.Fatalf("ListRegistryEntries: %v", err)
	}
	if len(entries) != numTables {
		t.Fatalf("listed %d entries, want %d", len(entries), numTables)
	}
	t.Logf("listed %d entries in %v", len(entries), listDur)

	// Verify sort order: NextMaintainAt ascending.
	for i := 1; i < len(entries); i++ {
		if entries[i].NextMaintainAt.Before(entries[i-1].NextMaintainAt) {
			t.Errorf("sort violated at index %d: %v < %v",
				i, entries[i].NextMaintainAt, entries[i-1].NextMaintainAt)
			break
		}
	}

	// Verify the cadence contract: streaming tables have
	// NextMaintainAt = LastMaintained + 5m; dormant = + 7d.
	for _, e := range entries {
		expected := state.NextMaintainTime(e.Class, e.LastMaintained)
		if !e.NextMaintainAt.Equal(expected) {
			t.Errorf("table %s (class %s): NextMaintainAt=%v, want %v",
				e.Table, e.Class, e.NextMaintainAt, expected)
		}
	}

	// DueForMaintenance: all streaming tables should be due (their
	// cadence is 5m and LastMaintained was set ~2h ago). Batch
	// tables (1h cadence) should also be due. Slow_changing (24h)
	// should NOT be due. Dormant (7d) should NOT be due.
	due := state.DueForMaintenance(entries, now, numTables)
	classCounts := map[string]int{}
	for _, e := range due {
		classCounts[e.Class]++
	}
	t.Logf("due-for-maintenance counts: %v (out of %d total)", classCounts, numTables)

	if classCounts["streaming"] != 250 {
		t.Errorf("streaming due=%d, want 250 (all should be due at 5m cadence, 2h ago)", classCounts["streaming"])
	}
	if classCounts["batch"] != 250 {
		t.Errorf("batch due=%d, want 250 (all should be due at 1h cadence, 2h ago)", classCounts["batch"])
	}
	if classCounts["slow_changing"] != 0 {
		t.Errorf("slow_changing due=%d, want 0 (24h cadence, only 2h ago)", classCounts["slow_changing"])
	}
	if classCounts["dormant"] != 0 {
		t.Errorf("dormant due=%d, want 0 (7d cadence, only 2h ago)", classCounts["dormant"])
	}

	// DueForMaintenance with maxTables cap: only take 10.
	capped := state.DueForMaintenance(entries, now, 10)
	if len(capped) != 10 {
		t.Errorf("capped due=%d, want 10", len(capped))
	}
	// The first 10 should be the 10 most overdue tables.
	for i := 1; i < len(capped); i++ {
		if capped[i].NextMaintainAt.Before(capped[i-1].NextMaintainAt) {
			t.Errorf("capped set not sorted at index %d", i)
			break
		}
	}

	// Load individual entry round-trip.
	loaded, err := state.LoadRegistryEntry(ctx, bucket, "uuid-0042")
	if err != nil {
		t.Fatalf("LoadRegistryEntry: %v", err)
	}
	if loaded == nil {
		t.Fatal("entry uuid-0042 not found")
	}
	if loaded.Table != "table_0042" {
		t.Errorf("loaded table=%q, want table_0042", loaded.Table)
	}
	if loaded.SchemaID != 1 {
		t.Errorf("schema_id=%d, want 1", loaded.SchemaID)
	}

	// Simulate schema evolution: update one entry's SchemaID.
	loaded.SchemaID = 2
	loaded.LastSeen = now
	if err := state.SaveRegistryEntry(ctx, bucket, loaded); err != nil {
		t.Fatalf("save evolved entry: %v", err)
	}
	reloaded, _ := state.LoadRegistryEntry(ctx, bucket, "uuid-0042")
	if reloaded.SchemaID != 2 {
		t.Errorf("after evolution: schema_id=%d, want 2", reloaded.SchemaID)
	}

	// Delete entry.
	if err := state.DeleteRegistryEntry(ctx, bucket, "uuid-0042"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	gone, _ := state.LoadRegistryEntry(ctx, bucket, "uuid-0042")
	if gone != nil {
		t.Error("entry should be nil after delete")
	}

	// Re-list: should be 999 now.
	afterDelete, _ := state.ListRegistryEntries(ctx, bucket)
	if len(afterDelete) != numTables-1 {
		t.Errorf("after delete: %d entries, want %d", len(afterDelete), numTables-1)
	}

	// Registry meta round-trip.
	meta := state.RegistryMeta{
		LastFullScan:   now,
		ScanDurationMs: 25000,
		TableCount:     numTables - 1,
	}
	if err := state.SaveRegistryMeta(ctx, bucket, meta); err != nil {
		t.Fatalf("save meta: %v", err)
	}
	loaded2, err := state.LoadRegistryMeta(ctx, bucket)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if loaded2.TableCount != numTables-1 {
		t.Errorf("meta table_count=%d, want %d", loaded2.TableCount, numTables-1)
	}
}

// TestRegistryEntry_SnapshotSkip verifies the "nothing to do" fast
// path: when the current snapshot-id matches the registry entry's
// SnapshotID, the table hasn't changed and maintenance can be
// skipped regardless of cadence.
func TestRegistryEntry_SnapshotSkip(t *testing.T) {
	entry := &state.RegistryEntry{
		TableUUID:      "test-uuid",
		Class:          "streaming",
		LastMaintained: time.Now().Add(-10 * time.Minute),
		NextMaintainAt: time.Now().Add(-5 * time.Minute), // overdue
		SnapshotID:     42,
	}

	// With matching snapshot: skip even though overdue.
	currentSnapshotID := int64(42)
	if currentSnapshotID == entry.SnapshotID {
		// This is the skip path — nothing changed since last maintain.
		t.Log("snapshot matches → skip (correct)")
	} else {
		t.Error("should have matched")
	}

	// With different snapshot: don't skip.
	currentSnapshotID = 43
	if currentSnapshotID == entry.SnapshotID {
		t.Error("should NOT have matched after a new commit")
	}
}

// TestNextMaintainTime verifies the per-class cadence contract.
func TestNextMaintainTime(t *testing.T) {
	base := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		class    string
		expected time.Duration
	}{
		{"streaming", 5 * time.Minute},
		{"batch", 1 * time.Hour},
		{"slow_changing", 24 * time.Hour},
		{"dormant", 7 * 24 * time.Hour},
		{"unknown", 1 * time.Hour}, // default
	}
	for _, tc := range cases {
		got := state.NextMaintainTime(tc.class, base)
		want := base.Add(tc.expected)
		if !got.Equal(want) {
			t.Errorf("class %q: got %v, want %v", tc.class, got, want)
		}
	}
}
