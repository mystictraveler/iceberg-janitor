package janitor_test

import (
	"context"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestCompact_SchemaEvolution_MixedSchemasSkipped is the correctness
// proof for the schema-evolution guard. It confirms that when a
// table's file set spans two schema versions, Compact refuses to
// cross the boundary: no write, no snapshot advance, no row loss. The
// round is marked Skipped with SkippedReason="mixed_schemas" so an
// operator can see the deferral in metrics or job logs.
//
// Rationale: schema evolutions on real tables happen at a cadence of
// days to weeks; compaction runs every few seconds on hot tables.
// Rather than rewriting data across a schema boundary (which would
// silently change column cardinality, lose dropped-column values,
// etc.), the janitor skips mixed rounds and lets the old-schema tail
// age out through Expire + OrphanFiles. See pkg/janitor/schema_group.go
// for the full rationale.
func TestCompact_SchemaEvolution_MixedSchemasSkipped(t *testing.T) {
	w := testutil.NewWarehouse(t)
	schema := testutil.SimpleFactSchema()
	tbl := w.CreateTable(t, "se", "events", schema)

	// Seed 3 batches at the ORIGINAL schema — schema id 0 on a fresh
	// table. Each Append is one commit → one data file.
	for i := 0; i < 3; i++ {
		rows := make([]testutil.SimpleFactRow, 4)
		for j := 0; j < 4; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     int64(i*4 + j),
				Value:  int64(j),
				Region: "us",
			}
		}
		tbl = testutil.AppendSimpleFactRows(t, tbl, rows)
	}

	// Evolve the schema: add a nullable "extra" column. iceberg-go's
	// UpdateSchema API stamps the new schema at schema_id+1; a
	// subsequent Append writes parquet with the evolved schema and
	// the new iceberg.schema.id key in the footer.
	tx := tbl.NewTransaction()
	if err := tx.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, icebergpkg.PrimitiveTypes.String, "evolved column", false, nil).
		Commit(); err != nil {
		t.Fatalf("UpdateSchema.Commit: %v", err)
	}
	var err error
	tbl, err = tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("evolve commit: %v", err)
	}

	// Seed 2 more batches at the evolved schema. The testutil helper
	// writes Arrow records using the table's CURRENT schema, so the
	// parquet files stamp the NEW schema_id.
	for i := 0; i < 2; i++ {
		rows := make([]testutil.SimpleFactRow, 4)
		for j := 0; j < 4; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     int64(100 + i*4 + j),
				Value:  int64(j + 100),
				Region: "eu",
			}
		}
		tbl = testutil.AppendSimpleFactRows(t, tbl, rows)
	}

	// Pre-compact: 5 data files, 3 at schema-v0 and 2 at schema-v1.
	beforeFiles, beforeRows := janitor.SnapshotFileStatsFast(context.Background(), tbl)
	if beforeFiles != 5 {
		t.Fatalf("before files = %d, want 5", beforeFiles)
	}
	if beforeRows != 20 {
		t.Fatalf("before rows = %d, want 20", beforeRows)
	}
	beforeSnap := tbl.CurrentSnapshot().SnapshotID

	// Act: Compact. Expected: mixed-schemas skip.
	res, cerr := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"se", "events"}, janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		})
	if cerr != nil {
		t.Fatalf("Compact returned error (should have returned nil-with-skip): %v", cerr)
	}

	// Verify the skip was recorded correctly.
	if !res.Skipped {
		t.Errorf("expected Skipped=true, got false")
	}
	if res.SkippedReason != "mixed_schemas" {
		t.Errorf("SkippedReason = %q, want %q", res.SkippedReason, "mixed_schemas")
	}
	if res.SkippedDetail == "" {
		t.Errorf("SkippedDetail should be populated with per-group breakdown, got empty string")
	}
	t.Logf("skipped_detail: %s", res.SkippedDetail)

	// Verify the snapshot did NOT advance — the table is untouched.
	reloaded := w.LoadTable(t, "se", "events")
	afterSnap := reloaded.CurrentSnapshot().SnapshotID
	if afterSnap != beforeSnap {
		t.Errorf("snapshot advanced: %d → %d, want no change (mixed-schema skip must not write)",
			beforeSnap, afterSnap)
	}

	// Verify file + row counts unchanged.
	afterFiles, afterRows := janitor.SnapshotFileStatsFast(context.Background(), reloaded)
	if afterFiles != beforeFiles {
		t.Errorf("after files = %d, want %d (unchanged)", afterFiles, beforeFiles)
	}
	if afterRows != beforeRows {
		t.Errorf("after rows = %d, want %d (no row loss)", afterRows, beforeRows)
	}
}

// TestCompact_SchemaEvolution_SingleSchemaRunsNormally is the
// happy-path check that proves the guard only skips when schemas
// actually differ. With all source files at the same (post-
// evolution) schema, Compact succeeds and reduces the file count.
func TestCompact_SchemaEvolution_SingleSchemaRunsNormally(t *testing.T) {
	w := testutil.NewWarehouse(t)
	schema := testutil.SimpleFactSchema()
	tbl := w.CreateTable(t, "se", "events", schema)

	// Evolve the schema BEFORE seeding any data. All subsequent files
	// will be written at the evolved schema.
	tx := tbl.NewTransaction()
	if err := tx.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, icebergpkg.PrimitiveTypes.String, "evolved column", false, nil).
		Commit(); err != nil {
		t.Fatalf("UpdateSchema.Commit: %v", err)
	}
	var err error
	tbl, err = tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("evolve commit: %v", err)
	}

	// Seed 5 batches all at the evolved schema.
	for i := 0; i < 5; i++ {
		rows := make([]testutil.SimpleFactRow, 4)
		for j := 0; j < 4; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     int64(i*4 + j),
				Value:  int64(j),
				Region: "us",
			}
		}
		tbl = testutil.AppendSimpleFactRows(t, tbl, rows)
	}

	res, cerr := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"se", "events"}, janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		})
	if cerr != nil {
		t.Fatalf("Compact: %v", cerr)
	}
	if res.Skipped {
		t.Errorf("single-schema round should NOT be skipped (got SkippedReason=%q, Detail=%q)",
			res.SkippedReason, res.SkippedDetail)
	}
	if res.AfterFiles >= res.BeforeFiles {
		t.Errorf("expected file reduction, got before=%d after=%d", res.BeforeFiles, res.AfterFiles)
	}
	if res.AfterRows != res.BeforeRows {
		t.Errorf("row count changed: before=%d after=%d", res.BeforeRows, res.AfterRows)
	}
}
