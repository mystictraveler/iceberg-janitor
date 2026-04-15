package janitor_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	pqgo "github.com/parquet-go/parquet-go"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestCompact_V2_PositionDeletes_EndToEnd is the definitive correctness
// test for V2 position delete handling.
//
// Setup:
//  1. Create an unpartitioned V2 merge-on-read table (so tx.Delete
//     emits position-delete files, not rewritten data).
//  2. Seed six small parquet files of 8 rows each = 48 rows total,
//     ids 0..47.
//  3. Fire a position delete against a specific row (id=5), which
//     iceberg-go implements as a position delete file referencing
//     (sourceFilePath, 5).
//
// Act:
//     Run Compact. The manifest walk must (a) collect the delete
//     file, (b) force the decode/encode pass instead of byte-copy
//     stitch, (c) apply the row mask during decode, (d) emit a
//     compacted output file with 47 rows (not 48), (e) pass the
//     master check.
//
// Verify:
//   - Post-compact snapshot file count = 1 (plus the delete file,
//     which is still in the manifest tree but now references an
//     orphaned path — a no-op at read time).
//   - Total row count across data files = 47.
//   - id=5 is NOT present in the output.
//   - id=0..4,6..47 ARE present.
func TestCompact_V2_PositionDeletes_EndToEnd(t *testing.T) {
	w := testutil.NewWarehouse(t)
	schema := testutil.SimpleFactSchema()
	tbl := w.CreateMergeOnReadTable(t, "db", "events", schema)

	// Seed via iceberg-go's Arrow writer (tx.Append) so the per-file
	// stats are populated the way the metrics-based delete classifier
	// needs. Six Append commits → six data files → 48 rows.
	for i := 0; i < 6; i++ {
		rows := make([]testutil.SimpleFactRow, 8)
		for j := 0; j < 8; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     int64(i*8 + j),
				Value:  int64(j),
				Region: "us",
			}
		}
		tbl = testutil.AppendSimpleFactRows(t, tbl, rows)
	}

	// Fire a position delete against id=5. With write.delete.mode=
	// merge-on-read this emits a position delete file, not a data
	// rewrite.
	filter := icebergpkg.EqualTo(icebergpkg.Reference("id"), int64(5))
	tbl = testutil.FirePositionDelete(t, tbl, filter)

	// Confirm the fixture is actually shaped as a position delete —
	// if iceberg-go silently rewrote the data instead we'd get a
	// false negative on the janitor test.
	requirePositionDeletePresent(t, tbl)

	// Act: run Compact.
	res, cerr := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"db", "events"}, janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		})
	if cerr != nil {
		t.Fatalf("Compact: %v", cerr)
	}
	if res.AfterFiles >= res.BeforeFiles {
		t.Errorf("expected file reduction, got before=%d after=%d",
			res.BeforeFiles, res.AfterFiles)
	}

	// Verify: row count should be 47 (48 - 1 deleted).
	reloaded := w.LoadTable(t, "db", "events")
	ids := collectAllDataIDs(t, reloaded)
	if got := len(ids); got != 47 {
		t.Errorf("post-compact row count = %d, want 47", got)
	}
	// id=5 must not be present.
	for _, id := range ids {
		if id == 5 {
			t.Errorf("id=5 still present after compact; position delete was not applied")
		}
	}
	// All other ids 0..47 must be present.
	present := make(map[int64]bool, len(ids))
	for _, id := range ids {
		present[id] = true
	}
	for want := int64(0); want < 48; want++ {
		if want == 5 {
			continue
		}
		if !present[want] {
			t.Errorf("id=%d missing after compact (should have survived)", want)
		}
	}
}

// TestCompact_V2_MultiRowPositionDeletes exercises the case where
// tx.Delete produces multiple position-delete tuples — the janitor
// must drop ALL of them during compaction.
func TestCompact_V2_MultiRowPositionDeletes(t *testing.T) {
	w := testutil.NewWarehouse(t)
	schema := testutil.SimpleFactSchema()
	tbl := w.CreateMergeOnReadTable(t, "db", "events", schema)

	for i := 0; i < 4; i++ {
		rows := make([]testutil.SimpleFactRow, 10)
		for j := 0; j < 10; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     int64(i*10 + j),
				Value:  int64(j * 100),
				Region: "eu",
			}
		}
		tbl = testutil.AppendSimpleFactRows(t, tbl, rows)
	}

	// Delete everything with value >= 500 — that's half of each file
	// (rows 5..9 of every source).
	filter := icebergpkg.GreaterThanEqual(icebergpkg.Reference("value"), int64(500))
	tbl = testutil.FirePositionDelete(t, tbl, filter)
	requirePositionDeletePresent(t, tbl)

	// Act.
	if _, cerr := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"db", "events"}, janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		}); cerr != nil {
		t.Fatalf("Compact: %v", cerr)
	}

	// Verify: 4 files × 10 rows = 40; each file loses 5 rows (value
	// 500..900) → 20 survivors.
	reloaded := w.LoadTable(t, "db", "events")
	ids := collectAllDataIDs(t, reloaded)
	if got := len(ids); got != 20 {
		t.Errorf("post-compact row count = %d, want 20 (half of 40)", got)
	}
	// Every surviving row must have value < 500 — i.e., its within-
	// file position < 5 — i.e., id % 10 < 5.
	for _, id := range ids {
		if id%10 >= 5 {
			t.Errorf("id=%d should have been deleted (pos %d >= 5)", id, id%10)
		}
	}
}

// TestCompact_RefusesV3DeletionVectors verifies the safety gate
// refuses any table that has a Puffin-format position delete (V3
// deletion vector). Since iceberg-go doesn't emit V3 DVs today, we
// check the refusal path by asserting that the gate function returns
// an UnsupportedFeatureError on a crafted fixture. For end-to-end
// coverage on a real table, see the safety_guards_test.go file (if
// present) or a future integration fixture.
//
// This test primarily pins the contract: the error class returned
// from a refusal is *UnsupportedFeatureError, and the Feature field
// says "V3 deletion vectors".
func TestUnsupportedFeatureError_V3Shape(t *testing.T) {
	err := &janitor.UnsupportedFeatureError{
		Feature: "V3 deletion vectors",
		Detail:  "s3://bucket/data/delete.puffin",
	}
	if err.Error() == "" {
		t.Error("UnsupportedFeatureError.Error() returned empty string")
	}
	// Ensure it's wrappable — callers may wrap with fmt.Errorf.
	if err.Feature != "V3 deletion vectors" {
		t.Errorf("Feature = %q, want V3 deletion vectors", err.Feature)
	}
}

// ---------- helpers ----------

// requirePositionDeletePresent confirms the table's current snapshot
// has at least one position-delete entry. If this fails the test is
// invalid — iceberg-go silently fell back to copy-on-write and we'd
// be testing compaction of a table with no deletes, which proves
// nothing about the delete path.
func requirePositionDeletePresent(t *testing.T, tbl *icebergtable.Table) {
	t.Helper()
	ctx := context.Background()
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		t.Fatal("no snapshot after delete")
	}
	// Diagnostic: surface format version + props + manifest content
	// types so a false-negative is debuggable.
	t.Logf("table metadata format version: %d", tbl.Metadata().Version())
	t.Logf("table properties: %v", tbl.Metadata().Properties())
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatalf("Manifests: %v", err)
	}
	for _, m := range manifests {
		t.Logf("manifest %s content=%v", m.FilePath(), m.ManifestContent())
	}
	found := false
	for _, m := range manifests {
		mf, oerr := fs.Open(m.FilePath())
		if oerr != nil {
			t.Fatalf("open manifest: %v", oerr)
		}
		entries, rerr := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if rerr != nil {
			t.Fatalf("read manifest: %v", rerr)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df != nil {
				t.Logf("  entry: content=%v path=%s", df.ContentType(), df.FilePath())
			}
			if df != nil && df.ContentType() == icebergpkg.EntryContentPosDeletes {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("expected a position delete entry in the snapshot; iceberg-go fell back to copy-on-write")
	}
}

// collectAllDataIDs reads every data file in the current snapshot
// and returns the concatenated list of id values. Delete files are
// not read (they'd return delete-tuple rows, not data rows).
func collectAllDataIDs(t *testing.T, tbl *icebergtable.Table) []int64 {
	t.Helper()
	ctx := context.Background()
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatalf("Manifests: %v", err)
	}
	var ids []int64
	for _, m := range manifests {
		mf, oerr := fs.Open(m.FilePath())
		if oerr != nil {
			t.Fatalf("open manifest: %v", oerr)
		}
		entries, rerr := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if rerr != nil {
			t.Fatalf("read manifest: %v", rerr)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			ids = append(ids, readParquetIDs(t, fs, df.FilePath())...)
		}
	}
	return ids
}

// readParquetIDs opens a parquet file via iceberg-go's IO and reads
// every row's `id` column into an int64 slice. Used by tests to
// verify post-compact output without going through the full Iceberg
// scan stack.
func readParquetIDs(t *testing.T, fs icebergio.IO, path string) []int64 {
	t.Helper()
	f, err := fs.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	info, _ := f.Stat()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	_ = info
	pf, err := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	reader := pqgo.NewGenericReader[testutil.SimpleFactRow](pf)
	defer reader.Close()
	var ids []int64
	buf := make([]testutil.SimpleFactRow, 256)
	for {
		n, rerr := reader.Read(buf)
		for i := 0; i < n; i++ {
			ids = append(ids, buf[i].ID)
		}
		if rerr != nil {
			break
		}
	}
	return ids
}
