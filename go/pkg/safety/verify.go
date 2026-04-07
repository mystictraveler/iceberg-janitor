// Package safety hosts the master pre-commit consistency check that every
// maintenance op must pass before pkg/catalog.AtomicCommit. It also owns the
// recycle bin (_janitor/recycle/<run_id>/), the orphan-removal trust horizon,
// and the tags-and-branches expiration guard. Verification failure aborts
// the commit, releases the lease, and emits a structured Verification record
// to the snapshot summary and _janitor/results/<run_id>.json.
//
// The master check is row-count + structural-equivalence verification across
// nine invariants (I1..I9). It is mandatory and non-bypassable; --force does
// not disable it.
package safety

import (
	"context"
	"fmt"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

// Verification is the structured result of running the master check on a
// pending commit. Implemented invariants: I1 (row count), I2 (schema by
// id), I3 (per-column value count), I4 (per-column null count), I5
// (per-column bounds presence + cardinality), I7 (manifest reference
// existence). I6 (V3 row lineage), I8 (manifest set equality), and I9
// (content hash) land alongside the maintenance ops that need them.
type Verification struct {
	SchemeVersion  int    `json:"scheme_version"`
	CheckedAt      string `json:"checked_at"`
	CheckerVersion string `json:"checker_version"`

	InputSnapshotID  int64 `json:"input_snapshot_id"`
	OutputSnapshotID int64 `json:"output_snapshot_id"`

	I1RowCount     RowCountCheck     `json:"I1_row_count"`
	I2Schema       SchemaCheck       `json:"I2_schema"`
	I3ValueCounts  ColumnCountsCheck `json:"I3_value_counts"`
	I4NullCounts   ColumnCountsCheck `json:"I4_null_counts"`
	I5Bounds       BoundsCheck       `json:"I5_bounds"`
	I7ManifestRefs ManifestRefsCheck `json:"I7_manifest_refs"`
	// I6, I8, I9 will be added by subsequent iterations.

	Overall string `json:"overall"` // "pass" | "fail"
}

type RowCountCheck struct {
	In     int64  `json:"in"`
	DVs    int64  `json:"dvs"`
	Out    int64  `json:"out"`
	Result string `json:"result"` // "pass" | "fail"
	Reason string `json:"reason,omitempty"`
}

type SchemaCheck struct {
	InID   int    `json:"in_id"`
	OutID  int    `json:"out_id"`
	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`
}

// ColumnCountsCheck reports the result of summing a per-column count
// (value count for I3, null count for I4) across all input data files
// vs all staged data files. `Checked` is the number of columns
// compared, `Passed` is the number that matched. On failure, the
// `FailedColumns` slice lists the column IDs whose totals diverged.
type ColumnCountsCheck struct {
	Checked       int    `json:"checked"`
	Passed        int    `json:"passed"`
	FailedColumns []int  `json:"failed_columns,omitempty"`
	Result        string `json:"result"`
	Reason        string `json:"reason,omitempty"`
}

// BoundsCheck reports the result of the I5 column bounds presence
// check. For the MVP, we verify that the SET of column IDs with
// bounds present is identical between input and staged — the staged
// snapshot must have bounds for every column the input had bounds
// for, and not have spurious bounds for columns the input didn't.
// The full byte-level (output ⊆ input) comparison lands alongside
// sort / zorder compaction, which needs schema-typed decoding.
type BoundsCheck struct {
	InColumns      int    `json:"in_columns"`
	OutColumns     int    `json:"out_columns"`
	MissingInOut   []int  `json:"missing_in_out,omitempty"`
	SpuriousInOut  []int  `json:"spurious_in_out,omitempty"`
	Result         string `json:"result"`
	Reason         string `json:"reason,omitempty"`
}

type ManifestRefsCheck struct {
	Checked int    `json:"checked"`
	Passed  int    `json:"passed"`
	Result  string `json:"result"`
	Reason  string `json:"reason,omitempty"`
}

// VerifyCompactionConsistency runs the master check against a pending
// compaction. `before` is the table immediately prior to the maintenance
// op; `staged` is the staged Table the in-flight transaction would commit
// if Commit were called now. The function reads both snapshots' manifest
// rows and asserts row-count conservation.
//
// MVP scope: only I1 (row count). The function is mandatory and the
// caller MUST refuse to commit on error. There is no --force bypass.
func VerifyCompactionConsistency(ctx context.Context, before *icebergtable.Table, staged *icebergtable.StagedTable, props map[string]string) (*Verification, error) {
	v := &Verification{
		SchemeVersion:  1,
		CheckedAt:      nowRFC3339(),
		CheckerVersion: "iceberg-janitor-go MVP",
	}
	if before == nil || staged == nil {
		return v, fmt.Errorf("VerifyCompactionConsistency: before or staged is nil")
	}

	beforeSnap := before.CurrentSnapshot()
	if beforeSnap != nil {
		v.InputSnapshotID = beforeSnap.SnapshotID
	}
	stagedSnap := staged.CurrentSnapshot()
	if stagedSnap != nil {
		v.OutputSnapshotID = stagedSnap.SnapshotID
	}

	// Aggregate per-file stats for both before and staged in one walk
	// each. We need rows (I1) plus per-column value counts, null
	// counts, and bounds presence (I3, I4, I5).
	beforeAgg, err := aggregateDataStats(ctx, before, props)
	if err != nil {
		return v, fmt.Errorf("aggregating pre-compaction stats: %w", err)
	}
	stagedAgg, err := aggregateDataStats(ctx, staged.Table, props)
	if err != nil {
		return v, fmt.Errorf("aggregating staged stats: %w", err)
	}

	// I1: total row count.
	v.I1RowCount = RowCountCheck{
		In:  beforeAgg.totalRows,
		DVs: 0, // MVP: no deletion vectors yet
		Out: stagedAgg.totalRows,
	}
	if beforeAgg.totalRows != stagedAgg.totalRows {
		v.I1RowCount.Result = "fail"
		v.I1RowCount.Reason = fmt.Sprintf("input rows %d != staged rows %d", beforeAgg.totalRows, stagedAgg.totalRows)
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I1 row count): %s", v.I1RowCount.Reason)
	}
	v.I1RowCount.Result = "pass"

	// I2: schema identity. The staged schema must equal the input
	// schema by current-schema-id. A mismatch means a maintenance op
	// silently changed the table's schema, which is not allowed
	// without an explicit schema-evolution path.
	beforeSchema := before.Metadata().CurrentSchema()
	stagedSchema := staged.Metadata().CurrentSchema()
	v.I2Schema = SchemaCheck{}
	if beforeSchema != nil {
		v.I2Schema.InID = beforeSchema.ID
	}
	if stagedSchema != nil {
		v.I2Schema.OutID = stagedSchema.ID
	}
	if beforeSchema == nil || stagedSchema == nil || beforeSchema.ID != stagedSchema.ID {
		v.I2Schema.Result = "fail"
		v.I2Schema.Reason = fmt.Sprintf("schema id changed from %d to %d", v.I2Schema.InID, v.I2Schema.OutID)
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I2 schema): %s", v.I2Schema.Reason)
	}
	v.I2Schema.Result = "pass"

	// I3: per-column value count. For each column id present in the
	// input data files, the sum of value counts across all input data
	// files must equal the sum across all staged data files.
	v.I3ValueCounts = compareColumnCounts(beforeAgg.valueCounts, stagedAgg.valueCounts)
	if v.I3ValueCounts.Result == "fail" {
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I3 value counts): %s", v.I3ValueCounts.Reason)
	}

	// I4: per-column null count. Same shape as I3.
	v.I4NullCounts = compareColumnCounts(beforeAgg.nullCounts, stagedAgg.nullCounts)
	if v.I4NullCounts.Result == "fail" {
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I4 null counts): %s", v.I4NullCounts.Reason)
	}

	// I5: column bounds presence. Every column with bounds in the
	// input must also have bounds in the staged output. The full
	// byte-level (output ⊆ input) check requires schema-typed decoding
	// and lands alongside sort/zorder compaction.
	v.I5Bounds = compareBoundsPresence(beforeAgg.boundsCols, stagedAgg.boundsCols)
	if v.I5Bounds.Result == "fail" {
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I5 bounds): %s", v.I5Bounds.Reason)
	}

	// I7: manifest reference existence. Every data file referenced by
	// the staged snapshot's manifests must actually exist in object
	// storage. This catches the case where a transaction wrote a
	// manifest entry but the underlying data file write failed (or
	// was lost). Cheap: N HEAD calls, parallelizable.
	checked, passed, err := verifyManifestReferences(ctx, staged.Table, props)
	v.I7ManifestRefs = ManifestRefsCheck{Checked: checked, Passed: passed}
	if err != nil {
		v.I7ManifestRefs.Result = "fail"
		v.I7ManifestRefs.Reason = err.Error()
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I7 manifest refs): %s", v.I7ManifestRefs.Reason)
	}
	v.I7ManifestRefs.Result = "pass"

	v.Overall = "pass"
	return v, nil
}

// verifyManifestReferences walks every manifest in the table's current
// snapshot and HEAD-checks each referenced data file. Returns
// (checked, passed, err). On success, checked == passed.
func verifyManifestReferences(ctx context.Context, tbl *icebergtable.Table, props map[string]string) (int, int, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		fs, err = openFS(ctx, tbl.MetadataLocation(), props)
		if err != nil {
			return 0, 0, fmt.Errorf("opening table FS: %w", err)
		}
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, 0, fmt.Errorf("listing manifests: %w", err)
	}
	checked := 0
	passed := 0
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return checked, passed, fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return checked, passed, fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			checked++
			fpath := df.FilePath()
			df_fh, err := fs.Open(fpath)
			if err != nil {
				return checked, passed, fmt.Errorf("data file %s does not exist: %w", fpath, err)
			}
			df_fh.Close()
			passed++
		}
	}
	return checked, passed, nil
}

// dataStats aggregates per-column counts and bounds presence across
// all data files in a snapshot. The maps are keyed by column id.
type dataStats struct {
	totalRows   int64
	valueCounts map[int]int64 // sum of DataFile.ValueCounts across all files
	nullCounts  map[int]int64 // sum of DataFile.NullValueCounts
	boundsCols  map[int]bool  // column ids that have bounds set in at least one file
}

// aggregateDataStats walks the current snapshot's manifests, reads
// every data file's per-column statistics from the manifest entry, and
// returns the aggregate. Delete files are ignored — MVP scope. Cost is
// the same as totalDataRows used to be: it's a single walk over the
// manifest entries with no extra I/O.
func aggregateDataStats(ctx context.Context, tbl *icebergtable.Table, props map[string]string) (*dataStats, error) {
	out := &dataStats{
		valueCounts: map[int]int64{},
		nullCounts:  map[int]int64{},
		boundsCols:  map[int]bool{},
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return out, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		// Some staged tables may not yet have a usable FS factory; fall
		// back to opening one from the metadata location and the props.
		fs, err = openFS(ctx, tbl.MetadataLocation(), props)
		if err != nil {
			return nil, fmt.Errorf("opening table FS: %w", err)
		}
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("listing manifests: %w", err)
	}
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return nil, fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return nil, fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			out.totalRows += df.Count()
			for col, n := range df.ValueCounts() {
				out.valueCounts[col] += n
			}
			for col, n := range df.NullValueCounts() {
				out.nullCounts[col] += n
			}
			for col := range df.LowerBoundValues() {
				out.boundsCols[col] = true
			}
			for col := range df.UpperBoundValues() {
				out.boundsCols[col] = true
			}
		}
	}
	return out, nil
}

// compareColumnCounts is the I3/I4 worker: it compares two per-column
// sum maps and returns a structured ColumnCountsCheck. The check is
// strict equality on every column id present in either map.
func compareColumnCounts(in, out map[int]int64) ColumnCountsCheck {
	result := ColumnCountsCheck{}
	cols := unionKeys(in, out)
	result.Checked = len(cols)
	for _, col := range cols {
		if in[col] == out[col] {
			result.Passed++
		} else {
			result.FailedColumns = append(result.FailedColumns, col)
		}
	}
	if len(result.FailedColumns) == 0 {
		result.Result = "pass"
		return result
	}
	result.Result = "fail"
	result.Reason = fmt.Sprintf("%d/%d columns mismatched (column ids: %v)",
		len(result.FailedColumns), result.Checked, result.FailedColumns)
	return result
}

// compareBoundsPresence is the I5 worker (MVP scope: presence + set
// equality, not byte-level bounds intersection). The set of column
// ids that have bounds in the input must equal the set in the staged
// output, modulo columns that legitimately have no values (whose bounds
// can't be computed).
func compareBoundsPresence(in, out map[int]bool) BoundsCheck {
	result := BoundsCheck{
		InColumns:  len(in),
		OutColumns: len(out),
	}
	for col := range in {
		if !out[col] {
			result.MissingInOut = append(result.MissingInOut, col)
		}
	}
	for col := range out {
		if !in[col] {
			result.SpuriousInOut = append(result.SpuriousInOut, col)
		}
	}
	if len(result.MissingInOut) == 0 && len(result.SpuriousInOut) == 0 {
		result.Result = "pass"
		return result
	}
	result.Result = "fail"
	result.Reason = fmt.Sprintf("bounds set mismatch: missing_in_out=%v spurious_in_out=%v",
		result.MissingInOut, result.SpuriousInOut)
	return result
}

func unionKeys(a, b map[int]int64) []int {
	seen := map[int]struct{}{}
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]int, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

func openFS(ctx context.Context, location string, props map[string]string) (icebergio.IO, error) {
	return icebergio.LoadFS(ctx, props, location)
}

func nowRFC3339() string {
	return timeNow().Format("2006-01-02T15:04:05Z07:00")
}
