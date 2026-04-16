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
	"golang.org/x/sync/errgroup"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
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

	// SnapshotRetain is populated only by VerifyExpireConsistency.
	// It's a separate field from the I-numbered invariants because
	// "every snapshot in the retain set survives the stage" is an
	// expire-only check that has no analogue in compact.
	SnapshotRetain SnapshotRetainCheck `json:"snapshot_retain,omitempty"`

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
// VerifyOption tunes the consistency check.
type VerifyOption func(*verifyCfg)

// WithDeletedRows tells the master check that the caller legitimately
// dropped this many rows during compaction (e.g. V2 position or
// equality deletes applied in the decode/encode pass). I1 uses this
// to allow a controlled row-count reduction: input - deletedRows
// must equal staged. Omitted = 0 = standard row-conservation check.
func WithDeletedRows(n int64) VerifyOption {
	return func(c *verifyCfg) { c.deletedRows = n }
}

type verifyCfg struct {
	deletedRows int64
}

func VerifyCompactionConsistency(ctx context.Context, before *icebergtable.Table, staged *icebergtable.StagedTable, props map[string]string, opts ...VerifyOption) (*Verification, error) {
	tr := observe.Tracer("janitor.safety")
	ctx, span := tr.Start(ctx, "VerifyCompactionConsistency")
	defer span.End()

	cfg := verifyCfg{}
	for _, o := range opts {
		o(&cfg)
	}
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

	// I1: total row count. The invariant is
	//   beforeData - droppedByDeletes == stagedData
	// where droppedByDeletes is the count the janitor passed in via
	// WithDeletedRows (0 for plain compaction; >0 for V2 merge-on-
	// read cases where the decode/encode pass applied pos/eq
	// deletes). This formulation is independent of whether the
	// (orphaned) delete files are still in the snapshot, which we
	// can't cheaply tell from manifest metadata alone.
	expectedStaged := beforeAgg.totalRows - cfg.deletedRows
	v.I1RowCount = RowCountCheck{
		In:  beforeAgg.totalRows,
		DVs: cfg.deletedRows,
		Out: stagedAgg.totalRows,
	}
	if expectedStaged != stagedAgg.totalRows {
		v.I1RowCount.Result = "fail"
		v.I1RowCount.Reason = fmt.Sprintf("input rows %d - deleted %d = %d != staged rows %d",
			beforeAgg.totalRows, cfg.deletedRows, expectedStaged, stagedAgg.totalRows)
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
	// files MINUS the deleted-row hint must equal the sum across all
	// staged data files. The minus term is non-zero only for
	// merge-on-read V2 paths where the caller told us it dropped N
	// rows during the decode/encode pass. Null counts (I4) can't use
	// the same arithmetic — we don't know how many of the deleted
	// rows had a NULL in each column — so I4 is skipped (treated as
	// info-only "checked=0") when deletedRows > 0.
	v.I3ValueCounts = compareColumnCountsWithOffset(beforeAgg.valueCounts, stagedAgg.valueCounts, cfg.deletedRows)
	if v.I3ValueCounts.Result == "fail" {
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I3 value counts): %s", v.I3ValueCounts.Reason)
	}

	// I4: per-column null count. Skip entirely when deletedRows > 0;
	// we can't compute an exact expected delta per column without
	// touching the parquet payload.
	if cfg.deletedRows > 0 {
		v.I4NullCounts = ColumnCountsCheck{Result: "skip", Reason: "null counts not reconcilable with applied deletes"}
	} else {
		v.I4NullCounts = compareColumnCounts(beforeAgg.nullCounts, stagedAgg.nullCounts)
		if v.I4NullCounts.Result == "fail" {
			v.Overall = "fail"
			return v, fmt.Errorf("MASTER CHECK FAILED (I4 null counts): %s", v.I4NullCounts.Reason)
		}
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

	// I7: manifest reference existence — but only for files this op
	// just wrote. Every data file referenced by the staged snapshot
	// that is NOT also referenced by the input snapshot is a "new"
	// file: it was either created by this transaction or pre-existed
	// out of band. Either way it's the only set of files whose
	// existence the master check has any business verifying — every
	// other file in the staged snapshot was already present in the
	// input snapshot, which is the known-good base of the operation.
	//
	// This is the fix for github.com/mystictraveler/iceberg-janitor#5.
	// The previous implementation walked every data file in the WHOLE
	// staged table and HEAD'd each one, which is O(table_size) S3
	// round-trips per master check call — fine for fileblob, but on
	// MinIO/S3 it burns 20-60 seconds per compact attempt and the
	// writer always wins the retry race against a hot table. The
	// narrowed check is O(files_added_by_this_op), which for the
	// parquet-go-direct compaction path is exactly 1.
	newPaths := make([]string, 0)
	for p := range stagedAgg.filePaths {
		if _, present := beforeAgg.filePaths[p]; !present {
			newPaths = append(newPaths, p)
		}
	}
	checked, passed, err := verifyDataFilesExist(ctx, staged.Table, props, newPaths)
	v.I7ManifestRefs = ManifestRefsCheck{Checked: checked, Passed: passed}
	if err != nil {
		v.I7ManifestRefs.Result = "fail"
		v.I7ManifestRefs.Reason = err.Error()
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I7 manifest refs): %s", v.I7ManifestRefs.Reason)
	}
	v.I7ManifestRefs.Result = "pass"

	span.SetAttributes(
		observe.Result("pass"),
		observe.Rows(v.I1RowCount.Out),
	)
	v.Overall = "pass"
	return v, nil
}

// verifyDataFilesExist HEAD-checks each path in `paths` against the
// table's filesystem. Returns (checked, passed, err). On success,
// checked == passed == len(paths). Empty input is fine — returns
// (0, 0, nil).
//
// This is I7's actual unit of work: it answers "do the files this
// transaction wrote actually exist in object storage?" The caller
// (VerifyCompactionConsistency) is responsible for computing which
// files those are; this function does not enumerate the table or
// walk any manifests. That's the fix for issue #5: the previous
// verifyManifestReferences walked every manifest in the staged
// snapshot and opened every data file, which on S3 was O(table_size)
// HEAD round-trips per compact attempt.
func verifyDataFilesExist(ctx context.Context, tbl *icebergtable.Table, props map[string]string, paths []string) (int, int, error) {
	if len(paths) == 0 {
		return 0, 0, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		fs, err = openFS(ctx, tbl.MetadataLocation(), props)
		if err != nil {
			return 0, 0, fmt.Errorf("opening table FS: %w", err)
		}
	}
	checked := 0
	passed := 0
	for _, p := range paths {
		checked++
		fh, err := fs.Open(p)
		if err != nil {
			return checked, passed, fmt.Errorf("data file %s does not exist: %w", p, err)
		}
		fh.Close()
		passed++
	}
	return checked, passed, nil
}

// dataStats aggregates per-column counts and bounds presence across
// all data files in a snapshot. The maps are keyed by column id.
//
// filePaths is the set of every data file's absolute path in the
// snapshot. It is collected for free during the existing manifest
// walk (no extra I/O) and is used by VerifyCompactionConsistency to
// compute "files added by this op" via the set difference
// stagedPaths - beforePaths. That set is then the input to I7's
// HEAD-existence check, which previously walked the WHOLE staged
// table's manifests on every retry — see issue #5.
type dataStats struct {
	totalRows   int64
	valueCounts map[int]int64       // sum of DataFile.ValueCounts across all files
	nullCounts  map[int]int64       // sum of DataFile.NullValueCounts
	boundsCols  map[int]bool        // column ids that have bounds set in at least one file
	filePaths   map[string]struct{} // set of data file paths

	// V2 delete accounting. posDeletesApplicable is the sum of rows
	// in position delete files that reference a data file currently
	// in the snapshot's data-file set (i.e. non-orphaned position
	// deletes). eqDeletesApplicable is the rough sum of rows in
	// equality delete files that could target data files with lower
	// sequence number — it is NOT a tight bound, because we cannot
	// evaluate the predicate without opening the delete file, but
	// it's a safe upper bound on the number of rows that could be
	// masked. Both counters are used by I1 to compute the logical
	// (visible) row count = totalRows - applicable deletes.
	posDeletesApplicable int64
}

// posDeleteRef is a lightweight record captured during the walk for
// later applicability analysis. We can't decide applicability in the
// parallel worker because the full data-file set isn't known until
// every manifest is read.
type posDeleteRef struct {
	rowCount   int64
	referenced string
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
		filePaths:   map[string]struct{}{},
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

	// Bounded-concurrency manifest read. The master check runs twice
	// per compact attempt (once for `before`, once for `staged`); on
	// a hot streaming table with hundreds of manifests, sequential
	// reads dominate the per-attempt latency and the compact loses
	// the writer-fight CAS race. Each worker reads one manifest into
	// a per-manifest local accumulator; the merge into `out` runs
	// single-threaded after all workers complete, so no map locks
	// are needed during the parallel phase. See issue #6.
	type manifestAgg struct {
		totalRows   int64
		valueCounts map[int]int64
		nullCounts  map[int]int64
		boundsCols  map[int]bool
		filePaths   []string
		posDeletes  []posDeleteRef
	}
	results := make([]manifestAgg, len(manifests))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(manifestReadConcurrency)
	for i, m := range manifests {
		i, m := i, m
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			mf, err := fs.Open(m.FilePath())
			if err != nil {
				return fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
			}
			entries, err := icebergpkg.ReadManifest(m, mf, true)
			mf.Close()
			if err != nil {
				return fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
			}
			local := manifestAgg{
				valueCounts: map[int]int64{},
				nullCounts:  map[int]int64{},
				boundsCols:  map[int]bool{},
			}
			for _, e := range entries {
				df := e.DataFile()
				if df == nil {
					continue
				}
				switch df.ContentType() {
				case icebergpkg.EntryContentPosDeletes:
					// V2 position delete file. Remember the row count
					// and the referenced data file path; applicability
					// (does the referenced data file still live in
					// the snapshot?) is computed after the walk.
					ref := ""
					if rd := df.ReferencedDataFile(); rd != nil {
						ref = *rd
					}
					local.posDeletes = append(local.posDeletes, posDeleteRef{
						rowCount:   df.Count(),
						referenced: ref,
					})
					continue
				case icebergpkg.EntryContentEqDeletes:
					// Equality deletes can't be reconciled against
					// the row count without opening the delete file
					// and evaluating the predicate; we rely on the
					// janitor's per-attempt dropped-row accumulator
					// and the I1 check's `DVs` field (set by the
					// caller) to cover eq deletes. Skip for now.
					continue
				case icebergpkg.EntryContentData:
					// fall through
				default:
					continue
				}
				local.totalRows += df.Count()
				local.filePaths = append(local.filePaths, df.FilePath())
				for col, n := range df.ValueCounts() {
					local.valueCounts[col] += n
				}
				for col, n := range df.NullValueCounts() {
					local.nullCounts[col] += n
				}
				for col := range df.LowerBoundValues() {
					local.boundsCols[col] = true
				}
				for col := range df.UpperBoundValues() {
					local.boundsCols[col] = true
				}
			}
			results[i] = local
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, r := range results {
		out.totalRows += r.totalRows
		for col, n := range r.valueCounts {
			out.valueCounts[col] += n
		}
		for col, n := range r.nullCounts {
			out.nullCounts[col] += n
		}
		for col := range r.boundsCols {
			out.boundsCols[col] = true
		}
		for _, p := range r.filePaths {
			out.filePaths[p] = struct{}{}
		}
	}
	// Second pass: decide which position deletes are "applicable" —
	// i.e. reference a data file still in the snapshot. Orphaned
	// position deletes (whose referenced data file was removed by a
	// prior ReplaceDataFiles commit) don't contribute to the logical
	// row count because no reader will ever see the rows they mark.
	//
	// V2.1 position deletes always record the referenced data file
	// path in the manifest entry; V2.0 may not, in which case we
	// assume applicable — safer overdetection than silent mismatch.
	for _, r := range results {
		for _, pd := range r.posDeletes {
			if pd.referenced == "" {
				out.posDeletesApplicable += pd.rowCount
				continue
			}
			if _, ok := out.filePaths[pd.referenced]; ok {
				out.posDeletesApplicable += pd.rowCount
			}
		}
	}
	return out, nil
}

// manifestReadConcurrency is the bounded-fan-out limit for parallel
// manifest avro reads in aggregateDataStats. Same rationale as the
// constant of the same name in pkg/janitor/compact_replace.go: 32 is
// a safe HTTP/2-friendly default that gets the per-table walk wall
// time down from O(N×L) to ~max(L, ⌈N/32⌉×L) without exhausting
// connection pool or file descriptors. See issue #6.
const manifestReadConcurrency = 32

// compareColumnCounts is the I3/I4 worker: it compares two per-column
// sum maps and returns a structured ColumnCountsCheck. The check is
// strict equality on every column id present in either map.
// compareColumnCountsWithOffset is the same as compareColumnCounts
// but allows `in - offset == out` to pass. Used by I3 when the caller
// legitimately dropped rows via V2 deletes: every non-null column
// value lost a contribution equal to the deleted row count.
func compareColumnCountsWithOffset(in, out map[int]int64, offset int64) ColumnCountsCheck {
	if offset == 0 {
		return compareColumnCounts(in, out)
	}
	result := ColumnCountsCheck{}
	cols := unionKeys(in, out)
	result.Checked = len(cols)
	for _, col := range cols {
		// A column's value count can drop by AT MOST `offset` (when
		// every deleted row had a non-null value for this column).
		// It can also drop by less (when some deleted rows had NULL
		// for this column — those didn't contribute to value count).
		// So the bound is: out[col] in [in[col] - offset, in[col]].
		delta := in[col] - out[col]
		if delta >= 0 && delta <= offset {
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
	result.Reason = fmt.Sprintf("%d/%d columns mismatched under offset=%d (column ids: %v)",
		len(result.FailedColumns), result.Checked, offset, result.FailedColumns)
	return result
}

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

// SnapshotRetainCheck reports the result of the expire-specific
// invariant: every snapshot in the operator-supplied retain set must
// still appear in the staged metadata's snapshot list, and the staged
// list must be a subset of the input list (an expire never adds new
// snapshots).
type SnapshotRetainCheck struct {
	BeforeSnapshots int     `json:"before_snapshots"`
	AfterSnapshots  int     `json:"after_snapshots"`
	Removed         int     `json:"removed"`
	Added           []int64 `json:"added,omitempty"` // populated only on failure
	Result          string  `json:"result"`
	Reason          string  `json:"reason,omitempty"`
}

// VerifyExpireConsistency runs the master check against a pending
// snapshot expiration. The invariants enforced here are different from
// VerifyCompactionConsistency:
//
//   - I1 (row count): the CURRENT snapshot's total rows must not
//     change. Expire only touches the parent chain; the branch tip
//     stays put.
//   - I2 (schema by id): must not change.
//   - Current snapshot id: must not change. Expire is forbidden from
//     reseating the main branch.
//   - Snapshot set monotonicity: stagedSnapshots ⊆ beforeSnapshots.
//     An expire that ADDS snapshots indicates a programming bug.
//
// I3/I4/I5 are skipped because the per-file column statistics don't
// move under expire (the same data files are still referenced by the
// surviving snapshots). I7 is skipped because expire writes no new
// data files; the post-commit orphan deletion in iceberg-go's
// removeSnapshotsUpdate.PostCommit is fire-and-forget AFTER the CAS
// has already succeeded, so any failure to delete a particular
// orphan is a separate (best-effort) error path that doesn't gate
// commit.
//
// The check is mandatory; no --force bypass.
func VerifyExpireConsistency(ctx context.Context, before *icebergtable.Table, staged *icebergtable.StagedTable, props map[string]string) (*Verification, error) {
	v := &Verification{
		SchemeVersion:  1,
		CheckedAt:      nowRFC3339(),
		CheckerVersion: "iceberg-janitor-go expire",
	}
	if before == nil || staged == nil {
		return v, fmt.Errorf("VerifyExpireConsistency: before or staged is nil")
	}

	beforeSnap := before.CurrentSnapshot()
	stagedSnap := staged.CurrentSnapshot()
	if beforeSnap != nil {
		v.InputSnapshotID = beforeSnap.SnapshotID
	}
	if stagedSnap != nil {
		v.OutputSnapshotID = stagedSnap.SnapshotID
	}

	// Current snapshot id must not change.
	if beforeSnap == nil || stagedSnap == nil || beforeSnap.SnapshotID != stagedSnap.SnapshotID {
		v.I1RowCount = RowCountCheck{Result: "fail",
			Reason: fmt.Sprintf("expire reseated the main branch: %d -> %d", v.InputSnapshotID, v.OutputSnapshotID)}
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (expire reseated main branch): %s", v.I1RowCount.Reason)
	}

	// I1: current-snapshot row count is invariant under expire. We
	// derive it from the snapshot's summary so we don't have to walk
	// the manifests twice. The summary's "total-records" is written
	// by every iceberg writer (including ours, since iceberg-go fills
	// it on append/replace) and we're comparing the SAME snapshot's
	// summary against itself reached two different ways — so the
	// numbers had better match. If summary is missing on either side
	// we fall back to a manifest walk via aggregateDataStats; that's
	// slower but always correct.
	beforeRows, err := snapshotTotalRows(ctx, before, beforeSnap, props)
	if err != nil {
		return v, fmt.Errorf("computing before rows: %w", err)
	}
	stagedRows, err := snapshotTotalRows(ctx, staged.Table, stagedSnap, props)
	if err != nil {
		return v, fmt.Errorf("computing staged rows: %w", err)
	}
	v.I1RowCount = RowCountCheck{In: beforeRows, Out: stagedRows}
	if beforeRows != stagedRows {
		v.I1RowCount.Result = "fail"
		v.I1RowCount.Reason = fmt.Sprintf("current-snapshot rows changed under expire: %d -> %d", beforeRows, stagedRows)
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I1 row count under expire): %s", v.I1RowCount.Reason)
	}
	v.I1RowCount.Result = "pass"

	// I2: schema id must not change.
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
		v.I2Schema.Reason = fmt.Sprintf("schema id changed under expire: %d -> %d", v.I2Schema.InID, v.I2Schema.OutID)
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (I2 schema): %s", v.I2Schema.Reason)
	}
	v.I2Schema.Result = "pass"

	// Snapshot set monotonicity: staged ⊆ before. Any "added" snapshot
	// id is a programming bug — expire is supposed to be a strict
	// removal op against the metadata.snapshots[] slice.
	beforeIDs := make(map[int64]struct{}, len(before.Metadata().Snapshots()))
	for _, s := range before.Metadata().Snapshots() {
		beforeIDs[s.SnapshotID] = struct{}{}
	}
	var added []int64
	for _, s := range staged.Metadata().Snapshots() {
		if _, present := beforeIDs[s.SnapshotID]; !present {
			added = append(added, s.SnapshotID)
		}
	}
	v.SnapshotRetain = SnapshotRetainCheck{
		BeforeSnapshots: len(before.Metadata().Snapshots()),
		AfterSnapshots:  len(staged.Metadata().Snapshots()),
	}
	v.SnapshotRetain.Removed = v.SnapshotRetain.BeforeSnapshots - v.SnapshotRetain.AfterSnapshots
	if len(added) > 0 {
		v.SnapshotRetain.Added = added
		v.SnapshotRetain.Result = "fail"
		v.SnapshotRetain.Reason = fmt.Sprintf("expire produced %d new snapshot id(s): %v", len(added), added)
		v.Overall = "fail"
		return v, fmt.Errorf("MASTER CHECK FAILED (snapshot retain): %s", v.SnapshotRetain.Reason)
	}
	v.SnapshotRetain.Result = "pass"

	v.Overall = "pass"
	return v, nil
}

// snapshotTotalRows returns the total row count of a single snapshot.
// It prefers the snapshot summary's "total-records" key (O(1), already
// in memory) and falls back to walking the manifest entries
// (O(manifest count) S3 round trips). The caller passes the snapshot
// it cares about explicitly so we don't accidentally read the wrong
// snapshot when the staged table's CurrentSnapshot has shifted.
func snapshotTotalRows(ctx context.Context, tbl *icebergtable.Table, snap *icebergtable.Snapshot, props map[string]string) (int64, error) {
	if snap == nil {
		return 0, nil
	}
	if snap.Summary != nil {
		if v, ok := snap.Summary.Properties["total-records"]; ok {
			var n int64
			if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
				return n, nil
			}
		}
	}
	// Fallback: walk manifests directly. This duplicates a sliver of
	// aggregateDataStats but skips the per-column maps because expire
	// only needs the row total. On a healthy table the summary path
	// always wins; this fallback exists for paranoia.
	fs, err := tbl.FS(ctx)
	if err != nil {
		fs, err = openFS(ctx, tbl.MetadataLocation(), props)
		if err != nil {
			return 0, fmt.Errorf("opening fs: %w", err)
		}
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, fmt.Errorf("listing manifests: %w", err)
	}
	var rows int64
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return 0, fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return 0, fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			rows += df.Count()
		}
	}
	return rows, nil
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
