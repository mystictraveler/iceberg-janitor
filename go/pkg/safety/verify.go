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
// id), I7 (manifest reference existence). I3-I6, I8, I9 land as the
// relevant maintenance ops do (sort, V3 row lineage, manifest rewrite).
type Verification struct {
	SchemeVersion  int    `json:"scheme_version"`
	CheckedAt      string `json:"checked_at"`
	CheckerVersion string `json:"checker_version"`

	InputSnapshotID  int64 `json:"input_snapshot_id"`
	OutputSnapshotID int64 `json:"output_snapshot_id"`

	I1RowCount        RowCountCheck         `json:"I1_row_count"`
	I2Schema          SchemaCheck           `json:"I2_schema"`
	I7ManifestRefs    ManifestRefsCheck     `json:"I7_manifest_refs"`
	// I3..I6, I8, I9 will be added by subsequent iterations.

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

	beforeRows, err := totalDataRows(ctx, before, props)
	if err != nil {
		return v, fmt.Errorf("counting rows in pre-compaction snapshot: %w", err)
	}
	stagedRows, err := totalDataRows(ctx, staged.Table, props)
	if err != nil {
		return v, fmt.Errorf("counting rows in staged snapshot: %w", err)
	}

	v.I1RowCount = RowCountCheck{
		In:  beforeRows,
		DVs: 0, // MVP: no deletion vectors yet
		Out: stagedRows,
	}
	if beforeRows != stagedRows {
		v.I1RowCount.Result = "fail"
		v.I1RowCount.Reason = fmt.Sprintf("input rows %d != staged rows %d", beforeRows, stagedRows)
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

// totalDataRows walks the current snapshot's manifests and sums the row
// counts of every data file. Delete files are ignored — MVP scope.
func totalDataRows(ctx context.Context, tbl *icebergtable.Table, props map[string]string) (int64, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		// Some staged tables may not yet have a usable FS factory; fall
		// back to opening one from the metadata location and the props.
		fs, err = openFS(ctx, tbl.MetadataLocation(), props)
		if err != nil {
			return 0, fmt.Errorf("opening table FS: %w", err)
		}
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, fmt.Errorf("listing manifests: %w", err)
	}
	var total int64
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
			total += df.Count()
		}
	}
	return total, nil
}

func openFS(ctx context.Context, location string, props map[string]string) (icebergio.IO, error) {
	return icebergio.LoadFS(ctx, props, location)
}

func nowRFC3339() string {
	return timeNow().Format("2006-01-02T15:04:05Z07:00")
}
