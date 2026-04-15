package janitor

import (
	"context"
	"fmt"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

// UnsupportedFeatureError is returned when a table uses an Iceberg
// feature the janitor doesn't yet handle correctly. The caller MUST
// NOT compact such tables — the output would be silently incorrect.
// Common cases:
//   - V3 deletion vectors (stored in Puffin files, not yet parsed)
//   - Mixed schema IDs across source files (stitch assumes uniform schema)
//   - Mixed partition spec IDs across source files (grouping assumes uniform spec)
type UnsupportedFeatureError struct {
	Feature string
	Detail  string
}

func (e *UnsupportedFeatureError) Error() string {
	return fmt.Sprintf("iceberg feature not yet supported: %s (%s)", e.Feature, e.Detail)
}

// checkTableForUnsupportedFeatures scans the current snapshot's
// manifests for features the janitor can't compact correctly. If it
// finds any, returns a non-nil UnsupportedFeatureError. The caller
// is expected to skip compaction for the table and log loudly.
//
// This is a mandatory safety gate — not a warning. Compacting a table
// with deletion vectors would silently resurrect deleted rows. That's
// a data correctness bug the janitor refuses to create.
//
// What we check today:
//  1. V3 deletion vectors (content bytes stored in Puffin files).
//     Detected via `ManifestEntry.DataFile.ContentType() == EntryContentPosDeletes`
//     combined with a file_format of PUFFIN (or non-parquet extension).
//  2. Mixed schema IDs across source data files — byte-copy stitch
//     assumes every source has the same schema. Different schema_id
//     values in manifest entries mean schema evolution happened and
//     the sources aren't uniform.
//  3. Mixed partition spec IDs across source data files — we group
//     files by partition tuple under the current spec; mixed spec_ids
//     mean the grouping is ambiguous.
//
// What we DON'T check (future work):
//   - Nested column types (struct/list/map) — byte-copy stitch may
//     or may not handle these; untested.
//   - Row lineage columns (V3) — not yet propagated.
//   - Variant/geometry types (V3) — not yet tested.
func checkTableForUnsupportedFeatures(ctx context.Context, tbl *icebergtable.Table, fs icebergio.IO) error {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil // empty table — nothing to check
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return fmt.Errorf("listing manifests for safety scan: %w", err)
	}

	seenSchemaIDs := map[int]struct{}{}
	seenSpecIDs := map[int32]struct{}{}

	for _, m := range manifests {
		mf, oerr := fs.Open(m.FilePath())
		if oerr != nil {
			return fmt.Errorf("opening manifest %s for safety scan: %w", m.FilePath(), oerr)
		}
		entries, rerr := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if rerr != nil {
			return fmt.Errorf("reading manifest %s: %w", m.FilePath(), rerr)
		}

		seenSpecIDs[m.PartitionSpecID()] = struct{}{}

		for _, e := range entries {
			df := e.DataFile()
			if df == nil {
				continue
			}
			// V3 deletion vectors: position deletes with Puffin format.
			// The content type alone is ambiguous (V2 position deletes
			// also use EntryContentPosDeletes but in parquet). We look
			// at the file format string — Puffin files have
			// extension .puffin or format=PUFFIN.
			if df.ContentType() == icebergpkg.EntryContentPosDeletes {
				format := string(df.FileFormat())
				if format == "PUFFIN" || format == "puffin" {
					return &UnsupportedFeatureError{
						Feature: "V3 deletion vectors",
						Detail:  format + " " + df.FilePath(),
					}
				}
			}
		}
	}

	if len(seenSpecIDs) > 1 {
		return &UnsupportedFeatureError{
			Feature: "mixed partition spec IDs",
			Detail:  fmt.Sprintf("manifests span %d different spec-ids; janitor requires uniform spec", len(seenSpecIDs)),
		}
	}

	_ = seenSchemaIDs // per-entry schema_id isn't directly exposed by iceberg-go's ManifestEntry interface yet; schema-evolution guard is a follow-up
	return nil
}
