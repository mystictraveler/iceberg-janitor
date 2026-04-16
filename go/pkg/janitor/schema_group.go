package janitor

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
	pqgo "github.com/parquet-go/parquet-go"
	"golang.org/x/sync/errgroup"
)

// Schema-evolution guard.
//
// The byte-copy stitch assumes every source file in a compaction round
// has the same parquet schema (same column set, same field IDs). When
// a user evolves the Iceberg schema (adds a nullable column, drops an
// old one), new files are written at schema_id = N+1 and old files
// stay at schema_id = N. Mixing the two in one byte-copy stitch would
// silently corrupt the output (wrong column cardinality, missing or
// extra chunks).
//
// # Why "skip" is the right policy, not "rewrite across the boundary"
//
// Schema evolutions on a real Iceberg table are RARE events — days or
// weeks apart, not minutes. Streaming workloads (the janitor's hot
// path) typically run against a stable schema for the lifetime of the
// streamer; evolutions happen out-of-band via a deliberate DDL
// change. Compaction, in contrast, runs continuously — every few
// seconds on a hot table. The rate ratio is roughly 10^5 or larger:
// compaction cycles per schema change.
//
// Given that asymmetry, the architecturally simple thing is to make
// the compactor respect the boundary and let time heal:
//
//   - Skip any compaction round whose source set spans schemas. No
//     work, no risk of silent corruption, no coupling between
//     maintenance and DDL.
//   - As the writer produces more files at the new schema, the
//     schema-N+1 tail naturally grows. The next compaction round's
//     source selection window will eventually contain only N+1 files,
//     at which point it fires normally.
//   - Schema-N files left over age out through Expire (old snapshots
//     fall off the retain set, their unique data-file references
//     drop) and OrphanFiles (the recycle bin sweeps unreferenced
//     paths). Within one or two expire cycles the old schema is gone
//     from the active table entirely.
//
// The alternative — "rewrite across the boundary" — means decoding
// schema-N files and re-encoding them at schema N+1. That's not
// compaction anymore; it's a schema rewrite, a fundamentally
// different operation with different correctness risks (added column
// values become NULL in the output, dropped column values are lost
// irretrievably, type-widened values may overflow). It belongs in a
// separate `rewrite-schema` op if a user ever needs it, NOT silently
// bundled into the compactor.
//
// We handle this by refusing to cross schema-change boundaries: when
// a compaction round's source set contains files at more than one
// schema_id, the round is SKIPPED (not errored). The caller records
// SkippedReason="mixed_schemas" in the CompactResult and moves on.
//
// # How we detect the boundary
//
// iceberg-go v0.5.0 does not expose per-entry schema_id on the public
// DataFile interface (the spec records it in the manifest entry but
// iceberg-go's Go struct doesn't surface the field), and the
// iceberg-go writer does not stamp "iceberg.schema.id" in parquet
// key-value metadata either. So we derive a per-file "schema group"
// from the parquet footer:
//
//   - If the "iceberg.schema.id" KV key IS present (Spark, some
//     catalogs stamp it), we parse it and use the int directly.
//   - Otherwise we compute a field-ID + physical-type signature from
//     the parquet schema: SHA-256 over the sorted (field-id, type-
//     code) pairs for every leaf column. Two files with the same
//     signature have the same parquet chunk layout, which is the
//     property that matters for byte-copy stitch.
//
// The signature catches the four schema changes that matter for
// compaction:
//
//   - Add column     → new field-id appears      → signature differs
//   - Drop column    → field-id disappears       → signature differs
//   - Widen type     → same field-id, new code   → signature differs
//   - Rename column  → field-id unchanged        → signature matches
//     (benign — byte-copy is name-agnostic once field-IDs line up)
//
// Sign bit on the signature path: the returned schemaGroupKey is
// negated so it cannot collide with a legitimate non-negative
// schema-id from the direct-parse path, preventing cross-mechanism
// collisions.

// icebergSchemaIDKey is the parquet key-value metadata key iceberg-go
// stamps on every file it writes for an Iceberg table. Value is the
// decimal string of the schema-id at write time.
const icebergSchemaIDKey = "iceberg.schema.id"

// schemaGroupKey is the per-file grouping identifier used by
// groupPathsBySchemaID. When the iceberg.schema.id metadata key is
// present, this is the parsed int. When absent, it's a 64-bit FNV-
// style hash of the sorted top-level field-ID list, negated so it
// can't collide with a real non-negative schema-id.
type schemaGroupKey int64

// readSchemaGroup returns the schema group for one parquet file.
// Cost: one parquet footer read (cheap — bounded range GET at end of
// file). Callers should batch via groupPathsBySchemaID to parallelize.
func readSchemaGroup(ctx context.Context, fs icebergio.IO, path string) (schemaGroupKey, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", path, err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		return 0, fmt.Errorf("parse parquet %s: %w", path, err)
	}
	// Preferred signal: iceberg.schema.id in key-value metadata.
	for _, kv := range pf.Metadata().KeyValueMetadata {
		if kv.Key == icebergSchemaIDKey && kv.Value != "" {
			id, perr := strconv.Atoi(kv.Value)
			if perr == nil {
				return schemaGroupKey(id), nil
			}
			// If the key is present but garbage, fall through to
			// the signature path rather than error — the signature
			// is still correct for grouping.
		}
	}
	return fieldIDSignature(pf), nil
}

// fieldIDSignature produces a stable negative int64 hash of the sorted
// top-level field-ID list of a parquet file. Used when the iceberg
// schema-id metadata key is absent so we can still group files by
// their column content. The sign bit guarantees it can't collide with
// a legitimate schema-id (which is always non-negative).
func fieldIDSignature(pf *pqgo.File) schemaGroupKey {
	schema := pf.Schema()
	// parquet-go's Schema.Columns() returns column PATHS as [][]string.
	// For iceberg-written parquet, top-level field IDs are stored on
	// each SchemaElement's FieldID. We read them straight from the
	// FileMetaData.Schema flat list.
	// Collect (field-id, physical-type) pairs for every leaf column.
	// Including the type catches widening evolutions (int32 → int64,
	// float32 → float64, decimal precision/scale changes) which keep
	// the same field-id set but change the on-disk chunk width —
	// byte-copy would produce wrong-width chunks in the output.
	type fieldEntry struct {
		id       int32
		typeCode int32
	}
	var entries []fieldEntry
	for _, se := range pf.Metadata().Schema {
		// FieldID is optional in the Thrift definition; 0 is the
		// zero-value and iceberg-go populates a real positive ID on
		// every leaf column. Skipping 0 keeps the signature stable
		// across parquet-go versions that differ on how the root
		// element's FieldID is marshalled.
		if se.FieldID != 0 {
			var tc int32 = -1 // "no type" distinct from any parquet-go Type value
			if se.Type.Valid {
				tc = int32(se.Type.V)
			}
			entries = append(entries, fieldEntry{
				id:       se.FieldID,
				typeCode: tc,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })
	h := sha256.New()
	buf := make([]byte, 8)
	for _, e := range entries {
		binary.LittleEndian.PutUint32(buf[:4], uint32(e.id))
		binary.LittleEndian.PutUint32(buf[4:], uint32(e.typeCode))
		h.Write(buf)
	}
	_ = schema // silence unused — kept in the signature for documentation clarity
	sum := h.Sum(nil)
	// Take the low 63 bits and negate so it can't collide with a real
	// schema-id (which is always non-negative).
	n := int64(binary.LittleEndian.Uint64(sum[:8])) & 0x7FFFFFFFFFFFFFFF
	return schemaGroupKey(-1 - n)
}

// groupPathsBySchemaID reads each path's parquet footer in parallel
// and returns a map keyed by schema group. A compaction round is safe
// to run only when the returned map has exactly one entry.
//
// The bounded concurrency matches the stitch file-open loop —
// footer reads dominate on high-latency S3; 32 workers amortize.
func groupPathsBySchemaID(ctx context.Context, fs icebergio.IO, paths []string) (map[schemaGroupKey][]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	keys := make([]schemaGroupKey, len(paths))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parquetReadConcurrency)
	for i, p := range paths {
		i, p := i, p
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			k, err := readSchemaGroup(gctx, fs, p)
			if err != nil {
				return err
			}
			keys[i] = k
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	groups := make(map[schemaGroupKey][]string, 2)
	for i, p := range paths {
		groups[keys[i]] = append(groups[keys[i]], p)
	}
	return groups, nil
}

// describeSchemaGroups formats a stable, short description of the
// groups for logging/metrics. "schema=1 files=45 | schema=2 files=7".
// Called only on the mixed-schemas skip path, so cost is irrelevant.
func describeSchemaGroups(groups map[schemaGroupKey][]string) string {
	type kv struct {
		k schemaGroupKey
		n int
	}
	var rows []kv
	for k, v := range groups {
		rows = append(rows, kv{k, len(v)})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].k < rows[j].k })
	var b []byte
	for i, r := range rows {
		if i > 0 {
			b = append(b, " | "...)
		}
		if r.k < 0 {
			// signature fallback
			b = append(b, []byte(fmt.Sprintf("fid-sig=%x files=%d", uint64(-r.k-1), r.n))...)
		} else {
			b = append(b, []byte(fmt.Sprintf("schema=%d files=%d", int64(r.k), r.n))...)
		}
	}
	return string(b)
}

var _ = sync.Once{} // reserved for future memoization
