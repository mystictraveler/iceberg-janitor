package janitor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// IsRetryableConcurrencyError reports whether `err` represents an
// optimistic-concurrency conflict that the caller should respond to by
// reloading the table and retrying from scratch (rather than treating
// as a hard failure).
//
// There are TWO layers at which a janitor commit can lose a
// concurrent-write race, and both should drive a retry:
//
//  1. Our directory catalog's per-key conditional write fails with
//     catalog.ErrCASConflict because another writer committed
//     v(N+1).metadata.json before us. This is the case our retry loop
//     was originally designed for.
//
//  2. iceberg-go's Transaction.Commit calls Requirement.Validate
//     against the current metadata BEFORE our DirectoryCatalog.
//     CommitTable is even invoked. If the streamer commits between our
//     stage and our commit, the staged transaction's branch-snapshot
//     requirement no longer matches and validation fails with a
//     "requirement failed: branch main has changed: expected id X,
//     found Y" error (and several similar variants for other
//     requirement types — schema id changed, last assigned field id
//     changed, default spec id changed, etc).
//
// The iceberg-go errors are plain fmt.Errorf strings with no
// exported sentinel, so we string-match on the consistent
// "has changed" substring. All six requirement-validation failure
// messages in iceberg-go's table/requirements.go contain it (case
// varies, so we lowercase first). The match is intentionally narrow:
// we only retry on concurrency-conflict errors, NOT on other
// requirement failures like assertTableUuid mismatches (which would
// indicate a programming bug, not a race).
//
// This helper is the fix for github.com/mystictraveler/iceberg-janitor#1.
func IsRetryableConcurrencyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, catalog.ErrCASConflict) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "has changed") &&
		(strings.Contains(msg, "requirement failed") || strings.Contains(msg, "requirement validation failed")) {
		return true
	}
	return false
}

// CompactOptions configure a single Compact call.
type CompactOptions struct {
	// MaxAttempts caps the CAS-conflict retry loop. Default 15.
	MaxAttempts int
	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time, capped at MaxBackoff. Default 100ms.
	InitialBackoff time.Duration
	// MaxBackoff caps the per-attempt sleep so the exponential
	// doubling can't run away to multi-minute waits. Default 5s. The
	// streamer's commit window is sub-second on the bench, so 5s of
	// quiet is more than enough for the next attempt to find a
	// usable CAS slot. Without this cap, MaxAttempts=15 with 100ms
	// base produces a worst-case cumulative wait of ~3276s (~55 min)
	// — which under load shows up as a hung compactor that's just
	// patiently sleeping. The fix is one line; the bug surfaced in
	// the run-B 10-min bench when one CAS-loser hit attempt 12 and
	// sat in a 200-second select with no progress.
	MaxBackoff time.Duration

	// PartitionTuple, if non-nil, scopes the compaction to one
	// partition's worth of data files. The compactOnce implementation
	// (compact_replace.go) walks the snapshot manifest list directly,
	// looks up the column's iceberg field id from the table schema,
	// and matches against DataFile.Partition()[partition_field_id].
	//
	// Map key is the source schema column name (e.g. "ss_store_sk"
	// or "event_date"); value is the RAW string from the operator
	// (e.g. "5" or "2026-04-08" or "US"). compactOnce parses each
	// value as the corresponding iceberg type at runtime, after
	// loading the table — see buildPartitionLiterals in
	// compact_partition_types.go for the type-aware parser. The
	// CLI does NOT need to know the iceberg type to construct this
	// map; it just forwards the raw user intent.
	//
	// Supported partition column types: bool, int / long, float /
	// double, string, binary, fixed, uuid, date, time, timestamp,
	// timestamptz, timestamp_ns, timestamptz_ns. Decimal is
	// recognized but not yet parsed (see compact_partition_types.go).
	//
	// nil means "compact the whole table". On an unpartitioned
	// table, passing a non-nil PartitionTuple is a hard error — see
	// the guard at the top of compactOnce.
	//
	// This is the smaller-scope-per-compaction primitive for the
	// writer-fight pathology: instead of compacting the whole table
	// in one big atomic transaction (which races the streamer for
	// every file the streamer wrote since we started planning), we
	// compact one partition's worth of files in a smaller transaction
	// that only conflicts with the streamer's commits to that one
	// partition.
	PartitionTuple map[string]string

	// TargetFileSizeBytes is the "skip-already-large-files" threshold
	// per design plan decision #13 (stitching binpack as default
	// compaction). When non-zero, compactOnce skips every input data
	// file whose FileSizeBytes is >= this threshold. The skipped
	// files are not read, not rewritten, and not removed from the
	// snapshot — they survive the compact unchanged. Only the
	// small-file tail is read, merged, and rewritten as one new
	// (target-sized) output file.
	//
	// Why: without this threshold, every compaction round reads and
	// rewrites every file in the partition, which is O(table_size)
	// I/O per round. On a streaming table that's hot enough that
	// the total partition size grows faster than one compaction
	// round can drain it, the compactor permanently loses the
	// writer-fight CAS race. Run 9 of the TPC-DS streaming bench
	// surfaced this: store_sales partition compact took 222 s and
	// 12 retries on round 3 because Pattern A re-read 13499 files
	// to rewrite 270.
	//
	// Pattern B (this option) bounds compaction cost per round to
	// O(small-file tail), which is bounded by streaming_rate ×
	// maintenance_interval — independent of table size. The cost
	// per round becomes constant in steady state.
	//
	// 0 (default) disables the filter and preserves Pattern A
	// behavior — every matching file is read and rewritten. Tests
	// and the existing CLI default rely on the disabled behavior.
	// The bench harness and production deployments should set this
	// to a workload-appropriate value (typically 64-512 MB; the
	// bench uses 1 MB because the streamer's per-commit files are
	// only ~5-20 KB and a 1 MB threshold lets each round leave the
	// previous round's output untouched).
	TargetFileSizeBytes int64

	// CircuitBreaker, if non-nil, is consulted before and after the
	// compaction. Pre-compaction it gates entry: if the table has a
	// pause file present, Compact returns *safety.PausedError without
	// touching the table at all. Post-compaction it records the
	// outcome (success resets the counter; failure increments it and
	// trips the pause file at the CB8 threshold). Nil disables the
	// circuit breaker entirely — useful for tests and for the bench
	// harness when we want to observe raw failure modes without the
	// breaker hiding them.
	//
	// This is the implementation of github.com/mystictraveler/iceberg-janitor#2
	// (CB8: consecutive failure pause).
	CircuitBreaker *safety.CircuitBreaker

	// OldPathsOverride, if non-empty, replaces the manifest-walk file
	// selection with this exact list. The compactor stitches these
	// files into one new file and replaces them via CAS, skipping the
	// per-partition discovery logic. Used by the hot loop to do delta
	// stitching: the orchestrator picks an "anchor" large file plus
	// the small files added since the last round, and the compactor
	// just executes the replace.
	//
	// All paths must belong to the same partition (the master check
	// will fail otherwise on the row count invariant). The orchestrator
	// is responsible for the file selection; this option is the escape
	// hatch that lets it inject the result.
	//
	// When set, PartitionTuple and TargetFileSizeBytes are ignored —
	// the file list is taken at face value.
	OldPathsOverride []string

	// DryRun, when true, runs the manifest walk and input-file
	// selection exactly as a real compact would, then STOPS before
	// any side effects: no new parquet file is written, no
	// transaction is staged, no commit happens. The result
	// reports projected After* counts plus ContentionDetected,
	// which compares the table's current snapshot id before vs
	// after the manifest walk. If a foreign writer committed
	// during the walk, ContentionDetected is true — the caller
	// can infer that a real run would have retried through a CAS
	// conflict.
	DryRun bool
}

func (o *CompactOptions) defaults() {
	if o.MaxAttempts <= 0 {
		// 15 attempts at exponential backoff (100ms doubling) gives
		// us ~50s of patience under writer-fight, which is enough
		// for the streamer to leave a quiet window between commit
		// bursts on the partition we're touching.
		o.MaxAttempts = 15
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Second
	}
}

// CompactResult is the structured outcome of a successful Compact call.
// On failure the function returns an error and a partial result with
// whatever was filled in before the failure.
type CompactResult struct {
	Identifier icebergtable.Identifier `json:"identifier"`
	TableUUID  string                  `json:"table_uuid,omitempty"`
	Attempts   int                     `json:"attempts"`

	BeforeSnapshotID int64 `json:"before_snapshot_id"`
	BeforeFiles      int   `json:"before_files"`
	BeforeBytes      int64 `json:"before_bytes"`
	BeforeRows       int64 `json:"before_rows"`

	AfterSnapshotID int64 `json:"after_snapshot_id"`
	AfterFiles      int   `json:"after_files"`
	AfterBytes      int64 `json:"after_bytes"`
	AfterRows       int64 `json:"after_rows"`

	Verification *safety.Verification `json:"verification"`
	DurationMs   int64                `json:"duration_ms"`

	// DryRun is true when the caller set CompactOptions.DryRun. In
	// that case the Before* fields reflect the actual table state,
	// the After* fields reflect the projected outcome of a real
	// run (projected_after_files = before_files - len(oldPaths) + 1
	// when a rewrite would occur), and ContentionDetected reports
	// whether the snapshot id advanced during the manifest walk.
	DryRun             bool `json:"dry_run,omitempty"`
	ContentionDetected bool `json:"contention_detected,omitempty"`
	PlannedOldFiles    int  `json:"planned_old_files,omitempty"`
	PlannedNewFiles    int  `json:"planned_new_files,omitempty"`

	// Skipped is set when compaction deliberately did NOT run — the
	// source set was either empty (already compacted / nothing to do)
	// or crossed a schema-evolution boundary (SkippedReason =
	// "mixed_schemas"). Skipped runs are NOT failures: BeforeSnapshotID
	// == AfterSnapshotID, zero rows moved. See SkippedReason for the
	// discriminator.
	Skipped       bool   `json:"skipped,omitempty"`
	SkippedReason string `json:"skipped_reason,omitempty"`
	// SkippedDetail is a short human-readable description useful in
	// logs and dashboards, e.g.
	//   "schema=1 files=45 | schema=2 files=7"
	SkippedDetail string `json:"skipped_detail,omitempty"`
}

// Compact rewrites the table's data files via the parquet-go-direct
// compaction path (compactOnce in compact_replace.go), runs the
// mandatory pre-commit master check, and commits via the directory
// catalog's atomic CAS.
//
// On a CAS conflict (another writer beat us to v(N+1).metadata.json)
// or an iceberg-go requirement-validation failure (a foreign writer
// committed between our stage and our commit), Compact reloads the
// table and retries up to MaxAttempts times with exponential backoff.
// This is the optimistic-concurrency model the design assumes.
//
// This is the shared implementation called by both cmd/janitor-cli
// (in-process) and cmd/janitor-server (HTTP handler). Both binaries
// reuse the same code path so the master check, CAS retry, and
// reporting are identical regardless of how the request arrived.
//
// The compaction READ + WRITE both go through parquet-go directly
// (compact_replace.go). iceberg-go is only used for the COMMIT step
// (Transaction.ReplaceDataFiles), the metadata.json LoadTable, and
// the manifest walk used to discover the file list. iceberg-go's
// Scan / Overwrite path is no longer used because it loses rows under
// concurrent producer load — see the doc comment on compactOnce in
// compact_replace.go for the full story.
func Compact(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions) (*CompactResult, error) {
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "Compact")
	span.SetAttributes(
		observe.Table(ident[0], ident[1]),
		observe.Operation("compact"),
	)
	defer span.End()

	opts.defaults()
	started := time.Now()
	result := &CompactResult{Identifier: ident}

	// CB8 preflight. If a circuit breaker is configured, we need the
	// table's UUID to look up its state — and the only way to learn
	// the UUID is to load the table. We do this once at the top
	// (outside the retry loop) and remember it. The retry loop's
	// inner compactOnce also reloads the table, which double-LoadTables
	// on the first attempt — acceptable because LoadTable is one HEAD
	// + one GET on the metadata file and the bench shows it's not the
	// bottleneck.
	var tableUUID string
	if opts.CircuitBreaker != nil {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, fmt.Errorf("loading table for circuit breaker: %w", err)
		}
		tableUUID = tbl.Metadata().TableUUID().String()
		result.TableUUID = tableUUID
		if err := opts.CircuitBreaker.Preflight(ctx, tableUUID); err != nil {
			// PausedError is returned as-is so callers can errors.As
			// against *safety.PausedError. Other errors (e.g. blob
			// read failure on the pause file) are also propagated:
			// the breaker fails closed by design.
			result.DurationMs = time.Since(started).Milliseconds()
			return result, err
		}
	}

	runErr := func() error {
		backoff := opts.InitialBackoff
		for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
			result.Attempts = attempt
			err := compactOnce(ctx, cat, ident, opts, result)
			if err == nil {
				return nil
			}
			// Retry on either layer of optimistic-concurrency conflict:
			// our directory catalog's CAS or iceberg-go's requirement
			// validation. See IsRetryableConcurrencyError for details.
			if !IsRetryableConcurrencyError(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > opts.MaxBackoff {
				backoff = opts.MaxBackoff
			}
		}
		return fmt.Errorf("compaction failed: exceeded %d concurrency-retry attempts", opts.MaxAttempts)
	}()

	result.DurationMs = time.Since(started).Milliseconds()

	// CB8 outcome recording. Runs unconditionally on both success and
	// failure. A failure to record the outcome is logged via the
	// returned error chain but does NOT mask the original runErr —
	// the caller cares much more about "the compaction failed" than
	// "we also could not write the failure counter to the state
	// file". On success, the recordErr is the only error to return.
	if opts.CircuitBreaker != nil && tableUUID != "" {
		recordErr := opts.CircuitBreaker.RecordOutcome(ctx, tableUUID, runErr)
		if recordErr != nil && runErr == nil {
			return result, recordErr
		}
		// If runErr != nil we deliberately drop recordErr; the
		// underlying compaction failure is the actionable signal.
	}

	// Annotate the span with the transition + outcome. No-op on the
	// default NoOp tracer — costless when nobody's collecting spans.
	span.SetAttributes(observe.FilesBeforeAfter(result.BeforeFiles, result.AfterFiles)...)
	span.SetAttributes(observe.RowsBeforeAfter(result.BeforeRows, result.AfterRows, 0)...)
	span.SetAttributes(
		observe.Attempt(result.Attempts),
		observe.DurationMs(result.DurationMs),
	)

	// Metrics signal. One Counter.Add + one or two Histogram.Record
	// per Compact call, independent of the per-file worker hot path.
	// NoOp meter provider (production default) makes the recording
	// effectively free — see pkg/observe/metrics.go for the
	// rationale.
	tableTag := ident[0] + "." + ident[1]
	var outcome string
	switch {
	case result.Skipped:
		outcome = "skip"
	case runErr != nil:
		outcome = "fail"
	default:
		outcome = "pass"
	}
	observe.RecordCompactOutcome(ctx, tableTag, outcome, result.SkippedReason,
		result.DurationMs, result.Attempts, result.BeforeFiles, result.AfterFiles)

	if result.Skipped {
		span.SetAttributes(observe.SkippedReason(result.SkippedReason), observe.Result("skip"))
	} else if runErr != nil {
		span.SetAttributes(observe.Result("fail"))
		return result, observe.RecordError(span, runErr)
	} else {
		span.SetAttributes(observe.Result("pass"))
	}

	if runErr != nil {
		return result, runErr
	}
	return result, nil
}

// defaultTargetFileSizeBytes is used when neither the caller nor the
// table property specifies a target. 128 MB matches the iceberg spec's
// conventional floor and is what most query engines (DuckDB, Trino,
// Spark) assume for "well-sized" parquet files.
const defaultTargetFileSizeBytes int64 = 128 * 1024 * 1024

// defaultTargetFileSize reads the iceberg standard table property
// write.target-file-size-bytes and returns it as int64. If the
// property is absent or unparseable, returns defaultTargetFileSizeBytes
// (128 MB). This makes Pattern B opt-out rather than opt-in: any
// iceberg-spec-compliant table is auto-compacted to its own target
// without operator action.
func defaultTargetFileSize(tbl *icebergtable.Table) int64 {
	props := tbl.Metadata().Properties()
	if v, ok := props["write.target-file-size-bytes"]; ok {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return defaultTargetFileSizeBytes
}

// compactTableConcurrency is the maximum number of partitions
// compacted in parallel by CompactTable. Each partition compact is
// a full CAS-retry loop with manifest walk + parquet read/write,
// so this bounds the peak I/O and memory pressure.
const compactTableConcurrency = 8

// CompactTableResult aggregates the outcomes of compacting every
// partition in a table. Each partition gets its own CompactResult.
type CompactTableResult struct {
	Identifier       icebergtable.Identifier `json:"identifier"`
	PartitionsFound  int                     `json:"partitions_found"`
	PartitionsToCompact int                  `json:"partitions_to_compact"`
	PartitionsSucceeded int                  `json:"partitions_succeeded"`
	PartitionsFailed int                     `json:"partitions_failed"`
	PartitionResults []*CompactResult        `json:"partition_results,omitempty"`
	TotalDurationMs  int64                   `json:"total_duration_ms"`
}

// CompactTable discovers all partitions with small files in the table
// and compacts each in parallel via a bounded worker pool. This is the
// "one call compacts the whole table" entry point used by the maintain
// endpoint.
//
// The discovery walk reads the current snapshot's manifest entries,
// groups them by partition tuple, and selects partitions that have at
// least one file below TargetFileSizeBytes. Partitions where every
// file is already at or above the threshold are skipped (nothing to
// do — Pattern B would skip them anyway).
//
// Each selected partition is compacted via Compact() with the
// partition tuple set, running up to compactTableConcurrency partitions
// in parallel. A single partition failure does NOT abort the others;
// all results are collected and returned.
//
// For unpartitioned tables or when TargetFileSizeBytes is 0 (whole-
// table rewrite mode), CompactTable falls through to a single Compact()
// call with no partition filter.
func CompactTable(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions) (*CompactTableResult, error) {
	opts.defaults()
	started := time.Now()
	result := &CompactTableResult{Identifier: ident}

	// Load the table to discover its partition spec.
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, fmt.Errorf("loading table %v: %w", ident, err)
	}

	// If no target file size was set by the caller, read the iceberg
	// standard table property write.target-file-size-bytes. This is
	// what Spark, Glue, and other compaction tools use. If the
	// property isn't set either, default to 128MB — the iceberg spec's
	// conventional floor for well-organized data files.
	if opts.TargetFileSizeBytes == 0 {
		opts.TargetFileSizeBytes = defaultTargetFileSize(tbl)
	}

	spec := tbl.Spec()
	// Unpartitioned table or whole-table rewrite: single Compact call.
	if spec.NumFields() == 0 || opts.TargetFileSizeBytes == 0 {
		r, err := Compact(ctx, cat, ident, opts)
		result.PartitionsFound = 1
		result.PartitionsToCompact = 1
		result.PartitionResults = []*CompactResult{r}
		if err != nil {
			result.PartitionsFailed = 1
		} else {
			result.PartitionsSucceeded = 1
		}
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, err
	}

	// Discover partitions with small files.
	partitions, err := discoverSmallFilePartitions(ctx, tbl, spec, opts.TargetFileSizeBytes)
	if err != nil {
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, fmt.Errorf("discovering partitions: %w", err)
	}
	result.PartitionsFound = len(partitions)
	if len(partitions) == 0 {
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, nil
	}
	result.PartitionsToCompact = len(partitions)

	// Compact each partition in parallel.
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(compactTableConcurrency)
	for _, pt := range partitions {
		pt := pt
		g.Go(func() error {
			if gctx.Err() != nil {
				return nil // context canceled, skip
			}
			partOpts := opts
			partOpts.PartitionTuple = pt
			r, err := Compact(gctx, cat, ident, partOpts)
			mu.Lock()
			result.PartitionResults = append(result.PartitionResults, r)
			if err != nil {
				result.PartitionsFailed++
			} else {
				result.PartitionsSucceeded++
			}
			mu.Unlock()
			return nil // don't propagate — we want all partitions to run
		})
	}
	_ = g.Wait()
	result.TotalDurationMs = time.Since(started).Milliseconds()
	return result, nil
}

// discoverSmallFilePartitions walks the current snapshot's data files
// and returns the set of distinct partition tuples that have at least
// one file smaller than targetSize bytes. Each returned map is a
// PartitionTuple suitable for passing to CompactOptions.
func discoverSmallFilePartitions(ctx context.Context, tbl *icebergtable.Table, spec icebergpkg.PartitionSpec, targetSize int64) ([]map[string]string, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	// Build a reverse map: partition field id → source column name.
	schema := tbl.Metadata().CurrentSchema()
	fieldIDToName := buildFieldIDToName(spec, schema)

	// Collect partition tuples that have at least one small file.
	seen := map[partKey]map[string]string{}
	hasSmall := map[partKey]bool{}

	for _, m := range manifests {
		if m.ManifestContent() != icebergpkg.ManifestContentData {
			continue
		}
		entries, err := readManifestEntries(ctx, fs, m)
		if err != nil {
			continue // skip unreadable manifests
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			key := canonicalKey(df.Partition())
			if _, ok := seen[key]; !ok {
				seen[key] = partitionToTuple(df.Partition(), fieldIDToName)
			}
			if df.FileSizeBytes() < targetSize {
				hasSmall[key] = true
			}
		}
	}

	// Collect only partitions that have small files.
	result := make([]map[string]string, 0, len(hasSmall))
	for key := range hasSmall {
		if tuple, ok := seen[key]; ok && len(tuple) > 0 {
			result = append(result, tuple)
		}
	}
	// Sort for determinism.
	sort.Slice(result, func(i, j int) bool {
		return fmt.Sprint(result[i]) < fmt.Sprint(result[j])
	})
	return result, nil
}

// buildFieldIDToName returns a map from partition field ID to the
// source column name in the schema, used to construct PartitionTuple
// maps keyed by column name.
func buildFieldIDToName(spec icebergpkg.PartitionSpec, schema *icebergpkg.Schema) map[int]string {
	out := map[int]string{}
	for f := range spec.Fields() {
		srcField, ok := schema.FindFieldByID(f.SourceID)
		if ok {
			out[f.FieldID] = srcField.Name
		}
	}
	return out
}

// partitionToTuple converts a DataFile.Partition() map[int]any into
// a PartitionTuple map[string]string keyed by column name with
// string-encoded values.
func partitionToTuple(p map[int]any, fieldIDToName map[int]string) map[string]string {
	out := map[string]string{}
	for fid, val := range p {
		if name, ok := fieldIDToName[fid]; ok {
			out[name] = fmt.Sprint(val)
		}
	}
	return out
}

// canonicalKey produces a stable string key for a partition map,
// used for deduplication during discovery.
func canonicalKey(p map[int]any) partKey {
	keys := make([]int, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		fmt.Fprintf(&b, "%d=%v", k, p[k])
	}
	return partKey(b.String())
}

// partKey is a canonical string encoding of a partition tuple, used
// only for dedup during partition discovery.
type partKey string

func readManifestEntries(ctx context.Context, fs icebergio.IO, m icebergpkg.ManifestFile) ([]icebergpkg.ManifestEntry, error) {
	mf, err := fs.Open(m.FilePath())
	if err != nil {
		return nil, err
	}
	entries, err := icebergpkg.ReadManifest(m, mf, true)
	mf.Close()
	return entries, err
}

// SnapshotFileStats walks the current snapshot's manifests and returns
// (data file count, total bytes, total rows). Exported because both
// the CLI and the server need it for reporting.
//
// This reads every manifest body because `bytes` (total data file
// bytes) is not available in the manifest-list summary stats — only
// per-entry FileSizeBytes can give us the answer. Callers that don't
// need `bytes` should use SnapshotFileStatsFast instead, which reads
// nothing beyond the already-in-memory manifest list and is 10-50×
// faster on tables with many manifests.
func SnapshotFileStats(ctx context.Context, tbl *icebergtable.Table) (files int, bytes int64, rows int64) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, 0
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return 0, 0, 0
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, 0, 0
	}
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			continue
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			continue
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			files++
			bytes += df.FileSizeBytes()
			rows += df.Count()
		}
	}
	return files, bytes, rows
}

// SnapshotFileStatsFast returns (files, rows) for the current
// snapshot using only the manifest-list summary stats — it does not
// open or decode any manifest bodies. This is 10-50× faster than
// SnapshotFileStats on streaming tables with many micro-manifests,
// which is exactly the shape Compact and Expire see in steady state.
//
// The tradeoff: bytes is NOT returned because manifest-list entries
// do not carry a total bytes count, only row and file counts. Callers
// that need bytes must use SnapshotFileStats.
//
// For the stats-only use cases (Compact's Before/AfterFiles,
// Expire's Before/AfterFiles, maintain's result reporting when bytes
// is optional), this path eliminates ~600 manifest body reads per
// Compact on a 300-manifest table.
func SnapshotFileStatsFast(ctx context.Context, tbl *icebergtable.Table) (files int, rows int64) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return 0, 0
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, 0
	}
	for _, m := range manifests {
		if m.ManifestContent() != icebergpkg.ManifestContentData {
			continue
		}
		// AddedDataFiles + ExistingDataFiles is the count of LIVE data
		// file entries in this manifest; deleted entries are excluded
		// (same as ReadManifest(discardDeleted=true) would have returned).
		files += int(m.AddedDataFiles() + m.ExistingDataFiles())
		rows += m.AddedRows() + m.ExistingRows()
	}
	return files, rows
}
