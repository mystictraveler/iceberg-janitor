package janitor

import (
	"context"
	"fmt"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/state"
)

// CompactColdOptions configures the cold-loop entry point.
type CompactColdOptions struct {
	// SmallFileThresholdBytes — files <= this byte count are "small".
	SmallFileThresholdBytes int64

	// SmallFileTrigger — partitions with at least this many small
	// files trigger a compaction. Maps to
	// classify.MaintainOptions.SmallFileThreshold.
	SmallFileTrigger int

	// FileCountTrigger — partitions with at least this many TOTAL
	// files trigger a compaction (catches fragmented partitions
	// with mostly-large but very many files).
	FileCountTrigger int

	// StaleRewriteAge — partitions whose last janitor rewrite is
	// older than this trigger a compaction. Reads from
	// _janitor/state/<table_uuid>/partitions.json.
	StaleRewriteAge time.Duration

	// HotWindowSnapshots — used to identify HOT partitions and
	// EXCLUDE them from the cold loop (the hot loop handles them
	// separately on a faster cadence). 0 means "don't exclude any".
	HotWindowSnapshots int

	// TargetFileSizeBytes — Pattern B threshold passed through to
	// the per-partition Compact call.
	TargetFileSizeBytes int64

	// DryRun, when true, forwards to every per-partition Compact
	// call so each partition's manifest walk runs but no side
	// effects happen. The aggregate result reports which partitions
	// WOULD be compacted; each sub-result carries its own DryRun
	// and ContentionDetected flags.
	DryRun bool
}

func (o *CompactColdOptions) defaults() {
	if o.SmallFileThresholdBytes <= 0 {
		o.SmallFileThresholdBytes = 64 * 1024 * 1024
	}
	if o.SmallFileTrigger <= 0 {
		o.SmallFileTrigger = 50
	}
	if o.FileCountTrigger <= 0 {
		o.FileCountTrigger = 200
	}
	if o.StaleRewriteAge <= 0 {
		o.StaleRewriteAge = 24 * time.Hour
	}
}

// CompactColdResult is the per-partition outcome of a cold-loop pass.
type CompactColdResult struct {
	Identifier         icebergtable.Identifier `json:"identifier"`
	PartitionsAnalyzed int                     `json:"partitions_analyzed"`
	PartitionsCold     int                     `json:"partitions_cold"`
	PartitionsTriggered int                    `json:"partitions_triggered"`
	PartitionsCompacted int                    `json:"partitions_compacted"`
	PartitionsSkipped  int                     `json:"partitions_skipped"`
	PartitionsFailed   int                     `json:"partitions_failed"`
	TotalDurationMs    int64                   `json:"total_duration_ms"`
	Partitions         []ColdPartitionResult   `json:"partitions"`
}

type ColdPartitionResult struct {
	PartitionKey      string `json:"partition_key"`
	FileCount         int    `json:"file_count"`
	SmallFileCount    int    `json:"small_file_count"`
	TriggerSmallFiles bool   `json:"trigger_small_files,omitempty"`
	TriggerMetadata   bool   `json:"trigger_metadata,omitempty"`
	TriggerStale      bool   `json:"trigger_stale,omitempty"`
	Compacted         bool   `json:"compacted"`
	Skipped           bool   `json:"skipped,omitempty"`
	Error             string `json:"error,omitempty"`
	DurationMs        int64  `json:"duration_ms"`
	BeforeFiles       int    `json:"before_files"`
	AfterFiles        int    `json:"after_files"`
}

// CompactCold runs the cold-loop maintenance pass on the table:
//
//  1. Analyze every partition (no hot/cold filtering up front — the
//     cold loop is the safety net for partitions the hot loop doesn't
//     touch).
//  2. Skip HOT partitions (they're owned by the hot loop).
//  3. For each remaining (cold) partition, evaluate the three
//     triggers: small_file_count >= threshold, file_count >= threshold,
//     last_rewrite_age >= threshold.
//  4. If any trigger fires, run a partition-scoped Compact (full,
//     non-delta) on that partition.
//
// The cold loop is sequential per partition (not parallel) — it
// prioritizes correctness and low CAS pressure over throughput.
// Long-running task semantics: takes its time, no urgency, lots of
// patience for the writer-fight CAS race.
//
// Per-partition state (state.PartitionState) is updated on success
// so the stale-rewrite trigger can fire on subsequent passes.
func CompactCold(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactColdOptions) (*CompactColdResult, error) {
	opts.defaults()
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "CompactCold")
	span.SetAttributes(observe.Table(ident[0], ident[1]))
	defer span.End()

	started := time.Now()
	result := &CompactColdResult{Identifier: ident}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return result, fmt.Errorf("loading table %v: %w", ident, err)
	}

	// Load per-partition state to populate the stale-rewrite trigger.
	tableUUID := tbl.Metadata().TableUUID().String()
	bucket := cat.Bucket()
	pstate, err := state.LoadPartitionState(ctx, bucket, tableUUID)
	if err != nil {
		// Don't fail the cold loop on a state read error — fall back
		// to "no stale-rewrite trigger" and let the other two triggers
		// (small_files, metadata) drive the work.
		pstate = &state.PartitionState{TableUUID: tableUUID, Partitions: map[string]state.PartitionRecord{}}
	}

	parts, err := analyzer.AnalyzePartitions(ctx, tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: opts.SmallFileThresholdBytes,
		SmallFileTrigger:        opts.SmallFileTrigger,
		FileCountTrigger:        opts.FileCountTrigger,
		HotWindowSnapshots:      opts.HotWindowSnapshots,
		LastRewriteAges:         pstate.LastRewriteAges(time.Now()),
		StaleRewriteAge:         opts.StaleRewriteAge,
	})
	if err != nil {
		return result, fmt.Errorf("analyzing partitions: %w", err)
	}
	result.PartitionsAnalyzed = len(parts)

	for _, p := range parts {
		// Skip HOT partitions: the hot loop owns them. The cold loop
		// is the safety net for everything else.
		if p.IsHot {
			continue
		}
		result.PartitionsCold++

		if !p.NeedsCompaction() {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, ColdPartitionResult{
				PartitionKey:   p.PartitionKey,
				FileCount:      p.FileCount,
				SmallFileCount: p.SmallFileCount,
				Skipped:        true,
			})
			continue
		}
		result.PartitionsTriggered++

		// Run partition-scoped Compact (slow path, full per-partition
		// rewrite) on this cold partition.
		partStarted := time.Now()
		compactResult, cerr := Compact(ctx, cat, ident, CompactOptions{
			PartitionTuple:      p.PartitionTuple,
			TargetFileSizeBytes: opts.TargetFileSizeBytes,
			DryRun:              opts.DryRun,
		})
		partResult := ColdPartitionResult{
			PartitionKey:      p.PartitionKey,
			FileCount:         p.FileCount,
			SmallFileCount:    p.SmallFileCount,
			TriggerSmallFiles: p.NeedsSmallFileCompact,
			TriggerMetadata:   p.NeedsMetadataReduction,
			TriggerStale:      p.NeedsStaleRewrite,
			DurationMs:        time.Since(partStarted).Milliseconds(),
		}
		if compactResult != nil {
			partResult.BeforeFiles = compactResult.BeforeFiles
			partResult.AfterFiles = compactResult.AfterFiles
		}
		if cerr != nil {
			result.PartitionsFailed++
			partResult.Error = cerr.Error()
		} else {
			result.PartitionsCompacted++
			partResult.Compacted = true
			// Record the rewrite for the next stale-trigger evaluation.
			pstate.Record(p.PartitionKey, "compact", partResult.AfterFiles, p.TotalBytes)
		}
		result.Partitions = append(result.Partitions, partResult)
	}

	// Best-effort save of partition state. A save failure shouldn't
	// fail the whole cold loop — the next pass will reload from S3
	// and the state-derived triggers will recompute.
	if result.PartitionsCompacted > 0 {
		_ = state.SavePartitionState(ctx, bucket, pstate)
	}

	result.TotalDurationMs = time.Since(started).Milliseconds()
	return result, nil
}
