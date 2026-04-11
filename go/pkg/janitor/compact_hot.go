package janitor

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
)

// CompactHotOptions configures the hot-loop entry point.
type CompactHotOptions struct {
	// SmallFileThresholdBytes — files <= this byte count are eligible
	// for inclusion in a delta-stitch round.
	SmallFileThresholdBytes int64

	// MaintainInterval — how frequently the hot loop is invoked.
	// Used as the rotation period for the time-based round-robin
	// anchor selection: epoch_minute / interval_minutes determines
	// the rotation index, and (rotation_index + hash(partition_key))
	// % len(large_files) picks the anchor for this round. The result
	// is that successive hot rounds touch different large files in
	// the same partition, distributing wear and avoiding the case
	// where one giant file keeps absorbing the small-file tail while
	// other large files in the partition stay static.
	MaintainInterval time.Duration

	// HotWindowSnapshots — only partitions whose
	// MaxSequenceNumber is within the last N snapshot sequence
	// numbers are touched. Cold partitions are skipped (cold loop
	// handles them).
	HotWindowSnapshots int

	// MinSmallFiles — skip partitions with fewer small files than
	// this. Default 2 (no point stitching one file).
	MinSmallFiles int
}

func (o *CompactHotOptions) defaults() {
	if o.SmallFileThresholdBytes <= 0 {
		o.SmallFileThresholdBytes = 64 * 1024 * 1024
	}
	if o.MaintainInterval <= 0 {
		o.MaintainInterval = 5 * time.Minute
	}
	if o.HotWindowSnapshots <= 0 {
		o.HotWindowSnapshots = 5
	}
	if o.MinSmallFiles <= 0 {
		o.MinSmallFiles = 2
	}
}

// CompactHotResult is the per-partition outcome of a hot-loop round.
type CompactHotResult struct {
	Identifier         icebergtable.Identifier `json:"identifier"`
	PartitionsAnalyzed int                     `json:"partitions_analyzed"`
	PartitionsHot      int                     `json:"partitions_hot"`
	PartitionsStitched int                     `json:"partitions_stitched"`
	PartitionsSkipped  int                     `json:"partitions_skipped"`
	PartitionsFailed   int                     `json:"partitions_failed"`
	TotalDurationMs    int64                   `json:"total_duration_ms"`
	Partitions         []HotPartitionResult    `json:"partitions"`
}

type HotPartitionResult struct {
	PartitionKey  string `json:"partition_key"`
	Anchor        string `json:"anchor"`
	SmallCount    int    `json:"small_count"`
	Bootstrap     bool   `json:"bootstrap"` // true if no large files existed
	Stitched      bool   `json:"stitched"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkippedReason string `json:"skipped_reason,omitempty"`
	Error         string `json:"error,omitempty"`
	DurationMs    int64  `json:"duration_ms"`
	BeforeFiles   int    `json:"before_files"`
	AfterFiles    int    `json:"after_files"`
}

// CompactHot runs the hot-loop maintenance pass on the table:
//
//  1. Analyze partitions to find the hot ones (those whose
//     MaxSequenceNumber falls inside the hot window).
//  2. For each hot partition with at least MinSmallFiles small files:
//     a. Pick an "anchor" file via time-based round-robin over the
//        partition's large files. If no large files exist, use the
//        biggest small file as the anchor (bootstrap).
//     b. Build the stitch input set: anchor + all small files in the
//        partition (excluding the anchor itself if it was a small).
//     c. Call Compact with OldPathsOverride set to that input set.
//        compactOnce's fast path skips the manifest walk and stitches
//        the given files directly.
//
// Each partition is processed independently. A failure on one partition
// does not stop the others — the result aggregates per-partition
// outcomes. The function returns a non-nil error only if the analyzer
// itself fails.
func CompactHot(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactHotOptions) (*CompactHotResult, error) {
	opts.defaults()
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "CompactHot")
	span.SetAttributes(observe.Table(ident[0], ident[1]))
	defer span.End()

	started := time.Now()
	result := &CompactHotResult{Identifier: ident}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return result, fmt.Errorf("loading table %v: %w", ident, err)
	}

	parts, err := analyzer.AnalyzePartitions(ctx, tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: opts.SmallFileThresholdBytes,
		HotWindowSnapshots:      opts.HotWindowSnapshots,
	})
	if err != nil {
		return result, fmt.Errorf("analyzing partitions: %w", err)
	}
	result.PartitionsAnalyzed = len(parts)

	// Time-based rotation index. epoch_minutes / interval_minutes
	// gives a rotation that advances each maintain cycle and is
	// stable across multiple invocations within the same window.
	intervalMin := int64(opts.MaintainInterval / time.Minute)
	if intervalMin < 1 {
		intervalMin = 1
	}
	rotationIndex := time.Now().Unix() / 60 / intervalMin

	for _, p := range parts {
		if !p.IsHot {
			continue
		}
		result.PartitionsHot++

		if p.SmallFileCount < opts.MinSmallFiles {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				SmallCount:    p.SmallFileCount,
				Skipped:       true,
				SkippedReason: fmt.Sprintf("only %d small files (min %d)", p.SmallFileCount, opts.MinSmallFiles),
			})
			continue
		}

		// Pick the anchor.
		anchor, bootstrap := pickAnchor(p, rotationIndex)
		if anchor.Path == "" {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				SmallCount:    p.SmallFileCount,
				Skipped:       true,
				SkippedReason: "no anchor candidate",
			})
			continue
		}

		// Build the stitch input: anchor + all small files (excluding
		// anchor if it was already in the small set during bootstrap).
		stitchInputs := []string{anchor.Path}
		for _, sf := range p.SmallFiles {
			if sf.Path == anchor.Path {
				continue
			}
			stitchInputs = append(stitchInputs, sf.Path)
		}
		if len(stitchInputs) < 2 {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				Anchor:        anchor.Path,
				SmallCount:    p.SmallFileCount,
				Bootstrap:     bootstrap,
				Skipped:       true,
				SkippedReason: "stitch set has fewer than 2 inputs",
			})
			continue
		}

		// Run the delta stitch via Compact's fast path.
		partStarted := time.Now()
		compactResult, cerr := Compact(ctx, cat, ident, CompactOptions{
			OldPathsOverride: stitchInputs,
		})
		partResult := HotPartitionResult{
			PartitionKey: p.PartitionKey,
			Anchor:       anchor.Path,
			SmallCount:   p.SmallFileCount,
			Bootstrap:    bootstrap,
			DurationMs:   time.Since(partStarted).Milliseconds(),
		}
		if compactResult != nil {
			partResult.BeforeFiles = compactResult.BeforeFiles
			partResult.AfterFiles = compactResult.AfterFiles
		}
		if cerr != nil {
			result.PartitionsFailed++
			partResult.Error = cerr.Error()
		} else {
			result.PartitionsStitched++
			partResult.Stitched = true
		}
		result.Partitions = append(result.Partitions, partResult)
	}

	result.TotalDurationMs = time.Since(started).Milliseconds()
	return result, nil
}

// pickAnchor selects the anchor file for a partition's delta-stitch
// round. Strategy:
//
//   - If the partition has any large files, time-based round-robin
//     picks one of them: large_files[(rotationIndex + hash(key)) % N].
//     This distributes the stitch wear across all large files in the
//     partition rather than always growing the same one.
//
//   - If the partition has NO large files (typical of a freshly-seeded
//     partition or one whose janitor has never run), bootstrap by
//     picking the largest small file as the anchor. After the first
//     stitch round, the result becomes the partition's first large
//     file and the round-robin path engages from then on.
//
// Returns the picked file and a bootstrap flag for telemetry.
func pickAnchor(p analyzer.PartitionHealth, rotationIndex int64) (analyzer.FileInfo, bool) {
	if len(p.LargeFiles) > 0 {
		// Stable sort by path so the rotation is deterministic across
		// runs even if the analyzer's manifest walk produces different
		// orderings.
		large := append([]analyzer.FileInfo(nil), p.LargeFiles...)
		sort.Slice(large, func(i, j int) bool {
			return large[i].Path < large[j].Path
		})
		idx := (rotationIndex + hashKey(p.PartitionKey)) % int64(len(large))
		if idx < 0 {
			idx += int64(len(large))
		}
		return large[idx], false
	}
	if len(p.SmallFiles) == 0 {
		return analyzer.FileInfo{}, false
	}
	// Bootstrap: pick the biggest small file.
	biggest := p.SmallFiles[0]
	for _, f := range p.SmallFiles[1:] {
		if f.Bytes > biggest.Bytes {
			biggest = f
		}
	}
	return biggest, true
}

func hashKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
