package analyzer

import (
	"context"
	"fmt"
	"sort"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

// FileInfo is a minimal per-file record kept on each PartitionHealth
// so the hot/cold orchestrators can plan stitch operations without
// re-walking the manifest list.
type FileInfo struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
	Rows  int64  `json:"rows"`
}

// PartitionHealth is the per-partition view of a table's data layout.
// It is the unit of work for the hot/cold maintenance loops.
type PartitionHealth struct {
	// PartitionKey is a stable string representation of the partition
	// tuple, e.g. "ss_store_sk=5" or "event_date=2026-04-10/region=US".
	// Used as the cache/state key.
	PartitionKey string `json:"partition_key"`

	// PartitionTuple is the raw column→value map suitable for passing
	// to CompactOptions.PartitionTuple.
	PartitionTuple map[string]string `json:"partition_tuple"`

	// Files holds every data file in this partition. Cheap to keep
	// (typically a few hundred strings per partition) and lets the
	// hot/cold orchestrators plan stitch sets without re-walking the
	// manifest list. The slice is split into LargeFiles + SmallFiles
	// by the threshold field for convenient access.
	Files      []FileInfo `json:"files,omitempty"`
	LargeFiles []FileInfo `json:"-"` // computed from Files
	SmallFiles []FileInfo `json:"-"` // computed from Files

	FileCount      int   `json:"file_count"`
	SmallFileCount int   `json:"small_file_count"`
	TotalBytes     int64 `json:"total_bytes"`
	TotalRows      int64 `json:"total_rows"`

	// MaxSequenceNumber is the highest manifest sequence number that
	// references a data file in this partition. The classifier uses
	// this as a proxy for "most recently touched" — partitions whose
	// max_sequence_number is within the snapshot's recent tail are
	// HOT, the rest are COLD.
	MaxSequenceNumber int64 `json:"max_sequence_number"`

	// IsHot is true when MaxSequenceNumber is within the hot window
	// (typically the last N snapshots, configurable per workload class).
	IsHot bool `json:"is_hot"`

	// Triggers fired for this partition. The cold loop runs full
	// compaction iff at least one trigger is true.
	NeedsSmallFileCompact   bool `json:"needs_small_file_compact"`
	NeedsMetadataReduction  bool `json:"needs_metadata_reduction"`
	NeedsStaleRewrite       bool `json:"needs_stale_rewrite"`
}

// NeedsCompaction returns true if any of the three triggers fired.
// This is the cold-loop entry condition.
func (p *PartitionHealth) NeedsCompaction() bool {
	return p.NeedsSmallFileCompact || p.NeedsMetadataReduction || p.NeedsStaleRewrite
}

// PartitionHealthOptions configure the per-partition trigger thresholds.
// Defaults derive from classify.MaintainOptions; the analyzer takes the
// numeric thresholds directly so it has no dependency on classify.
type PartitionHealthOptions struct {
	// SmallFileThresholdBytes — files <= this byte count are "small".
	SmallFileThresholdBytes int64

	// SmallFileTrigger — partition needs compaction if its small file
	// count exceeds this. Maps to classify.Thresholds.SmallFileTrigger.
	SmallFileTrigger int

	// FileCountTrigger — partition needs compaction if its TOTAL file
	// count exceeds this (independent of small/large mix). Catches
	// fragmented partitions even when files are individually large.
	FileCountTrigger int

	// HotWindowSnapshots — a partition is HOT if its
	// max_sequence_number is within the last N snapshot sequence
	// numbers. Streaming tables typically use a small number (the
	// last few minutes of activity).
	HotWindowSnapshots int

	// LastRewriteAges — optional per-partition map of how long ago
	// each partition was last rewritten by the janitor. Read from
	// _janitor/state/<table>/partitions.json. Partitions whose
	// rewrite age exceeds StaleRewriteAge get NeedsStaleRewrite=true.
	LastRewriteAges map[string]time.Duration

	// StaleRewriteAge — partitions whose last rewrite is older than
	// this get NeedsStaleRewrite=true. Maps to
	// classify.MaintainOptions.StaleRewriteAge.
	StaleRewriteAge time.Duration
}

func (o *PartitionHealthOptions) defaults() {
	if o.SmallFileThresholdBytes <= 0 {
		o.SmallFileThresholdBytes = 64 * 1024 * 1024
	}
	if o.SmallFileTrigger <= 0 {
		o.SmallFileTrigger = 50
	}
	if o.FileCountTrigger <= 0 {
		o.FileCountTrigger = 200
	}
	if o.HotWindowSnapshots <= 0 {
		o.HotWindowSnapshots = 5
	}
	if o.StaleRewriteAge <= 0 {
		o.StaleRewriteAge = 24 * time.Hour
	}
}

// AnalyzePartitions walks the current snapshot's manifest list, groups
// data files by partition tuple, and produces a PartitionHealth record
// per partition with hot/cold and trigger flags pre-computed.
//
// The walk is single-pass and the results are sorted by PartitionKey
// for deterministic output. Manifest reads are done sequentially here
// (not parallelized) — this is the analyzer, not the compactor, and
// it runs from the orchestrator at low frequency.
func AnalyzePartitions(ctx context.Context, tbl *icebergtable.Table, opts PartitionHealthOptions) ([]PartitionHealth, error) {
	opts.defaults()

	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting fs: %w", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("listing manifests: %w", err)
	}

	spec := tbl.Spec()

	// Aggregate by partition key.
	parts := map[string]*PartitionHealth{}
	maxSnapSeq := int64(0)

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

		mSeq := m.SequenceNum()
		if mSeq > maxSnapSeq {
			maxSnapSeq = mSeq
		}

		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}

			tuple := dataFilePartitionTuple(df, spec)
			key := partitionKey(tuple)

			ph, ok := parts[key]
			if !ok {
				ph = &PartitionHealth{
					PartitionKey:   key,
					PartitionTuple: tuple,
				}
				parts[key] = ph
			}
			ph.FileCount++
			ph.TotalBytes += df.FileSizeBytes()
			ph.TotalRows += df.Count()
			fi := FileInfo{Path: df.FilePath(), Bytes: df.FileSizeBytes(), Rows: df.Count()}
			ph.Files = append(ph.Files, fi)
			if df.FileSizeBytes() <= opts.SmallFileThresholdBytes {
				ph.SmallFileCount++
				ph.SmallFiles = append(ph.SmallFiles, fi)
			} else {
				ph.LargeFiles = append(ph.LargeFiles, fi)
			}
			if mSeq > ph.MaxSequenceNumber {
				ph.MaxSequenceNumber = mSeq
			}
		}
	}

	// Compute hot/cold + triggers.
	hotThreshold := maxSnapSeq - int64(opts.HotWindowSnapshots)
	out := make([]PartitionHealth, 0, len(parts))
	for _, ph := range parts {
		ph.IsHot = ph.MaxSequenceNumber > hotThreshold
		ph.NeedsSmallFileCompact = ph.SmallFileCount >= opts.SmallFileTrigger
		// Metadata reduction trigger: a partition with very high file
		// count (small or not) burdens the manifest layer.
		ph.NeedsMetadataReduction = ph.FileCount >= opts.FileCountTrigger
		// Stale rewrite: only fire if we have state for this partition
		// AND its last rewrite is older than the threshold.
		if age, ok := opts.LastRewriteAges[ph.PartitionKey]; ok && age > opts.StaleRewriteAge {
			ph.NeedsStaleRewrite = true
		}
		out = append(out, *ph)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PartitionKey < out[j].PartitionKey
	})
	return out, nil
}

// dataFilePartitionTuple converts a DataFile's partition record into
// the column→value map the rest of the system uses. The keys are
// source schema column names, the values are stringified for
// convenient comparison and serialization.
func dataFilePartitionTuple(df icebergpkg.DataFile, spec icebergpkg.PartitionSpec) map[string]string {
	tuple := map[string]string{}
	part := df.Partition()
	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		// PartitionSpec field has SourceID but we need the source
		// column NAME. df.Partition() is keyed by partition field id.
		v, ok := part[field.FieldID]
		if !ok {
			continue
		}
		tuple[field.Name] = fmt.Sprintf("%v", v)
	}
	return tuple
}

// partitionKey returns a stable string representation of a partition
// tuple. Sorted by column name so equivalent tuples produce equal keys
// regardless of map iteration order.
func partitionKey(tuple map[string]string) string {
	if len(tuple) == 0 {
		return "(unpartitioned)"
	}
	keys := make([]string, 0, len(tuple))
	for k := range tuple {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += "/"
		}
		out += k + "=" + tuple[k]
	}
	return out
}
