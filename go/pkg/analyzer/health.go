// Package analyzer produces a HealthReport for an Iceberg table by walking
// its current snapshot's manifest list and aggregating file-level stats.
// Port of src/iceberg_janitor/analyzer/health.py.
package analyzer

import (
	"context"
	"fmt"

	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

// HealthReport summarizes a single Iceberg table's current state in the
// terms the janitor uses for triggering and policy decisions.
type HealthReport struct {
	// Identifier of the table within the warehouse.
	TableLocation string
	TableUUID     string
	FormatVersion int

	// Snapshot under which the report was computed.
	CurrentSnapshotID  int64
	SnapshotCount      int
	LiveSnapshotCount  int
	LastUpdatedMillis  int64

	// Data-file aggregates over the current snapshot.
	DataFileCount        int
	DataBytes            int64
	TotalRowCount        int64

	// File-size distribution relative to the small-file threshold.
	SmallFileThreshold int64 // bytes; the threshold used to compute SmallFileCount
	SmallFileCount     int
	SmallFileBytes     int64
	SmallFileRatio     float64 // SmallFileCount / DataFileCount, 0..1

	// Manifest aggregates.
	ManifestCount      int
	ManifestBytes      int64
	AvgManifestBytes   int64

	// Metadata aggregates and the crown-rule axiom.
	MetadataBytes        int64
	MetadataDataRatio    float64 // MetadataBytes / DataBytes, the H1 / CB3 axiom

	// Convenience flags relative to the default thresholds. Operators and
	// the orchestrator both consult these.
	IsHealthy           bool   // ratio < 5% AND small file ratio < 30%
	NeedsAttention      bool   // ratio > 5% OR small file ratio > 30%
	IsCritical          bool   // ratio > 10% OR small file ratio > 60%
	AttentionReason     string // human-readable summary if NeedsAttention
}

// AnalyzerOptions configures HealthReport computation.
type AnalyzerOptions struct {
	// SmallFileThresholdBytes is the per-file size below which a file is
	// counted as "small" for the small-file-ratio metric. Defaults to
	// 64 MiB if zero (a common Iceberg target). The streaming workload
	// class typically uses a higher threshold.
	SmallFileThresholdBytes int64

	// MetadataRatioWarn / MetadataRatioCritical are the H1 thresholds.
	// Defaults: 0.05 (5%) and 0.10 (10%).
	MetadataRatioWarn     float64
	MetadataRatioCritical float64

	// SmallFileRatioWarn / SmallFileRatioCritical are the small-file
	// thresholds for the IsHealthy / NeedsAttention / IsCritical flags.
	// Defaults: 0.30 (30%) and 0.60 (60%).
	SmallFileRatioWarn     float64
	SmallFileRatioCritical float64
}

func (o *AnalyzerOptions) defaults() {
	if o.SmallFileThresholdBytes == 0 {
		o.SmallFileThresholdBytes = 64 * 1024 * 1024
	}
	if o.MetadataRatioWarn == 0 {
		o.MetadataRatioWarn = 0.05
	}
	if o.MetadataRatioCritical == 0 {
		o.MetadataRatioCritical = 0.10
	}
	if o.SmallFileRatioWarn == 0 {
		o.SmallFileRatioWarn = 0.30
	}
	if o.SmallFileRatioCritical == 0 {
		o.SmallFileRatioCritical = 0.60
	}
}

// Assess walks `tbl`'s current snapshot, reads every manifest entry, and
// aggregates a HealthReport. The walk is bounded by the current snapshot
// only — it does NOT scan history (use TableHistory for that).
//
// Cost: one read of the table's metadata.json (already in memory by the
// time `tbl` is loaded), one read of the manifest list, and one read per
// referenced manifest. No data file reads.
func Assess(ctx context.Context, tbl *icebergtable.Table, opts AnalyzerOptions) (*HealthReport, error) {
	opts.defaults()

	meta := tbl.Metadata()
	report := &HealthReport{
		TableLocation:      tbl.Location(),
		TableUUID:          meta.TableUUID().String(),
		FormatVersion:      meta.Version(),
		LastUpdatedMillis:  meta.LastUpdatedMillis(),
		SnapshotCount:      len(meta.Snapshots()),
		LiveSnapshotCount:  len(meta.Snapshots()), // same as SnapshotCount unless we filter
		SmallFileThreshold: opts.SmallFileThresholdBytes,
	}

	currentSnap := tbl.CurrentSnapshot()
	if currentSnap == nil {
		// Empty table, no snapshots yet.
		report.IsHealthy = true
		return report, nil
	}
	report.CurrentSnapshotID = currentSnap.SnapshotID

	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening table FS: %w", err)
	}

	// Walk every manifest in the current snapshot.
	manifests, err := currentSnap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("listing manifests for snapshot %d: %w", currentSnap.SnapshotID, err)
	}
	report.ManifestCount = len(manifests)

	for _, m := range manifests {
		report.ManifestBytes += m.Length()

		// Read entries to get per-file stats.
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return nil, fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return nil, fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		for _, entry := range entries {
			df := entry.DataFile()
			if df == nil {
				continue
			}
			// Only count data files. Delete files (DV / position deletes)
			// are accounted for separately later — for the v0 health
			// report we focus on data.
			if df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			report.DataFileCount++
			report.DataBytes += df.FileSizeBytes()
			report.TotalRowCount += df.Count()
			if df.FileSizeBytes() < opts.SmallFileThresholdBytes {
				report.SmallFileCount++
				report.SmallFileBytes += df.FileSizeBytes()
			}
		}
	}

	if report.DataFileCount > 0 {
		report.SmallFileRatio = float64(report.SmallFileCount) / float64(report.DataFileCount)
	}
	if report.ManifestCount > 0 {
		report.AvgManifestBytes = report.ManifestBytes / int64(report.ManifestCount)
	}

	// Metadata bytes = manifest list + manifests + the metadata.json
	// itself. We approximate the metadata.json size from its location
	// (we don't HEAD it here to save a round-trip; the loader could
	// pass it in if precision matters).
	report.MetadataBytes = report.ManifestBytes
	// TODO: include manifest list length and metadata.json size

	if report.DataBytes > 0 {
		report.MetadataDataRatio = float64(report.MetadataBytes) / float64(report.DataBytes)
	}

	// Health flags.
	switch {
	case report.MetadataDataRatio > opts.MetadataRatioCritical || report.SmallFileRatio > opts.SmallFileRatioCritical:
		report.IsCritical = true
		report.NeedsAttention = true
		report.AttentionReason = critReason(report, opts)
	case report.MetadataDataRatio > opts.MetadataRatioWarn || report.SmallFileRatio > opts.SmallFileRatioWarn:
		report.NeedsAttention = true
		report.AttentionReason = warnReason(report, opts)
	default:
		report.IsHealthy = true
	}

	return report, nil
}

func warnReason(r *HealthReport, opts AnalyzerOptions) string {
	switch {
	case r.MetadataDataRatio > opts.MetadataRatioWarn:
		return fmt.Sprintf("metadata-to-data ratio %.2f%% exceeds warn threshold %.0f%%",
			r.MetadataDataRatio*100, opts.MetadataRatioWarn*100)
	case r.SmallFileRatio > opts.SmallFileRatioWarn:
		return fmt.Sprintf("small-file ratio %.2f%% exceeds warn threshold %.0f%%",
			r.SmallFileRatio*100, opts.SmallFileRatioWarn*100)
	}
	return ""
}

func critReason(r *HealthReport, opts AnalyzerOptions) string {
	switch {
	case r.MetadataDataRatio > opts.MetadataRatioCritical:
		return fmt.Sprintf("CRITICAL: metadata-to-data ratio %.2f%% exceeds critical threshold %.0f%% — janitor maintenance is BLOCKED",
			r.MetadataDataRatio*100, opts.MetadataRatioCritical*100)
	case r.SmallFileRatio > opts.SmallFileRatioCritical:
		return fmt.Sprintf("CRITICAL: small-file ratio %.2f%% exceeds critical threshold %.0f%%",
			r.SmallFileRatio*100, opts.SmallFileRatioCritical*100)
	}
	return ""
}
