package janitor

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
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

// CompactOptions configure a single Compact call. The MVP exposes only
// the bare minimum; partition scoping, sort order, ROI estimate, and
// the rest land as the relevant maintenance ops do.
type CompactOptions struct {
	// MaxAttempts caps the CAS-conflict retry loop. Default 5.
	MaxAttempts int
	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time. Default 100ms.
	InitialBackoff time.Duration
}

func (o *CompactOptions) defaults() {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
}

// CompactResult is the structured outcome of a successful Compact call.
// On failure the function returns an error and a partial result with
// whatever was filled in before the failure.
type CompactResult struct {
	Identifier  icebergtable.Identifier `json:"identifier"`
	Attempts    int                     `json:"attempts"`

	BeforeSnapshotID int64 `json:"before_snapshot_id"`
	BeforeFiles      int   `json:"before_files"`
	BeforeBytes      int64 `json:"before_bytes"`
	BeforeRows      int64 `json:"before_rows"`

	AfterSnapshotID int64 `json:"after_snapshot_id"`
	AfterFiles      int   `json:"after_files"`
	AfterBytes      int64 `json:"after_bytes"`
	AfterRows      int64 `json:"after_rows"`

	Verification *safety.Verification `json:"verification"`
	DurationMs   int64                `json:"duration_ms"`
}

// Compact reads all data from the table, rewrites it via iceberg-go's
// transactional Overwrite, runs the mandatory pre-commit master check
// (I1 row count, I2 schema, I7 manifest references), and commits via
// the directory catalog's atomic CAS.
//
// On a CAS conflict (another writer beat us to v(N+1).metadata.json),
// Compact reloads the table and retries up to MaxAttempts times with
// exponential backoff. This is the optimistic-concurrency model the
// design assumes.
//
// This is the shared implementation called by both cmd/janitor-cli
// (in-process) and cmd/janitor-server (HTTP handler). Both binaries
// reuse the same code path so the master check, CAS retry, and
// reporting are identical regardless of how the request arrived.
//
// MVP scope: full read + full overwrite via streaming RecordReader.
// No partition scoping, no incremental work, no stitching binpack.
// True stitching (column-chunk byte copy) lands later.
func Compact(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions) (*CompactResult, error) {
	opts.defaults()
	started := time.Now()
	result := &CompactResult{Identifier: ident}

	backoff := opts.InitialBackoff
	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		result.Attempts = attempt
		err := compactOnce(ctx, cat, ident, result)
		if err == nil {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, nil
		}
		// Retry on either layer of optimistic-concurrency conflict:
		// our directory catalog's CAS or iceberg-go's requirement
		// validation. See IsRetryableConcurrencyError for details.
		if !IsRetryableConcurrencyError(err) {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, err
		}
		select {
		case <-ctx.Done():
			result.DurationMs = time.Since(started).Milliseconds()
			return result, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	result.DurationMs = time.Since(started).Milliseconds()
	return result, fmt.Errorf("compaction failed: exceeded %d concurrency-retry attempts", opts.MaxAttempts)
}

// compactOnce performs a single compaction attempt. It loads the table
// fresh, opens a transaction, stages a streaming overwrite, runs the
// master check, and commits. The before/after counts are written into
// `result` so the caller has structured stats even on failure.
func compactOnce(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, result *CompactResult) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	if snap := tbl.CurrentSnapshot(); snap != nil {
		result.BeforeSnapshotID = snap.SnapshotID
	}
	result.BeforeFiles, result.BeforeBytes, result.BeforeRows = SnapshotFileStats(ctx, tbl)

	scan := tbl.Scan()
	scanSchema, recordIter, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return fmt.Errorf("opening scan: %w", err)
	}
	reader := newStreamingRecordReader(scanSchema, recordIter)
	defer reader.Release()

	tx := tbl.NewTransaction()
	if err := tx.Overwrite(ctx, reader, nil); err != nil {
		return fmt.Errorf("staging streaming overwrite: %w", err)
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("scan iterator error: %w", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		return fmt.Errorf("getting staged table: %w", err)
	}

	// Mandatory pre-commit master check. Non-bypassable.
	verification, err := safety.VerifyCompactionConsistency(ctx, tbl, staged, cat.Props())
	result.Verification = verification
	if err != nil {
		return err
	}

	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return err
	}

	if snap := newTbl.CurrentSnapshot(); snap != nil {
		result.AfterSnapshotID = snap.SnapshotID
	}
	result.AfterFiles, result.AfterBytes, result.AfterRows = SnapshotFileStats(ctx, newTbl)
	if result.AfterRows != result.BeforeRows {
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", result.BeforeRows, result.AfterRows)
	}
	return nil
}

// SnapshotFileStats walks the current snapshot's manifests and returns
// (data file count, total bytes, total rows). Exported because both
// the CLI and the server need it for reporting.
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

// streamingRecordReader adapts an iter.Seq2[arrow.RecordBatch, error]
// (the streaming scan iterator iceberg-go returns from ToArrowRecords)
// into the array.RecordReader interface that Transaction.Overwrite
// expects. It pulls one batch at a time via iter.Pull2 — no buffering,
// no whole-table-in-memory.
//
// Moved here from cmd/janitor-cli so both the CLI and the server use
// the same memory-bounded streaming compaction code path.
type streamingRecordReader struct {
	schema  *arrow.Schema
	next    func() (arrow.RecordBatch, error, bool)
	stop    func()
	current arrow.RecordBatch
	err     error
	refs    int64
}

func newStreamingRecordReader(schema *arrow.Schema, seq iter.Seq2[arrow.RecordBatch, error]) *streamingRecordReader {
	next, stop := iter.Pull2(seq)
	return &streamingRecordReader{schema: schema, next: next, stop: stop, refs: 1}
}

func (r *streamingRecordReader) Schema() *arrow.Schema      { return r.schema }
func (r *streamingRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *streamingRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *streamingRecordReader) Err() error                     { return r.err }

func (r *streamingRecordReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	rec, err, ok := r.next()
	if !ok {
		return false
	}
	if err != nil {
		r.err = err
		return false
	}
	r.current = rec
	return true
}

func (r *streamingRecordReader) Retain() { r.refs++ }

func (r *streamingRecordReader) Release() {
	r.refs--
	if r.refs > 0 {
		return
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.stop != nil {
		r.stop()
		r.stop = nil
	}
}
