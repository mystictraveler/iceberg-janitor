package janitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

// CompactOptions configure a single Compact call.
type CompactOptions struct {
	// MaxAttempts caps the CAS-conflict retry loop. Default 15.
	MaxAttempts int
	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time. Default 100ms.
	InitialBackoff time.Duration

	// PartitionTuple, if non-nil, scopes the compaction to one
	// partition's worth of data files. The compactOnce implementation
	// (compact_replace.go) walks the snapshot manifest list directly,
	// looks up the column's iceberg field id from the table schema,
	// and matches against DataFile.Partition()[partition_field_id].
	//
	// Map key is the schema column name (e.g. "ss_store_sk"); value
	// is the typed Go value (int64 for the int columns we currently
	// support).
	//
	// nil means "compact the whole table". On an unpartitioned table,
	// passing a non-nil PartitionTuple is a hard error — see the
	// guard at the top of compactOnce.
	//
	// This is the smaller-scope-per-compaction primitive for the
	// writer-fight pathology: instead of compacting the whole table
	// in one big atomic transaction (which races the streamer for
	// every file the streamer wrote since we started planning), we
	// compact one partition's worth of files in a smaller transaction
	// that only conflicts with the streamer's commits to that one
	// partition.
	PartitionTuple map[string]any

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

	if runErr != nil {
		return result, runErr
	}
	return result, nil
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
