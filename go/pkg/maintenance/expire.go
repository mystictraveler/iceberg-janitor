package maintenance

import (
	"context"
	"fmt"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// ExpireOptions configure a single Expire call.
//
// Both KeepLast and KeepWithin are forwarded to iceberg-go's
// Transaction.ExpireSnapshots, which intersects them: a snapshot is
// retained on the main branch's parent chain until BOTH (a) at least
// KeepLast snapshots are already retained AND (b) the snapshot is
// older than KeepWithin. iceberg-go also requires both values to be
// non-nil for branches that don't have their own ref-level
// MinSnapshotsToKeep / MaxSnapshotAgeMs properties — so we always
// pass both, even when the operator only specified one. Defaults are
// chosen to be safe rather than aggressive: 5 snapshots and 7 days.
//
// PostCommitDelete controls whether iceberg-go's removeSnapshotsUpdate
// runs its built-in orphan walker after the CAS commit succeeds. The
// walker reads each expired snapshot's manifests, gathers every
// referenced file path, subtracts the set still referenced by surviving
// snapshots, and Removes the difference (manifest_lists, manifests,
// data files). Default true. Set false on a directory catalog whose
// blob bucket is shared with consumers that may still hold references
// to the expired snapshot ids — that's the same trade-off any iceberg
// expire op makes.
//
// MaxAttempts and InitialBackoff mirror CompactOptions: the same CAS
// retry loop, the same exponential backoff, the same retryable-error
// classifier (janitor.IsRetryableConcurrencyError) covering both
// directory catalog CAS conflicts and iceberg-go's requirement
// validation failures.
//
// CircuitBreaker, when non-nil, gates Expire on the same per-table CB8
// pause file that compact uses. Expire failures count toward the same
// consecutive-failure budget. This is intentional: a table whose
// compactions are looping is also a table whose expirations should
// hold off until an operator looks at it.
type ExpireOptions struct {
	// KeepLast is the minimum number of snapshots to retain on the
	// main branch's parent chain. Default 5.
	KeepLast int

	// KeepWithin is the minimum age a snapshot must reach before it
	// becomes eligible for expiration. Default 7 days.
	KeepWithin time.Duration

	// PostCommitDelete controls iceberg-go's built-in orphan file
	// cleanup hook. Default true.
	PostCommitDelete bool

	// MaxAttempts caps the CAS-conflict retry loop. Default 15.
	MaxAttempts int

	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time, capped at MaxBackoff. Default 100ms.
	InitialBackoff time.Duration

	// MaxBackoff caps the per-attempt sleep so the doubling can't run
	// away. Default 5s. Same rationale as
	// CompactOptions.MaxBackoff — see the doc comment there.
	MaxBackoff time.Duration

	// CircuitBreaker, if non-nil, is consulted before the expiration
	// runs and is updated with the outcome afterward. Same shape as
	// CompactOptions.CircuitBreaker.
	CircuitBreaker *safety.CircuitBreaker

	// DryRun, when true, stages the expiration in-memory, runs the
	// master check on the staged table, and then STOPS before
	// tx.Commit. The result reports the snapshot ids that would be
	// removed plus the usual Before/After counters. Because staging
	// is pure in-memory work on the loaded Snapshots list, a dry run
	// does not write or read any extra data files — it only reloads
	// the table once at the end to probe for contention.
	DryRun bool
}

func (o *ExpireOptions) defaults() {
	if o.KeepLast <= 0 {
		o.KeepLast = 5
	}
	if o.KeepWithin <= 0 {
		o.KeepWithin = 7 * 24 * time.Hour
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 15
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 5 * time.Second
	}
}

// ExpireResult is the structured outcome of a successful Expire call.
type ExpireResult struct {
	Identifier icebergtable.Identifier `json:"identifier"`
	TableUUID  string                  `json:"table_uuid,omitempty"`
	Attempts   int                     `json:"attempts"`

	BeforeSnapshotID int64 `json:"before_snapshot_id"`
	BeforeSnapshots  int   `json:"before_snapshots"`
	BeforeRows       int64 `json:"before_rows"`
	BeforeFiles      int   `json:"before_files"`

	AfterSnapshotID int64 `json:"after_snapshot_id"`
	AfterSnapshots  int   `json:"after_snapshots"`
	AfterRows       int64 `json:"after_rows"`
	AfterFiles      int   `json:"after_files"`

	RemovedSnapshotIDs []int64 `json:"removed_snapshot_ids"`

	Verification *safety.Verification `json:"verification"`
	DurationMs   int64                `json:"duration_ms"`

	// DryRun is set when the caller passed ExpireOptions.DryRun. The
	// RemovedSnapshotIDs and After* fields reflect the projected
	// outcome computed from the staged snapshot list; nothing was
	// committed. ContentionDetected reports whether a foreign writer
	// advanced the table's current snapshot during the planning
	// phase.
	DryRun             bool `json:"dry_run,omitempty"`
	ContentionDetected bool `json:"contention_detected,omitempty"`
}

// Expire removes old snapshots from `ident`'s metadata and commits the
// result via the directory catalog's atomic CAS. The current snapshot
// (the main branch tip) is always retained: ExpireSnapshots only
// touches the parent chain.
//
// The expiration is structurally bounded:
//
//   - I1 (row count) of the current snapshot does not change. Expire
//     does not write data files.
//   - I2 (schema by id) does not change.
//   - The current_snapshot_id and current_schema_id pointers are
//     unchanged. Only the metadata.snapshots[] slice (and its referenced
//     manifest_list / manifest / data files, post-commit) shrink.
//   - Every snapshot id present in the retain set computed by
//     iceberg-go MUST appear in the staged metadata's Snapshots() list.
//
// VerifyExpireConsistency in pkg/safety enforces these. The check is
// mandatory; there is no --force bypass.
//
// On a CAS conflict (another writer beat us to v(N+1).metadata.json)
// or an iceberg-go requirement-validation failure, Expire reloads the
// table and retries up to MaxAttempts times with exponential backoff.
// This is the same retry classifier compact uses
// (janitor.IsRetryableConcurrencyError). The retry budget is small
// because expire is cheap to redo and the only races we can hit are
// foreign-writer commits between our load and our commit.
func Expire(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts ExpireOptions) (*ExpireResult, error) {
	opts.defaults()
	started := time.Now()
	result := &ExpireResult{Identifier: ident}

	// CB8 preflight, mirroring janitor.Compact.
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
			result.DurationMs = time.Since(started).Milliseconds()
			return result, err
		}
	}

	runErr := func() error {
		backoff := opts.InitialBackoff
		for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
			result.Attempts = attempt
			err := expireOnce(ctx, cat, ident, opts, result)
			if err == nil {
				return nil
			}
			if !janitor.IsRetryableConcurrencyError(err) {
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
		return fmt.Errorf("expire failed: exceeded %d concurrency-retry attempts", opts.MaxAttempts)
	}()

	result.DurationMs = time.Since(started).Milliseconds()

	if opts.CircuitBreaker != nil && tableUUID != "" {
		recordErr := opts.CircuitBreaker.RecordOutcome(ctx, tableUUID, runErr)
		if recordErr != nil && runErr == nil {
			return result, recordErr
		}
	}

	if runErr != nil {
		return result, runErr
	}
	return result, nil
}

// expireOnce is one CAS attempt. It loads the table, snapshots the
// "before" state, stages the ExpireSnapshots update, runs the master
// check, and commits.
func expireOnce(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts ExpireOptions, result *ExpireResult) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	// Capture the "before" snapshot list. We need this both to fill
	// out the result and to compute the set of removed snapshot ids
	// after the stage. tbl.Metadata().Snapshots() returns a stable
	// slice we can iterate without locks (it's set at LoadTable
	// time and doesn't mutate under us).
	beforeSnaps := tbl.Metadata().Snapshots()
	result.BeforeSnapshots = len(beforeSnaps)
	if cur := tbl.CurrentSnapshot(); cur != nil {
		result.BeforeSnapshotID = cur.SnapshotID
	}
	result.BeforeFiles, result.BeforeRows = janitor.SnapshotFileStatsFast(ctx, tbl)

	// Stage the expiration. iceberg-go's ExpireSnapshots requires
	// BOTH WithRetainLast AND WithOlderThan to be set when the table's
	// branch refs don't carry their own MinSnapshotsToKeep /
	// MaxSnapshotAgeMs properties — which is the common case.
	// Passing both unconditionally is safer than conditional logic.
	tx := tbl.NewTransaction()
	expOpts := []icebergtable.ExpireSnapshotsOpt{
		icebergtable.WithRetainLast(opts.KeepLast),
		icebergtable.WithOlderThan(opts.KeepWithin),
		icebergtable.WithPostCommit(opts.PostCommitDelete),
	}
	if err := tx.ExpireSnapshots(expOpts...); err != nil {
		return fmt.Errorf("staging expire: %w", err)
	}

	staged, err := tx.StagedTable()
	if err != nil {
		return fmt.Errorf("getting staged table: %w", err)
	}

	// Compute the set of removed snapshot ids by diffing the before
	// list against the staged list. We do this BEFORE the master
	// check (which also runs the diff via its own retain-set
	// invariant) so that the result fields are populated even if the
	// master check fails — useful for debugging.
	stagedSnaps := staged.Metadata().Snapshots()
	stagedIDs := make(map[int64]struct{}, len(stagedSnaps))
	for _, s := range stagedSnaps {
		stagedIDs[s.SnapshotID] = struct{}{}
	}
	result.RemovedSnapshotIDs = result.RemovedSnapshotIDs[:0]
	for _, s := range beforeSnaps {
		if _, kept := stagedIDs[s.SnapshotID]; !kept {
			result.RemovedSnapshotIDs = append(result.RemovedSnapshotIDs, s.SnapshotID)
		}
	}

	// Master check. Different invariants from compact: row count and
	// schema MUST NOT change, current snapshot id MUST NOT change,
	// and every staged snapshot must have been present in the before
	// snapshot list (we don't ADD snapshots in an expire).
	verification, err := safety.VerifyExpireConsistency(ctx, tbl, staged, cat.Props())
	result.Verification = verification
	if err != nil {
		return err
	}

	// Dry-run cut point. Master check passed on the staged snapshot
	// set; the commit would go through next. Stop here, fill in the
	// "after" counts from the staged metadata, and probe for
	// contention by reloading the table. See CompactOptions.DryRun
	// for the rationale.
	if opts.DryRun {
		result.DryRun = true
		stagedMeta := staged.Metadata()
		if cur := stagedMeta.CurrentSnapshot(); cur != nil {
			result.AfterSnapshotID = cur.SnapshotID
		} else {
			result.AfterSnapshotID = result.BeforeSnapshotID
		}
		result.AfterSnapshots = len(stagedMeta.Snapshots())
		// Expire removes snapshots from the parent chain but leaves
		// the current snapshot's file set untouched, so the visible
		// file + row counts are the same as before. Post-commit
		// orphan deletion acts on files that were ONLY referenced by
		// expired snapshots, which does not change the current
		// snapshot's query-visible set.
		result.AfterFiles = result.BeforeFiles
		result.AfterRows = result.BeforeRows
		if probe, perr := cat.LoadTable(ctx, ident); perr == nil {
			if cur := probe.CurrentSnapshot(); cur != nil && cur.SnapshotID != result.BeforeSnapshotID {
				result.ContentionDetected = true
			}
		}
		return nil
	}

	// Commit. Routes through tbl.doCommit → directory catalog
	// CommitTable → atomic CAS write of the new metadata.json. On a
	// CAS conflict the caller's retry loop reloads and tries again.
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return err
	}

	// On a no-op expire (nothing in the parent chain was old enough)
	// iceberg-go produces zero updates and Commit returns the same
	// *Table back. The "after" stats then equal the "before" stats,
	// which is the right answer to report.
	if cur := newTbl.CurrentSnapshot(); cur != nil {
		result.AfterSnapshotID = cur.SnapshotID
	}
	result.AfterSnapshots = len(newTbl.Metadata().Snapshots())
	result.AfterFiles, result.AfterRows = janitor.SnapshotFileStatsFast(ctx, newTbl)

	// Defense in depth: the master check already asserts row count
	// preservation, but we re-assert it here against the post-commit
	// table because the master check ran against the STAGED table.
	// If the directory catalog ever introduced a code path that
	// re-materialized snapshot ids during commit, this would catch
	// it before we report success.
	if result.AfterRows != result.BeforeRows {
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", result.BeforeRows, result.AfterRows)
	}
	return nil
}
