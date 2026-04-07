// Package classify auto-detects an Iceberg table's workload class
// (streaming / batch / slow_changing / dormant) from its observed
// commit pattern over the last 24 hours. The classifier excludes
// janitor-attributed snapshots so the janitor's own activity does not
// distort the classification.
//
// The four classes drive per-class default thresholds (cooldown,
// trigger thresholds, write buffer, daily byte budget, expire
// frequency) that the orchestrator consults when scheduling
// maintenance. See ClassDefaults for the values.
//
// The classifier is one of the prerequisites for the middle-path
// on-commit compaction design (go/MIDDLE_PATH.md): on-commit dispatch
// only fires for tables classified as `streaming`. Other classes use
// the polling cadence appropriate to their commit rate.
package classify

import (
	"time"

	icebergtable "github.com/apache/iceberg-go/table"
)

// WorkloadClass is the auto-detected workload pattern of a table.
type WorkloadClass string

const (
	// ClassStreaming: foreign commit rate > 6/h (one per 10 min or
	// faster), files added per commit small. Typical example: Flink
	// or Kafka Connect Iceberg sink with sub-minute checkpoints.
	ClassStreaming WorkloadClass = "streaming"

	// ClassBatch: foreign commit rate 0.04 to 6/h (once a day to once
	// per 10 min). Typical example: nightly Spark job, hourly Airflow
	// task, dbt model materialization.
	ClassBatch WorkloadClass = "batch"

	// ClassSlowChanging: foreign commit rate < 0.04/h (less than once
	// a day). Typical example: reference / dimension tables.
	ClassSlowChanging WorkloadClass = "slow_changing"

	// ClassDormant: no foreign commits in 7+ days. The janitor leaves
	// these alone aside from periodic health checks.
	ClassDormant WorkloadClass = "dormant"
)

// Thresholds is the set of per-class knobs the orchestrator reads when
// scheduling maintenance for a table. Per the design plan (decision
// #4a), every threshold is a per-class default rather than a global
// constant — streaming tables get aggressive 5-minute cadence, batch
// tables stay on hourly, dormant tables on weekly.
type Thresholds struct {
	// MinRecompactInterval is CB1 (cooldown). Minimum time between
	// successive compactions on the same table. Streaming: 5 min,
	// batch: 1 h, slow_changing: 6 h, dormant: 7 d.
	MinRecompactInterval time.Duration

	// SmallFileTrigger is the file count threshold above which
	// compaction is considered. Streaming: 50, batch: 200,
	// slow_changing: 500, dormant: irrelevant (no auto compaction).
	SmallFileTrigger int

	// TimeTrigger is the maximum age of an unprocessed file before
	// compaction is forced regardless of count. Streaming: 15 min,
	// batch: 6 h, slow_changing: 24 h, dormant: irrelevant.
	TimeTrigger time.Duration

	// WriteBufferSeconds is the writer-fight protection window (only
	// compact files older than this). Streaming: 60 s, batch: n/a,
	// slow_changing: n/a, dormant: n/a.
	WriteBufferSeconds int

	// DailyByteBudgetMultiple is CB7. Multiple of current table size
	// the janitor is allowed to rewrite per day. Streaming: unlimited
	// (rolling-window basis), batch: 5x, slow_changing: 1x.
	DailyByteBudgetMultiple float64

	// PartitionScope: how the planner picks files within a table.
	// Streaming: "active partition only" (incremental + time-windowed),
	// batch: "active + recently-written", slow_changing: "full table".
	PartitionScope PartitionScope

	// TierPreference biases the runtime tier. Streaming: warm
	// (Lambda/Knative for cold-start economics), batch: adaptive,
	// slow_changing: task tier (no urgency).
	TierPreference TierPreference

	// Priority is the scheduler weight. Streaming: high (query
	// freshness matters), batch: medium, slow_changing: low.
	Priority int

	// ExpireFrequency is how often expire-snapshots runs.
	ExpireFrequency time.Duration

	// ManifestRewriteTrigger fires manifest rewrite when the count
	// exceeds this threshold.
	ManifestRewriteTrigger int
}

type PartitionScope string

const (
	PartitionScopeActiveOnly        PartitionScope = "active_only"
	PartitionScopeActiveAndRecent   PartitionScope = "active_and_recent"
	PartitionScopeFullTable         PartitionScope = "full_table"
)

type TierPreference string

const (
	TierWarm     TierPreference = "warm"
	TierAdaptive TierPreference = "adaptive"
	TierTask     TierPreference = "task"
)

// ClassDefaults are the per-class default thresholds. Operators can
// override individual fields per table via _janitor/control/policy/
// but the defaults must serve 99% of tables without intervention —
// that's the operator-zero-touch principle (decision #28).
var ClassDefaults = map[WorkloadClass]Thresholds{
	ClassStreaming: {
		MinRecompactInterval:    5 * time.Minute,
		SmallFileTrigger:        50,
		TimeTrigger:             15 * time.Minute,
		WriteBufferSeconds:      60,
		DailyByteBudgetMultiple: -1, // unlimited within rolling window
		PartitionScope:          PartitionScopeActiveOnly,
		TierPreference:          TierWarm,
		Priority:                10,
		ExpireFrequency:         4 * time.Hour,
		ManifestRewriteTrigger:  200,
	},
	ClassBatch: {
		MinRecompactInterval:    1 * time.Hour,
		SmallFileTrigger:        200,
		TimeTrigger:             6 * time.Hour,
		WriteBufferSeconds:      0,
		DailyByteBudgetMultiple: 5.0,
		PartitionScope:          PartitionScopeActiveAndRecent,
		TierPreference:          TierAdaptive,
		Priority:                5,
		ExpireFrequency:         24 * time.Hour,
		ManifestRewriteTrigger:  1000,
	},
	ClassSlowChanging: {
		MinRecompactInterval:    6 * time.Hour,
		SmallFileTrigger:        500,
		TimeTrigger:             24 * time.Hour,
		WriteBufferSeconds:      0,
		DailyByteBudgetMultiple: 1.0,
		PartitionScope:          PartitionScopeFullTable,
		TierPreference:          TierTask,
		Priority:                1,
		ExpireFrequency:         7 * 24 * time.Hour,
		ManifestRewriteTrigger:  2000,
	},
	ClassDormant: {
		// No auto-maintenance for dormant tables. The orchestrator
		// reads this class and skips. Operators can force via the
		// CLI for one-off cleanup.
		MinRecompactInterval:    365 * 24 * time.Hour,
		SmallFileTrigger:        1 << 30,
		TimeTrigger:             365 * 24 * time.Hour,
		WriteBufferSeconds:      0,
		DailyByteBudgetMultiple: 0,
		PartitionScope:          PartitionScopeFullTable,
		TierPreference:          TierTask,
		Priority:                0,
		ExpireFrequency:         30 * 24 * time.Hour,
		ManifestRewriteTrigger:  10000,
	},
}

// Result is the structured output of Classify. Includes the inputs the
// classifier used so the decision is auditable from the analyzer's
// HealthReport (and ultimately from _janitor/state/<table>.json).
type Result struct {
	Class                  WorkloadClass `json:"class"`
	ForeignCommitsLast24h  int           `json:"foreign_commits_last_24h"`
	ForeignCommitsLast7d   int           `json:"foreign_commits_last_7d"`
	LastForeignCommitAt    time.Time     `json:"last_foreign_commit_at,omitempty"`
	JanitorCommitsExcluded int           `json:"janitor_commits_excluded"`
}

// JanitorRunIDKey is the snapshot summary property the janitor sets
// on every commit it produces. The classifier uses this key to
// distinguish foreign commits from janitor-attributed ones.
//
// Must match what pkg/janitor.Compact writes when it commits a new
// snapshot. Today the master check writes a Verification record but
// not a separate run-id property; that change lands when the
// orchestrator wires up _janitor/results/<run_id>.json. For now, the
// classifier treats every commit as foreign — which is the safe
// default (it never under-counts foreign activity).
const JanitorRunIDKey = "janitor.run_id"

// Classify reads `tbl`'s snapshot history and returns the workload
// class along with the inputs used to make the decision.
//
// Cost: zero extra I/O. The snapshot history is already in the loaded
// metadata.json that the analyzer reads.
func Classify(tbl *icebergtable.Table) Result {
	return classifyAt(tbl, time.Now())
}

// classifyAt is the testable form: pass an explicit "now" so unit
// tests can fix the clock and feed in synthetic snapshot histories.
func classifyAt(tbl *icebergtable.Table, now time.Time) Result {
	if tbl == nil {
		return Result{Class: ClassDormant}
	}
	snapshots := tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		return Result{Class: ClassDormant}
	}

	cutoff24h := now.Add(-24 * time.Hour)
	cutoff7d := now.Add(-7 * 24 * time.Hour)

	r := Result{}
	var lastForeignAt time.Time

	for i := range snapshots {
		snap := snapshots[i]
		ts := time.UnixMilli(snap.TimestampMs)

		isJanitor := false
		if snap.Summary != nil {
			if _, ok := snap.Summary.Properties[JanitorRunIDKey]; ok {
				isJanitor = true
			}
		}
		if isJanitor {
			r.JanitorCommitsExcluded++
			continue
		}
		if ts.After(cutoff7d) {
			r.ForeignCommitsLast7d++
		}
		if ts.After(cutoff24h) {
			r.ForeignCommitsLast24h++
		}
		if ts.After(lastForeignAt) {
			lastForeignAt = ts
		}
	}
	r.LastForeignCommitAt = lastForeignAt

	r.Class = classifyFromCounts(r, now)
	return r
}

// classifyFromCounts maps the (commits-in-window, last-commit-time)
// signal into a workload class per the design plan thresholds:
//
//	streaming:    foreign commit rate > 6/h over the last 24h
//	batch:        0.04 < commit rate <= 6/h
//	slow_changing: 0 < commit rate <= 0.04/h
//	dormant:      no commits in last 7d
func classifyFromCounts(r Result, now time.Time) WorkloadClass {
	if r.ForeignCommitsLast7d == 0 {
		return ClassDormant
	}
	// commit rate per hour over the last 24h
	rate := float64(r.ForeignCommitsLast24h) / 24.0
	switch {
	case rate > 6.0:
		return ClassStreaming
	case rate > 0.04:
		return ClassBatch
	default:
		return ClassSlowChanging
	}
}

// DefaultsFor returns the per-class default thresholds for `class`.
// Returns the dormant defaults for an unknown class.
func DefaultsFor(class WorkloadClass) Thresholds {
	if t, ok := ClassDefaults[class]; ok {
		return t
	}
	return ClassDefaults[ClassDormant]
}
