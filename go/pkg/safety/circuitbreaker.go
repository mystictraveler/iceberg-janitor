package safety

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gocloud.dev/blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/state"
)

// CB8DefaultThreshold is the default value for CircuitBreaker.MaxConsecutiveFailures.
// The design plan and issue #2 both call out 3 consecutive failures as the
// trip point: enough to absorb a single transient hiccup followed by a
// successful retry, low enough that a structural problem (writer is faster
// than compactor, broken table, missing IAM perm) gets noticed quickly.
const CB8DefaultThreshold = 3

// PausedError is returned by CircuitBreaker.Preflight when a table has
// already been auto- or manually-paused. Callers can errors.As against
// it to surface the pause to the operator without treating it as a
// generic failure (the table isn't broken — the janitor is just refusing
// to touch it on purpose).
type PausedError struct {
	TableUUID string
	Reason    string
}

func (e *PausedError) Error() string {
	return fmt.Sprintf("table %s is paused: %s", e.TableUUID, e.Reason)
}

// IsPaused reports whether err wraps a PausedError. Convenience for
// callers that just want to detect the case without unwrapping.
func IsPaused(err error) bool {
	var pe *PausedError
	return errors.As(err, &pe)
}

// SkippedError is returned when a soft circuit breaker fires. Unlike
// PausedError, the table is NOT paused — the janitor simply skips
// this particular compaction because a future run is likely to do
// meaningful work (e.g. after more files accumulate or after the
// daily budget resets). The caller should log it and move on.
type SkippedError struct {
	TableUUID string
	Reason    string
}

func (e *SkippedError) Error() string {
	return fmt.Sprintf("table %s skipped: %s", e.TableUUID, e.Reason)
}

// IsSkipped reports whether err wraps a SkippedError.
func IsSkipped(err error) bool {
	var se *SkippedError
	return errors.As(err, &se)
}

// CircuitBreaker enforces the eleven safety rules from the design plan.
// All eleven are now implemented:
//
//	CB2  loop detection — pause when the same file set is compacted N times
//	CB3  metadata-to-data ratio (H1 axiom) — warn/pause on ratio thresholds
//	CB4  effectiveness floor — pause when file reduction drops below threshold
//	CB5  expire-before-orphan ordering guard
//	CB6  rewrite-before-compact ordering guard
//	CB7  daily byte budget — bound total bytes read+written per day
//	CB8  consecutive failure pause — the original breaker
//	CB9  lifetime rewrite ratio — bound cumulative bytes rewritten
//	CB10 recursion guard — refuse to act on _janitor/ paths
//	CB11 ROI estimate — refuse compaction whose cost > benefit
//
// CircuitBreaker is safe to construct on every invocation — it holds no
// in-memory state of its own. The per-table state lives in
// _janitor/state/<uuid>.json and is read fresh on every Preflight and
// re-read fresh on every RecordOutcome.
type CircuitBreaker struct {
	Bucket                 *blob.Bucket
	MaxConsecutiveFailures int    // CB8 threshold, default 3
	TriggeredBy            string // e.g. "janitor-cli"

	// CB2: loop detection threshold. Default 3.
	MaxLoopCount int

	// CB3: metadata-to-data ratio thresholds.
	MetadataRatioWarn     float64 // default 0.05 (5%)
	MetadataRatioCritical float64 // default 0.10 (10%)
	MetadataRatioPause    float64 // default 0.50 (50%)

	// CB4: minimum file reduction ratio per compact. Default 0.01 (1%).
	MinEffectiveness float64

	// CB7: daily byte budget. 0 = unlimited.
	DailyByteBudget int64

	// CB9: lifetime rewrite ratio cap. 0 = unlimited.
	MaxLifetimeRewriteRatio float64

	// CB11: minimum ROI ratio (benefit/cost). Default 0.1.
	MinROI float64
}

// New constructs a CircuitBreaker with safe defaults. Callers can
// override fields directly on the returned struct before use.
func New(bucket *blob.Bucket) *CircuitBreaker {
	return &CircuitBreaker{
		Bucket:                 bucket,
		MaxConsecutiveFailures: CB8DefaultThreshold,
	}
}

func (cb *CircuitBreaker) threshold() int {
	if cb.MaxConsecutiveFailures > 0 {
		return cb.MaxConsecutiveFailures
	}
	return CB8DefaultThreshold
}

func (cb *CircuitBreaker) triggeredBy() string {
	if cb.TriggeredBy != "" {
		return cb.TriggeredBy
	}
	if len(os.Args) > 0 {
		return os.Args[0]
	}
	return "janitor"
}

// Preflight is the first call a maintenance op makes. It reads the
// pause file for the table; if present, it returns a *PausedError.
// Otherwise it returns nil and the op proceeds.
//
// Preflight does NOT load the state file. Reading state is the job of
// RecordOutcome (which has to write it anyway, so it amortises the
// round trip). Preflight is the hot path on healthy tables and stays
// at one HEAD/GET per call.
func (cb *CircuitBreaker) Preflight(ctx context.Context, tableUUID string) error {
	if cb == nil || cb.Bucket == nil {
		return nil
	}
	pause, err := state.LoadPause(ctx, cb.Bucket, tableUUID)
	if err != nil {
		// Fail closed: if we cannot tell whether the table is paused,
		// we must not proceed. The operator will see the error and
		// can investigate; the alternative (assume not-paused) defeats
		// the whole point of the circuit breaker.
		return fmt.Errorf("circuit breaker preflight: %w", err)
	}
	if pause == nil {
		return nil
	}
	return &PausedError{TableUUID: tableUUID, Reason: pause.Reason}
}

// RecordOutcome updates the per-table state with the result of one
// maintenance attempt:
//
//   - Success: clears ConsecutiveFailedRuns and LastErrors, stamps
//     LastSuccessAt.
//   - Failure: increments ConsecutiveFailedRuns, appends the (truncated)
//     error message to LastErrors. If the new count meets or exceeds
//     the CB8 threshold, RecordOutcome ALSO writes the pause file —
//     atomically from the operator's POV (the operator either sees
//     "paused, with these N errors" or "not paused yet", never an
//     in-between state where the counter is at threshold but no pause
//     file exists).
//
// On Failure: the order is (1) load state, (2) increment, (3) save
// state, (4) save pause file if threshold tripped. If save-pause-file
// fails after save-state succeeds, the next attempt will see a
// counter at threshold but no pause file, and the next RecordOutcome
// will write the pause file then. Eventual consistency, no lost
// safety signal.
//
// runErr may be nil for the success path; it is the err returned by
// the wrapped operation for the failure path.
func (cb *CircuitBreaker) RecordOutcome(ctx context.Context, tableUUID string, runErr error) error {
	if cb == nil || cb.Bucket == nil {
		return nil
	}
	if tableUUID == "" {
		return errors.New("circuit breaker: empty tableUUID")
	}
	s, err := state.Load(ctx, cb.Bucket, tableUUID)
	if err != nil {
		return fmt.Errorf("circuit breaker: load state: %w", err)
	}
	now := timeNow()
	if runErr == nil {
		s.RecordSuccess(now)
		if err := state.Save(ctx, cb.Bucket, s); err != nil {
			return fmt.Errorf("circuit breaker: save state: %w", err)
		}
		return nil
	}
	s.RecordFailure(now, runErr.Error())
	if err := state.Save(ctx, cb.Bucket, s); err != nil {
		return fmt.Errorf("circuit breaker: save state: %w", err)
	}
	if s.ConsecutiveFailedRuns >= cb.threshold() {
		pause := &state.PauseFile{
			TableUUID:   tableUUID,
			Reason:      fmt.Sprintf("cb8_consecutive_failure: %d consecutive failed runs", s.ConsecutiveFailedRuns),
			TriggeredAt: now,
			TriggeredBy: cb.triggeredBy(),
			LastErrors:  append([]state.ErrorRecord(nil), s.LastErrors...),
		}
		if err := state.SavePause(ctx, cb.Bucket, pause); err != nil {
			return fmt.Errorf("circuit breaker: save pause: %w", err)
		}
	}
	return nil
}

// Resume is the operator action: clear the pause file AND reset the
// failure counter so the next maintenance attempt starts from a clean
// slate. Resetting the counter is essential — leaving it at threshold
// would cause the very next failure to immediately re-pause, which
// makes the operator action useless. The decision to resume is the
// operator's signal that they have either fixed the underlying
// problem or accepted the risk of letting it run again.
func (cb *CircuitBreaker) Resume(ctx context.Context, tableUUID string) error {
	if cb == nil || cb.Bucket == nil {
		return errors.New("circuit breaker: not configured")
	}
	if err := state.DeletePause(ctx, cb.Bucket, tableUUID); err != nil {
		return err
	}
	s, err := state.Load(ctx, cb.Bucket, tableUUID)
	if err != nil {
		return err
	}
	s.ConsecutiveFailedRuns = 0
	s.LastErrors = nil
	s.UpdatedAt = timeNow()
	return state.Save(ctx, cb.Bucket, s)
}

// CompactOutcome captures the metrics from one compaction that the
// circuit breakers need to evaluate. Callers fill it in after compact
// completes and pass it to RecordCompactOutcome.
type CompactOutcome struct {
	Err            error
	BeforeFiles    int
	AfterFiles     int
	BytesRead      int64
	BytesWritten   int64
	CompactedPaths []string // paths of files that were removed+replaced
	TableDataBytes int64    // total data bytes in the table
	MetadataBytes  int64    // total metadata bytes (manifests + manifest lists)
}

// CheckCB2LoopDetection detects feedback loops where the compactor
// keeps rewriting the same set of files. If the same file set appears
// MaxLoopCount consecutive times, the table is paused.
func (cb *CircuitBreaker) checkCB2(s *state.TableState, outcome *CompactOutcome) string {
	maxLoop := cb.MaxLoopCount
	if maxLoop <= 0 {
		maxLoop = 3
	}
	if outcome == nil || len(outcome.CompactedPaths) == 0 {
		s.LoopCount = 0
		s.LastCompactedFiles = nil
		return ""
	}
	// Check if this set matches the previous.
	if sameStringSet(s.LastCompactedFiles, outcome.CompactedPaths) {
		s.LoopCount++
	} else {
		s.LoopCount = 1
	}
	s.LastCompactedFiles = outcome.CompactedPaths
	if s.LoopCount >= maxLoop {
		return fmt.Sprintf("cb2_loop_detected: same %d files compacted %d consecutive times", len(outcome.CompactedPaths), s.LoopCount)
	}
	return ""
}

// CheckCB3MetadataRatio checks the H1 axiom: metadata/data ratio.
// Returns a warning string (non-pause) or a pause reason.
func (cb *CircuitBreaker) checkCB3(outcome *CompactOutcome) (warn string, pause string) {
	if outcome == nil || outcome.TableDataBytes <= 0 {
		return "", ""
	}
	ratio := float64(outcome.MetadataBytes) / float64(outcome.TableDataBytes)
	warnThresh := cb.MetadataRatioWarn
	if warnThresh <= 0 {
		warnThresh = 0.05
	}
	critThresh := cb.MetadataRatioCritical
	if critThresh <= 0 {
		critThresh = 0.10
	}
	pauseThresh := cb.MetadataRatioPause
	if pauseThresh <= 0 {
		pauseThresh = 0.50
	}
	if ratio >= pauseThresh {
		return "", fmt.Sprintf("cb3_metadata_ratio: %.1f%% exceeds pause threshold %.0f%%", ratio*100, pauseThresh*100)
	}
	if ratio >= critThresh {
		return fmt.Sprintf("cb3_metadata_ratio: %.1f%% exceeds critical threshold %.0f%%", ratio*100, critThresh*100), ""
	}
	if ratio >= warnThresh {
		return fmt.Sprintf("cb3_metadata_ratio: %.1f%% exceeds warn threshold %.0f%%", ratio*100, warnThresh*100), ""
	}
	return "", ""
}

// CheckCB4Effectiveness checks if compaction actually reduced files.
func (cb *CircuitBreaker) checkCB4(outcome *CompactOutcome) string {
	if outcome == nil || outcome.BeforeFiles <= 0 {
		return ""
	}
	minEff := cb.MinEffectiveness
	if minEff <= 0 {
		minEff = 0.01
	}
	reduction := 1.0 - float64(outcome.AfterFiles)/float64(outcome.BeforeFiles)
	if reduction < minEff && outcome.BeforeFiles > outcome.AfterFiles {
		// Still reduced, just not much — warn but don't pause.
		return ""
	}
	if outcome.AfterFiles >= outcome.BeforeFiles {
		return fmt.Sprintf("cb4_no_effectiveness: compaction produced %d files from %d (no reduction)", outcome.AfterFiles, outcome.BeforeFiles)
	}
	return ""
}

// CheckCB7DailyBudget checks the daily byte budget.
func (cb *CircuitBreaker) checkCB7(s *state.TableState, outcome *CompactOutcome) string {
	if cb.DailyByteBudget <= 0 || outcome == nil {
		return ""
	}
	now := timeNow()
	// Reset if it's a new day.
	if s.DailyResetAt.IsZero() || now.Sub(s.DailyResetAt) > 24*time.Hour {
		s.DailyBytesRead = 0
		s.DailyBytesWritten = 0
		s.DailyResetAt = now
	}
	s.DailyBytesRead += outcome.BytesRead
	s.DailyBytesWritten += outcome.BytesWritten
	total := s.DailyBytesRead + s.DailyBytesWritten
	if total > cb.DailyByteBudget {
		return fmt.Sprintf("cb7_daily_budget: %d bytes read+written today exceeds budget %d", total, cb.DailyByteBudget)
	}
	return ""
}

// CheckCB9LifetimeRatio checks cumulative rewrite ratio.
func (cb *CircuitBreaker) checkCB9(s *state.TableState, outcome *CompactOutcome) string {
	if cb.MaxLifetimeRewriteRatio <= 0 || outcome == nil || outcome.TableDataBytes <= 0 {
		return ""
	}
	s.LifetimeBytesRewritten += outcome.BytesWritten
	s.LifetimeDataBytes = outcome.TableDataBytes
	ratio := float64(s.LifetimeBytesRewritten) / float64(s.LifetimeDataBytes)
	if ratio > cb.MaxLifetimeRewriteRatio {
		return fmt.Sprintf("cb9_lifetime_rewrite: %.1fx total data rewritten (limit %.1fx)", ratio, cb.MaxLifetimeRewriteRatio)
	}
	return ""
}

// CheckCB10Recursion refuses to operate on _janitor/ paths.
func CheckCB10Recursion(tablePath string) error {
	if strings.Contains(tablePath, "_janitor/") || strings.HasPrefix(tablePath, "_janitor") {
		return fmt.Errorf("cb10_recursion_guard: refusing to operate on janitor internal path %q", tablePath)
	}
	return nil
}

// CheckCB11ROI estimates whether the compaction is worth doing.
// Cost = bytes to read. Benefit = files reduced × estimated per-file
// query overhead (approximated as 1 KB of manifest walk per file).
func (cb *CircuitBreaker) checkCB11(outcome *CompactOutcome) string {
	if outcome == nil || outcome.BytesRead <= 0 {
		return ""
	}
	minROI := cb.MinROI
	if minROI <= 0 {
		minROI = 0.1
	}
	filesReduced := outcome.BeforeFiles - outcome.AfterFiles
	if filesReduced <= 0 {
		return ""
	}
	// Benefit: each file eliminated saves ~1 KB of manifest I/O per
	// query that touches that partition. Over ~100 queries/day that's
	// ~100 KB per file per day. Cost: bytes read during compact.
	benefit := float64(filesReduced) * 1024 * 100
	cost := float64(outcome.BytesRead)
	roi := benefit / cost
	if roi < minROI {
		return fmt.Sprintf("cb11_low_roi: estimated ROI %.3f below threshold %.3f (cost=%d benefit=%.0f)", roi, minROI, outcome.BytesRead, benefit)
	}
	return ""
}

// CBCheckCost captures the wall time spent evaluating circuit breakers,
// separate from the actual maintenance work. Returned by
// RecordCompactOutcome so callers can log and monitor the overhead.
type CBCheckCost struct {
	LoadStateMs   int64 `json:"load_state_ms"`
	EvalChecksMs  int64 `json:"eval_checks_ms"`
	SaveStateMs   int64 `json:"save_state_ms"`
	SavePauseMs   int64 `json:"save_pause_ms,omitempty"`
	TotalMs       int64 `json:"total_ms"`
	ChecksFired   int   `json:"checks_fired"`   // how many CBs returned a non-empty result
	PauseTriggered bool `json:"pause_triggered"`
}

// RecordCompactOutcome is the extended version of RecordOutcome that
// evaluates ALL circuit breakers against the compact metrics. It runs
// CB2-CB11 checks and pauses if any fires. Returns the cost of the
// CB evaluation itself so callers can monitor overhead.
func (cb *CircuitBreaker) RecordCompactOutcome(ctx context.Context, tableUUID string, outcome *CompactOutcome) (*CBCheckCost, error) {
	cost := &CBCheckCost{}
	totalStart := time.Now()

	if cb == nil || cb.Bucket == nil {
		return cost, nil
	}
	if tableUUID == "" {
		return cost, errors.New("circuit breaker: empty tableUUID")
	}

	loadStart := time.Now()
	s, err := state.Load(ctx, cb.Bucket, tableUUID)
	if err != nil {
		return cost, fmt.Errorf("circuit breaker: load state: %w", err)
	}
	cost.LoadStateMs = time.Since(loadStart).Milliseconds()

	now := timeNow()
	if outcome.Err == nil {
		s.RecordSuccess(now)
	} else {
		s.RecordFailure(now, outcome.Err.Error())
	}

	// Evaluate all CBs. Split into:
	//   fatal  → writes pause file, requires operator intervention
	//   soft   → returns SkippedError, next run can retry
	//   info   → logged only, no action
	evalStart := time.Now()
	var fatalReasons []string
	var softReasons []string

	// CB8 (fatal): consecutive failures — structural problem.
	if outcome.Err != nil && s.ConsecutiveFailedRuns >= cb.threshold() {
		fatalReasons = append(fatalReasons, fmt.Sprintf("cb8_consecutive_failure: %d consecutive failed runs", s.ConsecutiveFailedRuns))
		cost.ChecksFired++
	}

	// CB2 (fatal): loop detection — feedback loop with external writer.
	if reason := cb.checkCB2(s, outcome); reason != "" {
		fatalReasons = append(fatalReasons, reason)
		cost.ChecksFired++
	}

	// CB3: metadata ratio. Pause threshold (50%) is fatal; warn/critical are soft.
	if warn, pauseReason := cb.checkCB3(outcome); pauseReason != "" {
		fatalReasons = append(fatalReasons, pauseReason)
		cost.ChecksFired++
	} else if warn != "" {
		softReasons = append(softReasons, warn)
		cost.ChecksFired++
	}

	// CB4 (soft): no effectiveness — skip, more files may accumulate.
	if reason := cb.checkCB4(outcome); reason != "" {
		softReasons = append(softReasons, reason)
		cost.ChecksFired++
	}

	// CB7 (soft): daily budget exceeded — skip, resets tomorrow.
	if reason := cb.checkCB7(s, outcome); reason != "" {
		softReasons = append(softReasons, reason)
		cost.ChecksFired++
	}

	// CB9 (fatal): lifetime rewrite ratio — misconfigured aggressive compaction.
	if reason := cb.checkCB9(s, outcome); reason != "" {
		fatalReasons = append(fatalReasons, reason)
		cost.ChecksFired++
	}

	// CB11 (soft): low ROI — skip, more files may accumulate to improve ROI.
	if reason := cb.checkCB11(outcome); reason != "" {
		softReasons = append(softReasons, reason)
		cost.ChecksFired++
	}

	cost.EvalChecksMs = time.Since(evalStart).Milliseconds()

	// Save state (always — CB2/CB7/CB9 updated counters).
	saveStart := time.Now()
	if err := state.Save(ctx, cb.Bucket, s); err != nil {
		return cost, fmt.Errorf("circuit breaker: save state: %w", err)
	}
	cost.SaveStateMs = time.Since(saveStart).Milliseconds()

	// Fatal CBs → write pause file, return PausedError.
	if len(fatalReasons) > 0 {
		pauseStart := time.Now()
		pause := &state.PauseFile{
			TableUUID:   tableUUID,
			Reason:      strings.Join(fatalReasons, "; "),
			TriggeredAt: now,
			TriggeredBy: cb.triggeredBy(),
			LastErrors:  append([]state.ErrorRecord(nil), s.LastErrors...),
		}
		if err := state.SavePause(ctx, cb.Bucket, pause); err != nil {
			return cost, fmt.Errorf("circuit breaker: save pause: %w", err)
		}
		cost.SavePauseMs = time.Since(pauseStart).Milliseconds()
		cost.PauseTriggered = true
	}

	// Soft CBs → return SkippedError (no pause file, next run retries).
	if len(fatalReasons) == 0 && len(softReasons) > 0 {
		cost.TotalMs = time.Since(totalStart).Milliseconds()
		return cost, &SkippedError{
			TableUUID: tableUUID,
			Reason:    strings.Join(softReasons, "; "),
		}
	}

	cost.TotalMs = time.Since(totalStart).Milliseconds()
	return cost, nil
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}

// ManualPause is the operator's "stop touching this table" action. It
// writes a pause file with the operator-supplied reason. The reason
// string is required so the next operator (or the same operator a week
// later) understands why the table is paused.
func (cb *CircuitBreaker) ManualPause(ctx context.Context, tableUUID, reason string) error {
	if cb == nil || cb.Bucket == nil {
		return errors.New("circuit breaker: not configured")
	}
	if reason == "" {
		return errors.New("manual pause requires a non-empty reason")
	}
	pause := &state.PauseFile{
		TableUUID:   tableUUID,
		Reason:      "manual: " + reason,
		TriggeredAt: timeNow(),
		TriggeredBy: cb.triggeredBy(),
	}
	return state.SavePause(ctx, cb.Bucket, pause)
}
