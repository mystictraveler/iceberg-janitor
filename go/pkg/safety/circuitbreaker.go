package safety

import (
	"context"
	"errors"
	"fmt"
	"os"
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

// CB1DefaultCooldown is the default value for CircuitBreaker.CooldownDuration.
// Five minutes is the universal "back off" primitive: long enough that a
// freshly-resumed table (or one where a writer is committing faster than the
// compactor) doesn't immediately re-burn its CB8 retry budget, short enough
// that a healthy table on a quiet bench still gets touched within a single
// AutoCompact loop iteration. See design plan CB1.
const CB1DefaultCooldown = 5 * time.Minute

// CooldownError is returned by CircuitBreaker.Preflight when a table was
// acted on (success OR failure) less than CooldownDuration ago. Unlike
// PausedError, this is a SOFT skip that auto-clears when the cooldown
// elapses — no operator action is required. Callers can errors.As
// against it to surface the cooldown distinctly from a genuine failure.
type CooldownError struct {
	TableUUID         string
	RemainingDuration time.Duration
}

func (e *CooldownError) Error() string {
	return fmt.Sprintf("table %s is cooling down: %s remaining", e.TableUUID, e.RemainingDuration.Round(time.Second))
}

// IsCooldown reports whether err wraps a CooldownError. Convenience for
// callers that just want to detect the case without unwrapping.
func IsCooldown(err error) bool {
	var ce *CooldownError
	return errors.As(err, &ce)
}

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

// CircuitBreaker enforces the eleven safety rules from the design plan.
// Today only CB8 (consecutive failure pause) is implemented; the other
// ten will land alongside the maintenance ops they protect, sharing
// this type's state-load / state-save plumbing.
//
// CircuitBreaker is safe to construct on every invocation — it holds no
// in-memory state of its own. The per-table state lives in
// _janitor/state/<uuid>.json and is read fresh on every Preflight and
// re-read fresh on every RecordOutcome (so two RecordOutcome calls in
// the same process do not race against each other on a stale in-memory
// copy of the counter; the lease primitive eventually excludes
// cross-process races).
type CircuitBreaker struct {
	Bucket                 *blob.Bucket
	MaxConsecutiveFailures int
	// CooldownDuration is the CB1 per-table cooldown window. If the
	// table's LastRunAt is less than this old, Preflight returns a
	// *CooldownError and the caller skips the attempt. Zero means use
	// CB1DefaultCooldown. See CB1DefaultCooldown for rationale.
	CooldownDuration time.Duration
	// TriggeredBy is the identifier the breaker stamps into pause
	// files it writes (e.g. "janitor-cli", "janitor-server",
	// "janitor-lambda"). Defaults to os.Args[0] if empty.
	TriggeredBy string
}

// New constructs a CircuitBreaker with safe defaults. Callers can
// override fields directly on the returned struct before use.
func New(bucket *blob.Bucket) *CircuitBreaker {
	return &CircuitBreaker{
		Bucket:                 bucket,
		MaxConsecutiveFailures: CB8DefaultThreshold,
		CooldownDuration:       CB1DefaultCooldown,
	}
}

func (cb *CircuitBreaker) threshold() int {
	if cb.MaxConsecutiveFailures > 0 {
		return cb.MaxConsecutiveFailures
	}
	return CB8DefaultThreshold
}

func (cb *CircuitBreaker) cooldown() time.Duration {
	if cb.CooldownDuration > 0 {
		return cb.CooldownDuration
	}
	return CB1DefaultCooldown
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

// Preflight is the first call a maintenance op makes. It runs every
// check that can refuse an attempt before any work is done:
//
//  1. Pause file (CB8, or operator-initiated ManualPause). If present,
//     returns *PausedError.
//  2. Per-table cooldown (CB1). If the table's LastRunAt is less than
//     CooldownDuration ago, returns *CooldownError.
//
// Pause wins over cooldown: if a table is paused, the cooldown is
// irrelevant — the operator has explicitly said "stop touching this"
// and surfacing a cooldown message would be misleading.
//
// Cost: Preflight is the hot path on healthy tables. Before CB1 it did
// ONE GET (the pause file). With CB1 it now does TWO (pause file +
// state file). That's acceptable because the state file is small
// (~200 bytes) and the lookups are parallelizable by the object store,
// but note it for future breakers — adding a third file-per-Preflight
// should prompt a batching redesign.
//
// A brand-new table (LastRunAt is the zero value, meaning no state
// file exists or the state file has never been stamped) is NOT
// subject to the cooldown — first runs must not be blocked.
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
	if pause != nil {
		return &PausedError{TableUUID: tableUUID, Reason: pause.Reason}
	}
	// CB1 cooldown check. Load the state file to find LastRunAt. Same
	// fail-closed rationale as above: if we cannot read state, we
	// cannot prove the cooldown has elapsed, so we refuse.
	s, err := state.Load(ctx, cb.Bucket, tableUUID)
	if err != nil {
		return fmt.Errorf("circuit breaker preflight: %w", err)
	}
	if s.LastRunAt.IsZero() {
		// Brand-new table: no prior attempt, no cooldown applies.
		return nil
	}
	elapsed := timeNow().Sub(s.LastRunAt)
	if elapsed < cb.cooldown() {
		return &CooldownError{
			TableUUID:         tableUUID,
			RemainingDuration: cb.cooldown() - elapsed,
		}
	}
	return nil
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
	// Clear LastRunAt too. Leaving it stamped would cause the next
	// Preflight to immediately re-skip with CB1 cooldown, which makes
	// the Resume action useless for the same reason not resetting
	// ConsecutiveFailedRuns would make it useless for CB8: the
	// operator's resume signal means "start fresh".
	s.LastRunAt = time.Time{}
	s.UpdatedAt = timeNow()
	return state.Save(ctx, cb.Bucket, s)
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
