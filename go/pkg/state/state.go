package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// Outcome is the result of one maintenance attempt on a table. The
// circuit breaker increments ConsecutiveFailedRuns on Failure and resets
// it to zero on Success. Skipped runs (e.g. nothing to do, or table is
// already paused) do not touch the counter at all.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
)

// MaxRetainedErrors caps how many recent error messages a TableState
// keeps in LastErrors. The CB8 issue calls this out as the data the
// pause file should embed for the operator runbook ("here are the last
// N errors that caused the auto-pause"). Three matches the CB8 default
// failure threshold so a freshly-paused table has all its causal errors
// in one place.
const MaxRetainedErrors = 3

// ErrorRecord is one captured failure from a maintenance attempt. The
// At timestamp is wall-clock UTC; the Message is the err.Error() string
// truncated at 4 KiB so a runaway error can't blow up the state file.
type ErrorRecord struct {
	At      time.Time `json:"at"`
	Message string    `json:"message"`
}

const errorMessageMax = 4096

// TableState is the per-table janitor bookkeeping persisted at
// _janitor/state/<table_uuid>.json. Field set is intentionally minimal:
// only the data CB8 needs today, plus identification fields. Other
// circuit breakers (CB1 cooldown, CB7 daily byte budget, CB9 lifetime
// rewrite ratio) will add their own fields here as they ship; the JSON
// is forwards-compatible because every field is omitempty and unknown
// keys are ignored on read.
type TableState struct {
	TableUUID string    `json:"table_uuid"`
	UpdatedAt time.Time `json:"updated_at"`

	// CB8: consecutive maintenance failures with no successful run
	// in between. Reset to zero on every success. When this reaches
	// the CB8 threshold (default 3), the circuit breaker writes a
	// pause file at _janitor/control/paused/<uuid>.json and refuses
	// further work on this table until an operator clears it.
	ConsecutiveFailedRuns int           `json:"consecutive_failed_runs"`
	LastErrors            []ErrorRecord `json:"last_errors,omitempty"`
	LastOutcome           Outcome       `json:"last_outcome,omitempty"`
	LastSuccessAt         time.Time     `json:"last_success_at,omitempty"`

	// CB1: wall-clock timestamp of the most recent maintenance attempt
	// on this table, regardless of outcome. Distinct from LastSuccessAt
	// because cooldown applies to BOTH success and failure runs — we
	// need to know "when did we last try", not "when did we last
	// succeed". Updated by RecordSuccess AND RecordFailure.
	LastRunAt time.Time `json:"last_run_at,omitempty"`
}

// RecordSuccess clears the failure counter and the recent-errors list
// and stamps LastSuccessAt. Mutates in place.
func (s *TableState) RecordSuccess(now time.Time) {
	s.ConsecutiveFailedRuns = 0
	s.LastErrors = nil
	s.LastOutcome = OutcomeSuccess
	s.LastSuccessAt = now
	s.LastRunAt = now
	s.UpdatedAt = now
}

// RecordFailure increments the failure counter, appends the truncated
// error message to LastErrors (capped at MaxRetainedErrors), and stamps
// UpdatedAt. Mutates in place.
func (s *TableState) RecordFailure(now time.Time, errMsg string) {
	s.ConsecutiveFailedRuns++
	s.LastOutcome = OutcomeFailure
	if len(errMsg) > errorMessageMax {
		errMsg = errMsg[:errorMessageMax]
	}
	s.LastErrors = append(s.LastErrors, ErrorRecord{At: now, Message: errMsg})
	if len(s.LastErrors) > MaxRetainedErrors {
		s.LastErrors = s.LastErrors[len(s.LastErrors)-MaxRetainedErrors:]
	}
	s.LastRunAt = now
	s.UpdatedAt = now
}

// StateKey is the bucket-relative key for a table's state file.
// Exported so callers writing tests against a raw bucket can construct
// the same path the package uses.
func StateKey(tableUUID string) string {
	return path.Join("_janitor", "state", tableUUID+".json")
}

// PausedKey is the bucket-relative key for a table's pause file. Same
// rationale as StateKey.
func PausedKey(tableUUID string) string {
	return path.Join("_janitor", "control", "paused", tableUUID+".json")
}

// Load reads the state file for a table from the warehouse bucket.
// If the file does not exist, Load returns a zero TableState (with
// TableUUID populated) and a nil error — a brand-new table simply has
// no history yet. Any other error (network, malformed JSON, etc.) is
// returned and the caller should NOT proceed: if we cannot determine
// the table's history we have no basis for the circuit breaker
// decision, and the safe choice is to fail loudly rather than silently
// resetting the failure counter.
func Load(ctx context.Context, bucket *blob.Bucket, tableUUID string) (*TableState, error) {
	key := StateKey(tableUUID)
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return &TableState{TableUUID: tableUUID}, nil
		}
		return nil, fmt.Errorf("opening state %s: %w", key, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading state %s: %w", key, err)
	}
	if len(body) == 0 {
		return &TableState{TableUUID: tableUUID}, nil
	}
	var s TableState
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("decoding state %s: %w", key, err)
	}
	if s.TableUUID == "" {
		s.TableUUID = tableUUID
	}
	return &s, nil
}

// Save writes a state file for a table. This is an unconditional
// last-writer-wins put — concurrent writers on the same table would
// race, but the lease primitive (decision #9 in the design plan) is
// what prevents two janitor invocations from working the same table
// simultaneously, and the lease has not yet been built. The current
// caller (cmd/janitor-cli) is the only path that writes state, and the
// CLI is single-process, so the race is not reachable today. The
// circuit-breaker tests rely on Save being a plain put. Once the lease
// primitive lands, Save will not need to change — the lease excludes
// the racers at a higher level.
func Save(ctx context.Context, bucket *blob.Bucket, s *TableState) error {
	if s == nil {
		return errors.New("Save: nil state")
	}
	if s.TableUUID == "" {
		return errors.New("Save: state has empty TableUUID")
	}
	key := StateKey(s.TableUUID)
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}
	w, err := bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("opening writer for %s: %w", key, err)
	}
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return fmt.Errorf("writing state %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing state %s: %w", key, err)
	}
	return nil
}

// PauseFile is the JSON payload written to _janitor/control/paused/<uuid>.json
// when a circuit breaker auto-pauses a table. The format is a janitor-internal
// contract; operators read it via `janitor-cli status <table>`.
type PauseFile struct {
	TableUUID   string        `json:"table_uuid"`
	Reason      string        `json:"reason"`
	TriggeredAt time.Time     `json:"triggered_at"`
	TriggeredBy string        `json:"triggered_by"`
	LastErrors  []ErrorRecord `json:"last_errors,omitempty"`
}

// LoadPause reads the pause file for a table. Returns nil and a nil
// error if the file does not exist (i.e. the table is not paused).
// Any other error is propagated to the caller; the safe choice on
// "I cannot tell if this table is paused" is to fail closed.
func LoadPause(ctx context.Context, bucket *blob.Bucket, tableUUID string) (*PauseFile, error) {
	key := PausedKey(tableUUID)
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("opening pause %s: %w", key, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading pause %s: %w", key, err)
	}
	var p PauseFile
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("decoding pause %s: %w", key, err)
	}
	return &p, nil
}

// SavePause writes (or overwrites) the pause file for a table. As with
// Save, this is unconditional last-writer-wins; the underlying object
// store provides per-key durability and the lease primitive will
// provide cross-invocation exclusion when it lands. Overwriting an
// existing pause file is intentional: a CB8 re-trigger on a table that
// was manually re-paused should refresh the timestamp and error list
// rather than fail.
func SavePause(ctx context.Context, bucket *blob.Bucket, p *PauseFile) error {
	if p == nil {
		return errors.New("SavePause: nil pause")
	}
	if p.TableUUID == "" {
		return errors.New("SavePause: pause has empty TableUUID")
	}
	key := PausedKey(p.TableUUID)
	body, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding pause: %w", err)
	}
	w, err := bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("opening writer for %s: %w", key, err)
	}
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return fmt.Errorf("writing pause %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing pause %s: %w", key, err)
	}
	return nil
}

// DeletePause removes the pause file for a table. This is the
// operator's "resume" action. Deleting a non-existent pause file is
// not an error — it's the same observable result the operator wants
// either way (the table is no longer paused).
func DeletePause(ctx context.Context, bucket *blob.Bucket, tableUUID string) error {
	key := PausedKey(tableUUID)
	if err := bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return fmt.Errorf("deleting pause %s: %w", key, err)
	}
	return nil
}
