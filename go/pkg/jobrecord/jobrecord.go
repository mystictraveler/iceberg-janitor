// Package jobrecord provides cross-replica persistent job records
// stored on the warehouse object store. The cmd/janitor-server's
// in-process jobStore wraps these records with a local cache, but
// the source of truth is the file at:
//
//	_janitor/state/jobs/<job_id>.json
//
// Why on the object store and not in DDB / Redis?
//
// The janitor is intentionally catalog-less and stateless beyond
// what lives in the backing object storage. Adding a new database
// for job records would break that principle. The same primitive
// (gocloud.dev/blob.Bucket) that holds the lease files, the
// per-table state, and the Iceberg metadata.json holds the job
// records. Reading a job record costs one S3 GET (~30-60 ms);
// writing one costs one S3 PUT. Acceptable for a low-rate API
// where each maintain takes minutes.
//
// # Cross-replica semantics
//
// A janitor-server replica that holds the lease for an in-flight
// job is the SOLE writer of that job's record. Other replicas
// READ the record (e.g. to answer GET /v1/jobs/{id}) but never
// write. The lease nonce is the gate: a writer that loses its
// lease (because another replica took over after a stale-recovery)
// must NOT write further updates — see HeartbeatVerify in Phase 3.
//
// # Heartbeat
//
// While a job is running, the lease holder overwrites the job
// record every JOB_HEARTBEAT_INTERVAL with HeartbeatAt = now.
// Other replicas can use the heartbeat age as the actual liveness
// signal (the lease TTL is a backstop). On completion, the writer
// sets Status + DoneAt + Result + Error in a final write.
package jobrecord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// HeartbeatInterval is how often a running job's owner should
// rewrite its record with a fresh HeartbeatAt. 5 minutes is the
// default to keep S3 PUT cost negligible while still being well
// under the lease TTL (15 min default) so a missed heartbeat is
// recoverable before the lease expires.
const HeartbeatInterval = 5 * time.Minute

// ErrNotFound is returned by Read when the job record file does
// not exist.
var ErrNotFound = errors.New("job record not found")

// Record is the on-disk representation of a job. It mirrors the
// in-process Job struct in cmd/janitor-server but lives outside
// that package so the lease + jobrecord layers can be tested in
// isolation. The cmd/janitor-server jobStore is responsible for
// converting between Record and Job.
type Record struct {
	ID          string          `json:"id"`
	Table       string          `json:"table"`
	Operation   string          `json:"operation"`
	Status      string          `json:"status"` // pending, running, completed, failed
	CreatedAt   time.Time       `json:"created_at"`
	HeartbeatAt time.Time       `json:"heartbeat_at,omitempty"`
	DoneAt      *time.Time      `json:"done_at,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`

	// LeaseNonce ties this job record to a specific lease
	// acquisition. If the lease at the matching path has a
	// different nonce, this job record is from a previous owner
	// that has been taken over — readers should not trust its
	// Status/Result without first verifying the lease.
	LeaseNonce string `json:"lease_nonce"`

	// Owner identifies the system that created this record. Same
	// shape as the lease's Owner field
	// ("hostname:pid:startup-uuid"). Useful for operator
	// debugging — "which replica produced this output?"
	Owner string `json:"owner,omitempty"`
}

// Path returns the bucket-relative key for a given job ID.
func Path(jobID string) string {
	return fmt.Sprintf("_janitor/state/jobs/%s.json", jobID)
}

// Write serializes the record and writes it to the bucket at the
// canonical path. Overwrites any existing record. The caller is
// responsible for ensuring it owns the lease for this job before
// writing — the bucket has no per-record protection. Returns the
// path on success.
func Write(ctx context.Context, bucket *blob.Bucket, r *Record) (string, error) {
	if r == nil || r.ID == "" {
		return "", fmt.Errorf("jobrecord.Write: record must have non-empty ID")
	}
	path := Path(r.ID)
	payload, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encoding record: %w", err)
	}
	w, err := bucket.NewWriter(ctx, path, &blob.WriterOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return path, fmt.Errorf("opening writer %s: %w", path, err)
	}
	if _, werr := w.Write(payload); werr != nil {
		_ = w.Close()
		return path, fmt.Errorf("writing record %s: %w", path, werr)
	}
	if err := w.Close(); err != nil {
		return path, fmt.Errorf("closing writer %s: %w", path, err)
	}
	return path, nil
}

// Read loads a job record by ID. Returns (nil, ErrNotFound) if
// the file does not exist.
func Read(ctx context.Context, bucket *blob.Bucket, jobID string) (*Record, error) {
	if jobID == "" {
		return nil, fmt.Errorf("jobrecord.Read: empty jobID")
	}
	path := Path(jobID)
	r, err := bucket.NewReader(ctx, path, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("opening reader %s: %w", path, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", path, err)
	}
	return &rec, nil
}

// Delete removes a job record. Idempotent: deleting a missing
// record is not an error. Used by operators to clean up failed
// jobs; production callers should leave completed records in
// place for observability.
func Delete(ctx context.Context, bucket *blob.Bucket, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("jobrecord.Delete: empty jobID")
	}
	if err := bucket.Delete(ctx, Path(jobID)); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return fmt.Errorf("deleting %s: %w", Path(jobID), err)
	}
	return nil
}

// IsHeartbeatStale reports whether the job's last heartbeat is
// older than maxAge. Used by other replicas to decide if a
// "running" job has actually died (heartbeat stale) and should
// be considered for stale-takeover. Pending and terminal status
// values (completed, failed) are never considered stale.
func (r *Record) IsHeartbeatStale(now time.Time, maxAge time.Duration) bool {
	if r == nil {
		return true
	}
	if r.Status != "running" {
		return false
	}
	if r.HeartbeatAt.IsZero() {
		// Running but no heartbeat yet — give it the benefit of
		// the doubt for one heartbeat interval after creation.
		return now.Sub(r.CreatedAt) > maxAge
	}
	return now.Sub(r.HeartbeatAt) > maxAge
}
