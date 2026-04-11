// Package lease provides a distributed per-table operation lock
// for the janitor. Any janitor-server replica that wants to run a
// maintain/compact/expire/rewrite-manifests operation on a table
// must first acquire the lease for that (table, operation) pair.
// The primitive uses the gocloud.dev/blob conditional-create
// operation (s3://*, gs://*, file://* all supported) to ensure
// that at most one replica holds a given lease at a time.
//
// This is the cross-replica complement to the in-process inflight
// guard in cmd/janitor-server/jobs.go. The in-process map dedupes
// concurrent POSTs that hit the same replica; the lease dedupes
// concurrent POSTs that hit different replicas.
//
// # Liveness
//
// Leases have a wall-clock TTL (default 15 min). If an owner dies
// mid-job, its lease eventually becomes "stale" — another replica
// can take over by deleting the stale lease file (idempotent) and
// CAS-creating a new one. Takeover is guarded by a 60-second grace
// period to tolerate clock skew between replicas.
//
// # Safety
//
// The lease file is immutable once created. It is never refreshed
// in place; only deleted and re-created on takeover. The owning
// replica communicates liveness through a SEPARATE heartbeat that
// lives in the job record (pkg/jobrecord) — the lease itself does
// not need refreshing. This separation keeps the safety primitive
// simple and avoids the common "refresh races with takeover" class
// of distributed-lease bugs.
//
// # Why not DynamoDB / etcd / Redis?
//
// The janitor is intentionally catalog-less and has no external
// state store beyond its backing object storage. Adding a new
// database for leases breaks that principle. gocloud.dev/blob's
// conditional-create maps to S3/GCS/Azure native primitives, is
// already in the deployment, and costs the same as a metadata.json
// commit.
package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// SystemOwner returns a stable, per-process owner identity suitable
// for passing to Options.Owner. The identity is
// "<hostname>:<pid>:<startup-uuid>" which is:
//
//   - unique across replicas (hostname includes ECS task short-id)
//   - stable for one process lifetime (startup-uuid is cached)
//   - identifiable for operators grep-ing lease files in S3
//
// For multi-tenant deployments where one process serves multiple
// operator identities (future work), the caller should prepend the
// tenant ID / operator role and use that. For single-tenant
// deployments — the current janitor — SystemOwner is the right
// default.
func SystemOwner() string {
	systemOwnerOnce.Do(func() {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		systemOwnerValue = fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.New().String())
	})
	return systemOwnerValue
}

var (
	systemOwnerOnce  sync.Once
	systemOwnerValue string
)

// DefaultTTL is the lease lifetime if the caller does not specify one.
// 15 minutes is long enough to cover a typical compact_hot run on
// real S3 (Run 18 showed 22 min worst-case, so this is intentionally
// shorter than the worst observed job; operators should set a longer
// TTL via Options if they have tables where maintain > 15 min).
const DefaultTTL = 15 * time.Minute

// TakeoverGrace is the buffer added to the TTL before a stale lease
// becomes eligible for takeover. Handles clock skew between replicas.
// With NTP-synced Fargate tasks this is effectively a safety margin.
const TakeoverGrace = 60 * time.Second

// ErrLeaseHeld is returned by TryAcquire when another owner currently
// holds the lease. The existing Lease is returned so the caller can
// inspect the holder (owner id, job id, acquired_at) and decide
// whether to wait, dedup, or attempt a stale-takeover.
var ErrLeaseHeld = errors.New("lease is held by another owner")

// ErrNotFound is returned by Read when the lease file does not exist.
var ErrNotFound = errors.New("lease not found")

// Lease is the on-disk representation of a held lease. The Nonce
// uniquely identifies an acquisition — if the nonce changes between
// two reads, a takeover has happened between them.
type Lease struct {
	Path        string    `json:"-"`
	Owner       string    `json:"owner"`
	Nonce       string    `json:"nonce"`
	JobID       string    `json:"job_id"`
	AcquiredAt  time.Time `json:"acquired_at"`
	TTLSeconds  int       `json:"ttl_seconds"`
}

// TTL returns the lease lifetime as a time.Duration.
func (l *Lease) TTL() time.Duration {
	return time.Duration(l.TTLSeconds) * time.Second
}

// ExpiresAt returns the absolute wall-clock time at which the lease
// becomes eligible for stale-takeover (after TakeoverGrace is added).
func (l *Lease) ExpiresAt() time.Time {
	return l.AcquiredAt.Add(l.TTL())
}

// IsStale reports whether the lease can be taken over at the given
// time. Stale means: now is past the (AcquiredAt + TTL + grace)
// window. The grace window is the buffer for clock skew between
// replicas — a lease only 30 seconds past its TTL should NOT be
// taken over, because the owner might simply be mid-operation.
func (l *Lease) IsStale(now time.Time) bool {
	if l == nil {
		return true
	}
	deadline := l.AcquiredAt.Add(l.TTL()).Add(TakeoverGrace)
	return now.After(deadline)
}

// Options configures TryAcquire.
type Options struct {
	// Owner identifies this acquisition attempt. Must be stable
	// for the lifetime of the process (typically
	// "hostname:pid:startup-uuid"). Two acquisitions with the
	// same Owner are treated as different attempts — the Nonce
	// distinguishes them.
	Owner string

	// JobID is the job record ID this lease is for. Written into
	// the lease file so readers (other replicas) can point
	// clients at the in-flight job.
	JobID string

	// TTL is the lease lifetime. Defaults to DefaultTTL if zero.
	TTL time.Duration

	// Now is the clock used for AcquiredAt. Tests inject a fixed
	// time; production callers leave it zero (uses time.Now()).
	Now time.Time
}

// Path builds the lease file path for a (table, operation) pair.
// The key structure is:
//
//	_janitor/state/leases/<ns>.<name>/<operation>.lease
//
// The namespace-dot-name combination is collision-free and
// matches the job record "table" field for easy cross-reference.
func Path(tableName, operation string) string {
	return fmt.Sprintf("_janitor/state/leases/%s/%s.lease", tableName, operation)
}

// pathLocks gives a per-path mutex used to serialize TryAcquire and
// StaleTakeover within a single process. The mutex is required
// because gocloud.dev's fileblob `IfNotExist` is implemented as
// Stat-then-Rename — a TOCTOU race that lets multiple writers
// "succeed" on the local-file backend. S3's IfNotExist is genuinely
// atomic; the mutex is harmless overhead there. Different paths
// don't lock each other (good — multiple tables can be acquired in
// parallel).
//
// Cross-process: this mutex is process-local and does NOT prevent
// two server replicas from racing on the same lease file. On real
// S3 the cross-process safety comes from S3's IfNotExist atomicity;
// on fileblob there is no cross-process safety and the test
// fixtures simulate "replicas" by running multiple jobStores in
// one process so the mutex covers them.
var (
	pathLocksMu sync.Mutex
	pathLocks   = map[string]*sync.Mutex{}
)

func lockForPath(path string) *sync.Mutex {
	pathLocksMu.Lock()
	defer pathLocksMu.Unlock()
	m, ok := pathLocks[path]
	if !ok {
		m = &sync.Mutex{}
		pathLocks[path] = m
	}
	return m
}

// TryAcquire attempts to CAS-create the lease file at the given
// path. Returns the new Lease on success. Returns (existingLease,
// ErrLeaseHeld) if another owner currently holds the lease.
// Returns (nil, error) on I/O errors or JSON encode failures.
//
// The caller is responsible for Stale-check + takeover on a held
// lease — TryAcquire does NOT automatically take over. See
// StaleTakeover for the takeover helper.
func TryAcquire(ctx context.Context, bucket *blob.Bucket, path string, opts Options) (*Lease, error) {
	if opts.Owner == "" {
		return nil, fmt.Errorf("lease TryAcquire: Owner must be set")
	}
	if opts.JobID == "" {
		return nil, fmt.Errorf("lease TryAcquire: JobID must be set")
	}
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}

	// Serialize CAS attempts to the same lease path within this
	// process. Without this, fileblob's TOCTOU lets multiple
	// goroutines "win" the IfNotExist check; tests cover this
	// directly.
	mu := lockForPath(path)
	mu.Lock()
	defer mu.Unlock()

	newLease := &Lease{
		Path:       path,
		Owner:      opts.Owner,
		Nonce:      uuid.New().String(),
		JobID:      opts.JobID,
		AcquiredAt: now.UTC(),
		TTLSeconds: int(opts.TTL.Seconds()),
	}
	payload, err := json.Marshal(newLease)
	if err != nil {
		return nil, fmt.Errorf("encoding lease: %w", err)
	}

	w, err := bucket.NewWriter(ctx, path, &blob.WriterOptions{
		ContentType: "application/json",
		IfNotExist:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening lease writer %s: %w", path, err)
	}
	if _, werr := w.Write(payload); werr != nil {
		_ = w.Close()
		return nil, fmt.Errorf("writing lease %s: %w", path, werr)
	}
	if cerr := w.Close(); cerr != nil {
		if gcerrors.Code(cerr) == gcerrors.FailedPrecondition {
			// Another owner already created the file. Read their
			// lease so the caller can extract JobID / owner info.
			existing, rerr := Read(ctx, bucket, path)
			if rerr != nil {
				return nil, fmt.Errorf("lease held but read failed: %w", rerr)
			}
			return existing, ErrLeaseHeld
		}
		return nil, fmt.Errorf("closing lease writer %s: %w", path, cerr)
	}
	return newLease, nil
}

// Read loads the lease file at path. Returns (nil, ErrNotFound) if
// the file does not exist.
func Read(ctx context.Context, bucket *blob.Bucket, path string) (*Lease, error) {
	r, err := bucket.NewReader(ctx, path, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("opening lease reader %s: %w", path, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading lease %s: %w", path, err)
	}
	var l Lease
	if err := json.Unmarshal(data, &l); err != nil {
		return nil, fmt.Errorf("decoding lease %s: %w", path, err)
	}
	l.Path = path
	return &l, nil
}

// Release deletes the lease file. Idempotent: a Release on a
// non-existent lease is not an error. Callers should release a
// lease only when the work it represented is done (success or
// failure). Releasing a lease you don't own is technically
// allowed but should be reserved for stale-takeover.
func Release(ctx context.Context, bucket *blob.Bucket, path string) error {
	if err := bucket.Delete(ctx, path); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return fmt.Errorf("deleting lease %s: %w", path, err)
	}
	return nil
}

// StaleTakeover attempts to take over a stale lease at `path`. The
// caller passes `expectedNonce` — the Nonce of the stale lease
// they observed. Takeover only proceeds if the lease at `path` STILL
// has that nonce when StaleTakeover takes the per-path mutex; if
// it has changed (another replica beat us to it), StaleTakeover
// reads the new lease and returns ErrLeaseHeld instead.
//
// The verify-still-stale check closes the within-process race
// between two replicas both trying to take over the same stale
// lease. Cross-process between two real replicas, the verify
// check is best-effort: another replica's takeover could be
// observed after our verify but before our delete, in which case
// our delete kills the new lease. The mitigation is the
// heartbeat-verify check inside the running job (see Phase 3 in
// jobStore): a job whose lease nonce changes mid-run aborts.
//
// Returns (newLease, nil) on success, (existingLease, ErrLeaseHeld)
// if another replica won the race, or (_, error) on I/O failure.
func StaleTakeover(ctx context.Context, bucket *blob.Bucket, path string, expectedNonce string, opts Options) (*Lease, error) {
	mu := lockForPath(path)
	mu.Lock()
	defer mu.Unlock()

	// Verify the lease at `path` is still the stale one we expected.
	current, err := Read(ctx, bucket, path)
	switch {
	case errors.Is(err, ErrNotFound):
		// The stale lease has already been deleted by someone. Just
		// try to acquire fresh — but we can't call TryAcquire
		// because it takes the same per-path lock we already hold.
		// Inline the body.
		return tryAcquireLocked(ctx, bucket, path, opts)
	case err != nil:
		return nil, fmt.Errorf("stale takeover: verify read: %w", err)
	}
	if current.Nonce != expectedNonce {
		// Another replica beat us. Return their lease.
		return current, ErrLeaseHeld
	}
	if !current.IsStale(time.Now()) {
		// Caller misjudged staleness. Return the (still-valid) lease.
		return current, ErrLeaseHeld
	}

	// Best-effort delete of the stale lease, then re-create.
	if err := bucket.Delete(ctx, path); err != nil {
		if gcerrors.Code(err) != gcerrors.NotFound {
			return nil, fmt.Errorf("stale takeover: delete: %w", err)
		}
	}
	return tryAcquireLocked(ctx, bucket, path, opts)
}

// tryAcquireLocked is the inner CAS-create body assuming the
// caller already holds the per-path lock. Used by StaleTakeover
// after the verify check.
func tryAcquireLocked(ctx context.Context, bucket *blob.Bucket, path string, opts Options) (*Lease, error) {
	if opts.Owner == "" {
		return nil, fmt.Errorf("lease tryAcquireLocked: Owner must be set")
	}
	if opts.JobID == "" {
		return nil, fmt.Errorf("lease tryAcquireLocked: JobID must be set")
	}
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	newLease := &Lease{
		Path:       path,
		Owner:      opts.Owner,
		Nonce:      uuid.New().String(),
		JobID:      opts.JobID,
		AcquiredAt: now.UTC(),
		TTLSeconds: int(opts.TTL.Seconds()),
	}
	payload, err := json.Marshal(newLease)
	if err != nil {
		return nil, fmt.Errorf("encoding lease: %w", err)
	}
	w, err := bucket.NewWriter(ctx, path, &blob.WriterOptions{
		ContentType: "application/json",
		IfNotExist:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening lease writer %s: %w", path, err)
	}
	if _, werr := w.Write(payload); werr != nil {
		_ = w.Close()
		return nil, fmt.Errorf("writing lease %s: %w", path, werr)
	}
	if cerr := w.Close(); cerr != nil {
		if gcerrors.Code(cerr) == gcerrors.FailedPrecondition {
			existing, rerr := Read(ctx, bucket, path)
			if rerr != nil {
				return nil, fmt.Errorf("lease held but read failed: %w", rerr)
			}
			return existing, ErrLeaseHeld
		}
		return nil, fmt.Errorf("closing lease writer %s: %w", path, cerr)
	}
	return newLease, nil
}
