package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gocloud.dev/blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/jobrecord"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/lease"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/strategy/classify"

	icebergtable "github.com/apache/iceberg-go/table"
)

// Job represents an async maintenance operation.
type Job struct {
	ID        string          `json:"job_id"`
	Status    string          `json:"status"` // pending, running, completed, failed
	Table     string          `json:"table"`
	Operation string          `json:"operation"`
	CreatedAt time.Time       `json:"created_at"`
	DoneAt    *time.Time      `json:"done_at,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	// Owner identifies the server replica that created this job.
	// Set to lease.SystemOwner() in persistent mode. Used by the
	// jobStore to distinguish owned (we are the writer) from
	// foreign (another replica is the writer) cached entries
	// during get(). Empty in in-process-only mode.
	Owner string `json:"owner,omitempty"`
}

// jobStore is a simple in-memory job tracker.
//
// Per-table in-flight guard: inflight[operationKey] maps
// "maintain|<ns>.<table>" (or "compact|..." etc) to the
// currently-running job ID for that table+operation. create()
// atomically checks this map: if there's already an in-flight
// job for the same (operation, table), it returns (existingJob,
// false) instead of spawning a duplicate. Without this guard, a
// caller that POSTs /maintain twice in quick succession spawns
// two jobs that race for the same metadata.json CAS, exhaust
// their retry budgets, and mark partitions failed. This is the
// pathology Run 18 surfaced: the bench's 300s client timeout
// fired before maintain finished, the bench retried, and round-1
// and round-2 maintain jobs for the same table overlapped on
// the server for 8.5 minutes, fighting themselves.
//
// LIMITATION — single-replica only. This guard is IN-PROCESS.
// If the janitor-server ECS service is scaled to desired_count
// > 1, each replica has its own jobStore and its own inflight
// map; two replicas can independently accept the same table's
// maintain POST and spawn overlapping jobs that self-collide on
// the metadata.json CAS. The AWS deployment pins
// desired_count = 1 (see deploy/aws/terraform/ecs.tf) which
// makes this guard sufficient today.
//
// When the server needs to scale, replace this in-process map
// with a distributed lease keyed on (operation, table). The
// natural primitive is an S3 "lease file" at
// _janitor/state/<table_uuid>/<operation>.lease created via
// conditional CAS (If-None-Match: *), held for the job duration,
// and deleted on completion. This was the original CB1 design
// before it was removed as premature. The same guard shape
// works across any number of replicas because S3 conditional
// write is atomic.
type jobStore struct {
	mu       sync.RWMutex
	jobs     map[string]*Job
	inflight map[string]string // operationKey -> job_id of running job

	// bucket is the optional warehouse bucket. When non-nil, the
	// jobStore writes lease files to _janitor/state/leases/ and
	// job records to _janitor/state/jobs/, making the dedup +
	// observability work across multiple server replicas.
	// When nil, the jobStore is in-process only — same behavior
	// as the original implementation. Used by unit tests that
	// only exercise the local cache primitive.
	bucket *blob.Bucket
	// owner is this server's identity, derived from lease.SystemOwner
	// at construction time. Stable for the process lifetime.
	owner string
	// leaseTTL is how long lease entries live before being eligible
	// for stale-takeover. Defaults to 60 minutes — long enough to
	// cover the worst observed maintain wall time (Run 18: 22 min)
	// with a comfortable margin. Operators can shorten this for
	// testing or extend it for tables where maintain commonly
	// runs longer.
	leaseTTL time.Duration
}

// newJobStore returns an in-process-only jobStore. Used by unit
// tests that exercise the cache primitive in isolation. For the
// full cross-replica behavior, use newPersistentJobStore.
func newJobStore() *jobStore {
	return &jobStore{
		jobs:     make(map[string]*Job),
		inflight: make(map[string]string),
	}
}

// newPersistentJobStore returns a jobStore backed by the given
// warehouse bucket. lease and jobrecord files live under
// _janitor/state/{leases,jobs}/. The in-process map remains the
// fast-path cache; cache misses fall back to S3 reads.
//
// Multiple janitor-server replicas sharing the same bucket get
// cross-replica dedup (any replica that POSTs /maintain for a
// table that's already being processed gets the in-flight job's
// ID, not a duplicate) and cross-replica observability (any
// replica can answer GET /v1/jobs/{id} for a job that another
// replica is running).
func newPersistentJobStore(bucket *blob.Bucket) *jobStore {
	return newPersistentJobStoreWithOwner(bucket, lease.SystemOwner())
}

// newPersistentJobStoreWithOwner is like newPersistentJobStore but
// lets the caller specify the owner identity explicitly. Used by
// tests that simulate multiple replicas in a single process — in
// production, lease.SystemOwner() is process-singleton, so two
// stores constructed in the same process share identity. For
// tests we need distinct owners to exercise the cross-replica
// foreign-job code paths.
func newPersistentJobStoreWithOwner(bucket *blob.Bucket, owner string) *jobStore {
	return &jobStore{
		jobs:     make(map[string]*Job),
		inflight: make(map[string]string),
		bucket:   bucket,
		owner:    owner,
		leaseTTL: 60 * time.Minute,
	}
}

// inflightKey is the dedup key for the in-flight guard. Operations
// of different kinds (maintain vs compact) can run concurrently on
// the same table; only same-operation same-table jobs are deduped.
func inflightKey(operation, table string) string {
	return operation + "|" + table
}

// create atomically checks for an in-flight job and returns a
// snapshot of the existing job if one is already running. Returns
// (job, true) if a NEW job was created, (existingJob, false) if an
// in-flight job was returned. Callers that get (_, false) must NOT
// start a new goroutine — the existing job is already running.
//
// The returned Job is a VALUE COPY, not a pointer into the store.
// This is load-bearing: without the copy, the handler that marshals
// the job to JSON would race against the background goroutine that
// mutates the job via complete(). The race is a silent correctness
// bug — json.Encode reads Status/DoneAt/Result while complete()
// writes them — and is caught by `go test -race`.
func (s *jobStore) create(table, operation string) (Job, bool) {
	key := inflightKey(operation, table)

	// In-process-only mode (no bucket): hold the lock for the
	// entire check-and-create. This is the path used by unit
	// tests and matches the pre-Phase-3 behavior. Releasing the
	// lock between the fast-path check and the commit creates a
	// TOCTOU window where multiple goroutines can pass the
	// inflight check and create duplicate jobs — exactly what
	// the in-flight guard is supposed to prevent.
	if s.bucket == nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if existingID, ok := s.inflight[key]; ok {
			if existing, ok2 := s.jobs[existingID]; ok2 {
				if existing.Status == "pending" || existing.Status == "running" {
					return *existing, false
				}
				delete(s.inflight, key)
			}
		}
		j := &Job{
			ID:        uuid.New().String(),
			Status:    "pending",
			Table:     table,
			Operation: operation,
			CreatedAt: time.Now(),
		}
		s.jobs[j.ID] = j
		s.inflight[key] = j.ID
		return *j, true
	}

	// Persistent mode: fast-path check first, then go through the
	// lease primitive (which serializes cross-goroutine via its
	// own per-key mutex). The lease is the source of truth — if
	// the fast path misses but a concurrent goroutine wins the
	// lease, our lease attempt returns ErrLeaseHeld and we dedup
	// to that job.
	s.mu.RLock()
	if existingID, ok := s.inflight[key]; ok {
		if existing, ok2 := s.jobs[existingID]; ok2 {
			if existing.Status == "pending" || existing.Status == "running" {
				snapshot := *existing
				s.mu.RUnlock()
				return snapshot, false
			}
		}
	}
	s.mu.RUnlock()

	// Persistent mode: try to acquire the lease for this
	// (operation, table) pair. If we win, write the initial job
	// record and return a fresh job. If we lose, fetch the in-
	// flight job's record from S3 and return it as the dedup
	// response.
	leasePath := lease.Path(table, operation)
	jobID := uuid.New().String()
	ctx := context.Background()
	leaseObj, lerr := lease.TryAcquire(ctx, s.bucket, leasePath, lease.Options{
		Owner: s.owner,
		JobID: jobID,
		TTL:   s.leaseTTL,
	})
	if errors.Is(lerr, lease.ErrLeaseHeld) {
		// Another replica (or this one earlier) already holds the
		// lease. Read the in-flight job from S3 and return it.
		// If the existing lease is stale, attempt takeover.
		if leaseObj != nil && leaseObj.IsStale(time.Now()) {
			leaseObj, lerr = lease.StaleTakeover(ctx, s.bucket, leasePath, leaseObj.Nonce, lease.Options{
				Owner: s.owner,
				JobID: jobID,
				TTL:   s.leaseTTL,
			})
		}
		if errors.Is(lerr, lease.ErrLeaseHeld) {
			// Still held by another live owner — return the
			// existing job record. If we can't load the record,
			// fall back to a synthetic "in-flight on another
			// replica" Job pointer derived from the lease alone.
			rec, rerr := jobrecord.Read(ctx, s.bucket, leaseObj.JobID)
			if rerr == nil {
				return s.cacheForeignRecord(recordFromJobrecord(rec)), false
			}
			// Lease exists but record doesn't yet — owner is
			// still creating it. Return a placeholder Job that
			// the client can poll. The client will eventually
			// see the real record once the owner writes it.
			return Job{
				ID:        leaseObj.JobID,
				Status:    "pending",
				Table:     table,
				Operation: operation,
				CreatedAt: leaseObj.AcquiredAt,
			}, false
		}
	}
	if lerr != nil {
		// Lease acquisition failed for an unexpected reason. Fall
		// back to local-only behavior — we still create a job and
		// run it, but other replicas won't see it. Operators can
		// detect this via the WARN log line.
		return s.createLocal(key, table, operation), true
	}

	// We won the lease. Build the job, write the initial record
	// to S3, and store in local cache.
	now := time.Now()
	j := &Job{
		ID:        leaseObj.JobID,
		Status:    "pending",
		Table:     table,
		Operation: operation,
		CreatedAt: now,
		Owner:     s.owner,
	}
	rec := &Record{
		ID:         j.ID,
		Table:      table,
		Operation:  operation,
		Status:     "pending",
		CreatedAt:  now,
		LeaseNonce: leaseObj.Nonce,
		Owner:      s.owner,
	}
	if _, werr := jobrecord.Write(ctx, s.bucket, recToJobrecord(rec)); werr != nil {
		// Write failed — release the lease so another replica
		// can take over and don't cache the job.
		_ = lease.Release(ctx, s.bucket, leasePath)
		return s.createLocal(key, table, operation), true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
	s.inflight[key] = j.ID
	return *j, true
}

// createLocal is the in-process-only path used by both the
// nil-bucket store (unit tests) and as a fallback when the
// persistent path fails. Holds no lock; must be called with
// caller responsible for serialization. NOTE: this version
// does NOT touch S3 and so will not be visible to other
// replicas — used only when persistent storage is unavailable
// or in tests.
func (s *jobStore) createLocal(key, table, operation string) Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-check inflight under the lock in case the fast-path
	// missed by a hair.
	if existingID, ok := s.inflight[key]; ok {
		if existing, ok2 := s.jobs[existingID]; ok2 {
			if existing.Status == "pending" || existing.Status == "running" {
				return *existing
			}
		}
	}
	j := &Job{
		ID:        uuid.New().String(),
		Status:    "pending",
		Table:     table,
		Operation: operation,
		CreatedAt: time.Now(),
	}
	s.jobs[j.ID] = j
	s.inflight[key] = j.ID
	return *j
}

// cacheForeignRecord stores a job record from another replica in
// the local cache and returns a Job snapshot. The cached entry's
// Owner field is set to the foreign owner from the record so the
// jobStore can distinguish owned vs foreign cache entries on
// subsequent get() calls. Foreign entries get refreshed from S3
// on every get; owned entries are authoritative.
func (s *jobStore) cacheForeignRecord(rec *Record) Job {
	j := &Job{
		ID:        rec.ID,
		Status:    rec.Status,
		Table:     rec.Table,
		Operation: rec.Operation,
		CreatedAt: rec.CreatedAt,
		DoneAt:    rec.DoneAt,
		Result:    rec.Result,
		Error:     rec.Error,
		Owner:     rec.Owner,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Don't overwrite an existing OWNED entry with a foreign
	// snapshot. (Shouldn't happen but defensive.)
	if existing, ok := s.jobs[j.ID]; ok && existing.Owner == s.owner {
		return *existing
	}
	s.jobs[j.ID] = j
	return *j
}

// Record is the cmd/janitor-server-internal mirror of
// jobrecord.Record. It's a separate type so the cmd/janitor-server
// package owns its on-disk shape and can evolve it independently
// of pkg/jobrecord. The recToJobrecord helper converts.
type Record struct {
	ID          string
	Table       string
	Operation   string
	Status      string
	CreatedAt   time.Time
	HeartbeatAt time.Time
	DoneAt      *time.Time
	Result      json.RawMessage
	Error       string
	LeaseNonce  string
	Owner       string
}

func recToJobrecord(r *Record) *jobrecord.Record {
	return &jobrecord.Record{
		ID:          r.ID,
		Table:       r.Table,
		Operation:   r.Operation,
		Status:      r.Status,
		CreatedAt:   r.CreatedAt,
		HeartbeatAt: r.HeartbeatAt,
		DoneAt:      r.DoneAt,
		Result:      r.Result,
		Error:       r.Error,
		LeaseNonce:  r.LeaseNonce,
		Owner:       r.Owner,
	}
}

// get returns a value snapshot of the job. Returning a pointer
// would leak the store's internal mutable state and race against
// complete()/setRunning(). Callers that need to observe progress
// must call get() repeatedly (e.g. the handleJobStatus poll).
//
// Persistent mode: cache miss falls back to a jobrecord.Read on
// the bucket. The fetched record is cached as a foreign-record
// snapshot so subsequent gets are fast. The cache is small and
// foreign records DO NOT get invalidated automatically — for an
// updated view, the client should poll repeatedly and the cache
// will be re-populated each time the lookup goes through. (Phase
// 4 may add a foreign-record TTL if this becomes an issue.)
func (s *jobStore) get(id string) (Job, bool) {
	s.mu.RLock()
	j, ok := s.jobs[id]
	if ok {
		snapshot := *j
		s.mu.RUnlock()
		// Owned job in cache: cache is authoritative for "is this
		// our work in progress?" but we still want to forward to
		// S3 for foreign-record refreshes. Owned snapshot wins.
		if snapshot.Owner == s.owner || s.bucket == nil {
			return snapshot, true
		}
		// Foreign cached entry — refresh from S3 so the polling
		// client sees the live progress on the owning replica.
		if rec, rerr := jobrecord.Read(context.Background(), s.bucket, id); rerr == nil {
			return s.cacheForeignRecord(recordFromJobrecord(rec)), true
		}
		// S3 read failed — return stale cached value rather than
		// nothing. Better stale than 404.
		return snapshot, true
	}
	s.mu.RUnlock()
	// Cache miss + persistent mode: fall back to S3.
	if s.bucket == nil {
		return Job{}, false
	}
	rec, rerr := jobrecord.Read(context.Background(), s.bucket, id)
	if rerr != nil {
		return Job{}, false
	}
	return s.cacheForeignRecord(recordFromJobrecord(rec)), true
}

// (closing the function above)

func recordFromJobrecord(r *jobrecord.Record) *Record {
	return &Record{
		ID:          r.ID,
		Table:       r.Table,
		Operation:   r.Operation,
		Status:      r.Status,
		CreatedAt:   r.CreatedAt,
		HeartbeatAt: r.HeartbeatAt,
		DoneAt:      r.DoneAt,
		Result:      r.Result,
		Error:       r.Error,
		LeaseNonce:  r.LeaseNonce,
		Owner:       r.Owner,
	}
}

func (s *jobStore) complete(id string, result any, err error) {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	now := time.Now()
	j.DoneAt = &now
	if err != nil {
		j.Status = "failed"
		j.Error = err.Error()
	} else {
		j.Status = "completed"
	}
	if result != nil {
		data, _ := json.Marshal(result)
		j.Result = data
	}
	// Release the in-flight slot so a subsequent POST for the same
	// (operation, table) is allowed to start a fresh job.
	key := inflightKey(j.Operation, j.Table)
	if s.inflight[key] == id {
		delete(s.inflight, key)
	}
	finalSnapshot := *j
	s.mu.Unlock()

	// Persist the terminal state to S3 and release the lease so
	// other replicas can take over for the next maintain cycle.
	// Failures here are best-effort: the job has already finished
	// locally; an S3 outage shouldn't mask that.
	if s.bucket != nil {
		ctx := context.Background()
		rec := &Record{
			ID:          finalSnapshot.ID,
			Table:       finalSnapshot.Table,
			Operation:   finalSnapshot.Operation,
			Status:      finalSnapshot.Status,
			CreatedAt:   finalSnapshot.CreatedAt,
			HeartbeatAt: time.Now(),
			DoneAt:      finalSnapshot.DoneAt,
			Result:      finalSnapshot.Result,
			Error:       finalSnapshot.Error,
			Owner:       s.owner,
		}
		_, _ = jobrecord.Write(ctx, s.bucket, recToJobrecord(rec))
		_ = lease.Release(ctx, s.bucket, lease.Path(finalSnapshot.Table, finalSnapshot.Operation))
	}
}

func (s *jobStore) setRunning(id string) {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	j.Status = "running"
	snapshot := *j
	s.mu.Unlock()

	// Persistent mode: write the running-state record so other
	// replicas see we've started work.
	if s.bucket != nil {
		ctx := context.Background()
		rec := &Record{
			ID:          snapshot.ID,
			Table:       snapshot.Table,
			Operation:   snapshot.Operation,
			Status:      "running",
			CreatedAt:   snapshot.CreatedAt,
			HeartbeatAt: time.Now(),
			Owner:       s.owner,
		}
		_, _ = jobrecord.Write(ctx, s.bucket, recToJobrecord(rec))
	}
}

// parseCompactOpts reads optional query parameters:
//
//	?partition=col=value         — scope to one partition
//	?target_file_size=1MB        — Pattern B threshold (KB/MB/GB suffixes)
//	?dry_run=true                — plan only, no side effects (see pkg docs)
func parseCompactOpts(r *http.Request) janitor.CompactOptions {
	opts := janitor.CompactOptions{}
	if v := r.URL.Query().Get("partition"); v != "" {
		eq := strings.Index(v, "=")
		if eq > 0 {
			col := strings.TrimSpace(v[:eq])
			val := strings.TrimSpace(v[eq+1:])
			if col != "" && val != "" {
				opts.PartitionTuple = map[string]string{col: val}
			}
		}
	}
	if v := r.URL.Query().Get("target_file_size"); v != "" {
		if n, err := parseFileSizeServer(v); err == nil {
			opts.TargetFileSizeBytes = n
		}
	}
	if parseBoolQuery(r, "dry_run") {
		opts.DryRun = true
	}
	return opts
}

// parseBoolQuery reads a boolean query parameter. Accepts the usual
// truthy strings "true", "1", "yes", "on" (case-insensitive). Any
// other value — including missing — is false. Shared by every
// maintenance handler that needs a ?dry_run=... flag.
func parseBoolQuery(r *http.Request, name string) bool {
	v := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(name)))
	switch v {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// parseFileSizeServer is a minimal size parser for query params.
// Supports KB/MB/GB suffixes (powers of 1024). No suffix means bytes.
func parseFileSizeServer(s string) (int64, error) {
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)
	var mul int64 = 1
	switch {
	case strings.HasSuffix(upper, "KB"):
		mul = 1024
		s = s[:len(s)-2]
	case strings.HasSuffix(upper, "MB"):
		mul = 1024 * 1024
		s = s[:len(s)-2]
	case strings.HasSuffix(upper, "GB"):
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-2]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size: %s", s)
	}
	return n * mul, nil
}

// handleCompactAsync accepts a compact request, starts the work in a
// goroutine, and returns 202 Accepted with a job ID immediately.
//
// Query params: ?partition=col=value&target_file_size=1MB
func (s *server) handleCompactAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	compactOpts := parseCompactOpts(r)

	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job, isNew := s.jobs.create(tableName, "compact")
	if !isNew {
		s.logger.Info("compact job dedup — returning in-flight job",
			"job_id", job.ID, "table", tableName)
		writeJSON(w, http.StatusAccepted, job)
		return
	}

	s.logger.Info("compact job created",
		"job_id", job.ID,
		"namespace", ident[0],
		"table", ident[1],
	)

	go s.runCompactJob(job.ID, ident, compactOpts)

	writeJSON(w, http.StatusAccepted, job)
}

func (s *server) runCompactJob(jobID string, ident icebergtable.Identifier, opts janitor.CompactOptions) {
	s.jobs.setRunning(jobID)
	started := time.Now()
	s.logger.Info("compact job started",
		"job_id", jobID,
		"table", fmt.Sprintf("%s.%s", ident[0], ident[1]),
	)

	ctx := context.Background()
	result, err := janitor.Compact(ctx, s.cat, ident, opts)

	elapsed := time.Since(started)
	s.jobs.complete(jobID, result, err)

	if err != nil {
		s.logger.Error("compact job failed",
			"job_id", jobID,
			"table", fmt.Sprintf("%s.%s", ident[0], ident[1]),
			"elapsed_ms", elapsed.Milliseconds(),
			"err", err,
		)
		if result != nil && result.Verification != nil {
			s.logger.Error("compact job verification",
				"job_id", jobID,
				"overall", result.Verification.Overall,
				"I1", result.Verification.I1RowCount.Result,
				"I7", result.Verification.I7ManifestRefs.Result,
			)
		}
	} else {
		// Fields kept stable across releases — the CloudWatch
		// dashboard (deploy/aws/terraform/dashboard.tf) queries them
		// by name. When adding new fields, APPEND rather than
		// rename/remove.
		args := []any{
			"job_id", jobID,
			"table", fmt.Sprintf("%s.%s", ident[0], ident[1]),
			"elapsed_ms", elapsed.Milliseconds(),
			"before_files", result.BeforeFiles,
			"after_files", result.AfterFiles,
			"before_rows", result.BeforeRows,
			"after_rows", result.AfterRows,
			"before_bytes", result.BeforeBytes,
			"after_bytes", result.AfterBytes,
			"attempts", result.Attempts,
			// Skipped-round telemetry (schema-evolution guard, etc).
			// skipped=false for normal rounds; when skipped is true
			// the reason is the discriminator the dashboard stacks
			// by.
			"skipped", result.Skipped,
			"skipped_reason", result.SkippedReason,
		}
		if result.Verification != nil {
			// Surface the master-check result so a dashboard can
			// track pass/fail rate + per-invariant pass without a
			// separate error-path log line.
			args = append(args,
				"master_overall", result.Verification.Overall,
				"master_I1", result.Verification.I1RowCount.Result,
				"master_I2", result.Verification.I2Schema.Result,
				"master_I3", result.Verification.I3ValueCounts.Result,
				"master_I4", result.Verification.I4NullCounts.Result,
				"master_I5", result.Verification.I5Bounds.Result,
				"master_I7", result.Verification.I7ManifestRefs.Result,
				"dvs_applied", result.Verification.I1RowCount.DVs,
			)
		}
		s.logger.Info("compact job completed", args...)
	}
}

// parseExpireOpts reads optional query parameters from the request:
//
//	?keep_last=N       — minimum snapshots to retain (default 5)
//	?keep_within=DUR   — Go duration string, e.g. "168h" (default 7 days)
//
// Returns ExpireOptions with the parsed values. Unset params use the
// package-level defaults (filled in by ExpireOptions.defaults()).
func parseExpireOpts(r *http.Request) maintenance.ExpireOptions {
	opts := maintenance.ExpireOptions{}
	if v := r.URL.Query().Get("keep_last"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.KeepLast = n
		}
	}
	if v := r.URL.Query().Get("keep_within"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			opts.KeepWithin = d
		}
	}
	if parseBoolQuery(r, "dry_run") {
		opts.DryRun = true
	}
	return opts
}

// parseRewriteManifestsOpts reads ?dry_run=true from the request.
// RewriteManifests has no other operator-facing knobs yet.
func parseRewriteManifestsOpts(r *http.Request) maintenance.RewriteManifestsOptions {
	return maintenance.RewriteManifestsOptions{
		DryRun: parseBoolQuery(r, "dry_run"),
	}
}

// handleExpireAsync accepts an expire request and runs it in a
// background goroutine. Returns 202 Accepted with a job ID immediately.
func (s *server) handleExpireAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	expireOpts := parseExpireOpts(r)
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job, isNew := s.jobs.create(tableName, "expire")
	if !isNew {
		s.logger.Info("expire job dedup — returning in-flight job",
			"job_id", job.ID, "table", tableName)
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	s.logger.Info("expire job created", "job_id", job.ID, "table", tableName,
		"keep_last", expireOpts.KeepLast, "keep_within", expireOpts.KeepWithin)

	go func() {
		s.jobs.setRunning(job.ID)
		ctx := context.Background()
		result, err := maintenance.Expire(ctx, s.cat, ident, expireOpts)
		s.jobs.complete(job.ID, result, err)
		if err != nil {
			s.logger.Error("expire job failed", "job_id", job.ID, "table", tableName, "err", err)
		} else {
			s.logger.Info("expire job completed", "job_id", job.ID, "table", tableName,
				"removed", len(result.RemovedSnapshotIDs), "elapsed_ms", result.DurationMs)
		}
	}()

	writeJSON(w, http.StatusAccepted, job)
}

// handleRewriteManifestsAsync accepts a rewrite-manifests request and
// runs it in a background goroutine.
func (s *server) handleRewriteManifestsAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	rewriteOpts := parseRewriteManifestsOpts(r)
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job, isNew := s.jobs.create(tableName, "rewrite-manifests")
	if !isNew {
		s.logger.Info("rewrite-manifests job dedup — returning in-flight job",
			"job_id", job.ID, "table", tableName)
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	s.logger.Info("rewrite-manifests job created", "job_id", job.ID, "table", tableName)

	go func() {
		s.jobs.setRunning(job.ID)
		ctx := context.Background()
		result, err := maintenance.RewriteManifests(ctx, s.cat, ident, rewriteOpts)
		s.jobs.complete(job.ID, result, err)
		if err != nil {
			s.logger.Error("rewrite-manifests job failed", "job_id", job.ID, "table", tableName, "err", err)
		} else {
			s.logger.Info("rewrite-manifests job completed", "job_id", job.ID, "table", tableName,
				"before_manifests", result.BeforeManifests, "after_manifests", result.AfterManifests,
				"elapsed_ms", result.DurationMs)
		}
	}()

	writeJSON(w, http.StatusAccepted, job)
}

// handleMaintainAsync runs the full maintenance cycle against one
// table in the correct order:
//
//  1. expire              — drop old snapshots from the parent chain
//  2. rewrite-manifests   — consolidate surviving micro-manifests
//  3. compact             — rewrite small files (runs fast because
//                           the manifest list is already small)
//  4. rewrite-manifests   — fold compact's new micro-manifest back
//                           into the partition-organized layout
//
// This ordering is load-bearing. Steps 1+2 clean the metadata layer
// BEFORE compact runs, so compact walks fewer manifests and wins the
// CAS race against concurrent writers. Step 4 re-consolidates after
// compact's commit adds a new micro-manifest, keeping the metadata
// layer bounded for the NEXT maintenance cycle. Run 13's A/B bench
// proved the closed loop: without expire+rewrite the manifest list
// grows faster than compact can drain it, and every subsequent
// compact loses the writer-fight.
//
// Each step runs in sequence within a single background goroutine.
// The job tracks the combined result. If any step fails, subsequent
// steps are skipped and the job records the failure.
// handleMaintainAsync accepts query params for all sub-ops:
//
//	?partition=col=value         — scope compact to one partition
//	?target_file_size=1MB        — Pattern B threshold
//	?keep_last=N                 — snapshots to retain
//	?keep_within=DUR             — minimum age before expire
//	?dry_run=true                — plan every sub-op but skip all writes
//	                                and commits. Each sub-op's result
//	                                carries dry_run + contention_detected
//	                                so the client can decide whether to
//	                                re-run with dry_run=false.
func (s *server) handleMaintainAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])

	// Load the table once here (cheap) so we can classify it and pick
	// the right plan before spawning the job. If the table doesn't
	// exist, fail fast with 404 instead of 202 + async error.
	tbl, err := s.cat.LoadTable(r.Context(), ident)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "table not found", err)
		return
	}

	// Classify the table from its commit history. The classifier is
	// entirely offline — it walks the snapshot chain and counts
	// foreign commits in the last 24h.
	cr := classify.Classify(tbl)
	plan := classify.ClassToOptions(cr.Class)

	// Query params are OPTIONAL overrides. Callers who pass nothing
	// get the class-driven defaults; callers who pass values get them
	// merged over the plan.
	if v := r.URL.Query().Get("mode"); v != "" {
		plan.Mode = classify.MaintainMode(v)
	}
	dryRun := parseBoolQuery(r, "dry_run")

	job, isNew := s.jobs.create(tableName, "maintain")
	if !isNew {
		// In-flight guard: a previous maintain POST for this table
		// is still running. Return its job_id instead of spawning
		// a duplicate — the client can poll the same ID and will
		// see the running job complete. This prevents the Run 18
		// self-collision pathology where a client timeout fired
		// mid-maintain and the client retried, causing two
		// concurrent maintain jobs on the same table to fight for
		// the same metadata.json CAS.
		s.logger.Info("maintain job dedup — returning in-flight job",
			"job_id", job.ID, "table", tableName)
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	s.logger.Info("maintain job created",
		"job_id", job.ID,
		"table", tableName,
		"class", cr.Class,
		"mode", plan.Mode,
		"keep_last", plan.KeepLastSnapshots,
		"keep_within", plan.KeepWithin,
		"target_file_size", plan.TargetFileSizeBytes,
		"dry_run", dryRun,
	)

	go s.runMaintainJob(job.ID, ident, plan, dryRun)

	writeJSON(w, http.StatusAccepted, job)
}

// maintainResult aggregates the outcomes of the three maintenance
// steps so the caller can see exactly what happened in each phase.
type maintainResult struct {
	Class              string                              `json:"class"`
	Mode               string                              `json:"mode"`
	DryRun             bool                                `json:"dry_run,omitempty"`
	Expire             *maintenance.ExpireResult           `json:"expire,omitempty"`
	RewriteManifests   *maintenance.RewriteManifestsResult `json:"rewrite_manifests,omitempty"`
	CompactFull        *janitor.CompactTableResult         `json:"compact_full,omitempty"`
	CompactHot         *janitor.CompactHotResult           `json:"compact_hot,omitempty"`
	CompactCold        *janitor.CompactColdResult          `json:"compact_cold,omitempty"`
	PostCompactRewrite *maintenance.RewriteManifestsResult `json:"post_compact_rewrite,omitempty"`
	Steps              []string                            `json:"steps_completed"`
	TotalDurationMs    int64                               `json:"total_duration_ms"`
	MetadataLocation   string                              `json:"metadata_location,omitempty"`
}

func (s *server) runMaintainJob(jobID string, ident icebergtable.Identifier, plan classify.MaintainOptions, dryRun bool) {
	s.jobs.setRunning(jobID)
	started := time.Now()
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	s.logger.Info("maintain job started", "job_id", jobID, "table", tableName, "mode", plan.Mode, "dry_run", dryRun)

	ctx := context.Background()
	mr := &maintainResult{Mode: string(plan.Mode), DryRun: dryRun}

	// Step 1: expire — drop old snapshots so the manifest list
	// we're about to consolidate doesn't include dead references.
	expireOpts := maintenance.ExpireOptions{
		KeepLast:   plan.KeepLastSnapshots,
		KeepWithin: plan.KeepWithin,
		DryRun:     dryRun,
	}
	expireResult, err := maintenance.Expire(ctx, s.cat, ident, expireOpts)
	mr.Expire = expireResult
	if err != nil {
		mr.TotalDurationMs = time.Since(started).Milliseconds()
		s.jobs.complete(jobID, mr, fmt.Errorf("expire: %w", err))
		s.logger.Error("maintain job failed at expire", "job_id", jobID, "table", tableName, "err", err)
		return
	}
	mr.Steps = append(mr.Steps, "expire")
	s.logger.Info("maintain: expire done", "job_id", jobID, "table", tableName,
		"removed", len(expireResult.RemovedSnapshotIDs), "elapsed_ms", expireResult.DurationMs)

	// Step 2: rewrite-manifests (pre-compact) — consolidate the
	// surviving snapshot's micro-manifests so compact walks a small
	// manifest list and wins the CAS race against the writer.
	preRewrite, err := maintenance.RewriteManifests(ctx, s.cat, ident, maintenance.RewriteManifestsOptions{DryRun: dryRun})
	mr.RewriteManifests = preRewrite
	if err != nil {
		mr.TotalDurationMs = time.Since(started).Milliseconds()
		s.jobs.complete(jobID, mr, fmt.Errorf("rewrite-manifests (pre-compact): %w", err))
		s.logger.Error("maintain job failed at rewrite-manifests (pre-compact)", "job_id", jobID, "table", tableName, "err", err)
		return
	}
	mr.Steps = append(mr.Steps, "rewrite-manifests")
	s.logger.Info("maintain: rewrite-manifests (pre-compact) done", "job_id", jobID, "table", tableName,
		"before_manifests", preRewrite.BeforeManifests, "after_manifests", preRewrite.AfterManifests,
		"elapsed_ms", preRewrite.DurationMs)

	// Step 3: compact. Dispatch by mode from the class-driven plan:
	//
	//   hot  — CompactHot: delta-stitch active partitions only
	//   cold — CompactCold: trigger-based full compaction of cold partitions
	//   full — CompactTable: parallel full compaction of all partitions (legacy)
	//
	// hot and cold are complementary — they touch disjoint partition
	// sets. Running "full" is the safe override that handles everything
	// in one pass.
	var compactErr error
	switch plan.Mode {
	case classify.ModeHot:
		hotResult, err := janitor.CompactHot(ctx, s.cat, ident, janitor.CompactHotOptions{
			SmallFileThresholdBytes: plan.TargetFileSizeBytes,
			HotWindowSnapshots:      5,
			MinSmallFiles:           2,
			PartitionConcurrency:    partitionConcurrencyFromEnv(),
			DryRun:                  dryRun,
		})
		mr.CompactHot = hotResult
		compactErr = err
		if err == nil && hotResult != nil {
			s.logger.Info("maintain: compact_hot done", "job_id", jobID, "table", tableName,
				"partitions_hot", hotResult.PartitionsHot,
				"partitions_stitched", hotResult.PartitionsStitched,
				"partitions_failed", hotResult.PartitionsFailed,
				"elapsed_ms", hotResult.TotalDurationMs)
		}
	case classify.ModeCold:
		coldResult, err := janitor.CompactCold(ctx, s.cat, ident, janitor.CompactColdOptions{
			SmallFileThresholdBytes: plan.TargetFileSizeBytes,
			SmallFileTrigger:        plan.SmallFileThreshold,
			FileCountTrigger:        200,
			StaleRewriteAge:         plan.StaleRewriteAge,
			HotWindowSnapshots:      5,
			TargetFileSizeBytes:     plan.TargetFileSizeBytes,
			DryRun:                  dryRun,
		})
		mr.CompactCold = coldResult
		compactErr = err
		if err == nil && coldResult != nil {
			s.logger.Info("maintain: compact_cold done", "job_id", jobID, "table", tableName,
				"partitions_cold", coldResult.PartitionsCold,
				"partitions_triggered", coldResult.PartitionsTriggered,
				"partitions_compacted", coldResult.PartitionsCompacted,
				"partitions_failed", coldResult.PartitionsFailed,
				"elapsed_ms", coldResult.TotalDurationMs)
		}
	default: // ModeFull or unknown
		compactResult, err := janitor.CompactTable(ctx, s.cat, ident, janitor.CompactOptions{
			TargetFileSizeBytes: plan.TargetFileSizeBytes,
			DryRun:              dryRun,
		})
		mr.CompactFull = compactResult
		compactErr = err
		if err == nil && compactResult != nil {
			s.logger.Info("maintain: compact_full done", "job_id", jobID, "table", tableName,
				"partitions_found", compactResult.PartitionsFound,
				"partitions_compacted", compactResult.PartitionsSucceeded,
				"partitions_failed", compactResult.PartitionsFailed,
				"elapsed_ms", compactResult.TotalDurationMs)
		}
	}
	if compactErr != nil {
		mr.TotalDurationMs = time.Since(started).Milliseconds()
		s.jobs.complete(jobID, mr, fmt.Errorf("compact: %w", compactErr))
		s.logger.Error("maintain job failed at compact", "job_id", jobID, "table", tableName, "err", compactErr)
		return
	}
	mr.Steps = append(mr.Steps, "compact")

	// Step 4: rewrite-manifests (post-compact) — fold compact's new
	// micro-manifest back into the partition-organized layout so the
	// NEXT maintenance cycle starts with a clean manifest list.
	postRewrite, err := maintenance.RewriteManifests(ctx, s.cat, ident, maintenance.RewriteManifestsOptions{DryRun: dryRun})
	mr.PostCompactRewrite = postRewrite
	if err != nil {
		mr.TotalDurationMs = time.Since(started).Milliseconds()
		s.jobs.complete(jobID, mr, fmt.Errorf("rewrite-manifests (post-compact): %w", err))
		s.logger.Error("maintain job failed at rewrite-manifests (post-compact)", "job_id", jobID, "table", tableName, "err", err)
		return
	}
	mr.Steps = append(mr.Steps, "rewrite-manifests (post-compact)")
	s.logger.Info("maintain: rewrite-manifests (post-compact) done", "job_id", jobID, "table", tableName,
		"before_manifests", postRewrite.BeforeManifests, "after_manifests", postRewrite.AfterManifests,
		"elapsed_ms", postRewrite.DurationMs)

	// Capture the final metadata_location so the orchestration layer
	// (bench.sh, event dispatcher) can update external catalogs
	// (Glue, Unity, Polaris) with a direct pointer instead of
	// running expensive S3 prefix discovery.
	if finalTbl, lerr := s.cat.LoadTable(ctx, ident); lerr == nil {
		mr.MetadataLocation = finalTbl.MetadataLocation()
	}

	mr.TotalDurationMs = time.Since(started).Milliseconds()
	s.jobs.complete(jobID, mr, nil)
	s.logger.Info("maintain job completed", "job_id", jobID, "table", tableName,
		"mode", plan.Mode, "steps", mr.Steps, "total_ms", mr.TotalDurationMs,
		"metadata_location", mr.MetadataLocation)
}

// partitionConcurrencyFromEnv reads JANITOR_PARTITION_CONCURRENCY from
// the environment so operators can tune CompactHot parallelism without
// rebuilding the image. Returns 0 (= use default) if unset or invalid.
func partitionConcurrencyFromEnv() int {
	v := os.Getenv("JANITOR_PARTITION_CONCURRENCY")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0
	}
	return n
}

// handleJobStatus returns the current status of a job.
func (s *server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "missing job id", nil)
		return
	}

	job, ok := s.jobs.get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "job not found", nil)
		return
	}

	writeJSON(w, http.StatusOK, job)
}
