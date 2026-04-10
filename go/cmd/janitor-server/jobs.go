package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"

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
}

// jobStore is a simple in-memory job tracker.
type jobStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func newJobStore() *jobStore {
	return &jobStore{jobs: make(map[string]*Job)}
}

func (s *jobStore) create(table, operation string) *Job {
	j := &Job{
		ID:        uuid.New().String(),
		Status:    "pending",
		Table:     table,
		Operation: operation,
		CreatedAt: time.Now(),
	}
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	return j
}

func (s *jobStore) get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *jobStore) complete(id string, result any, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
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
}

func (s *jobStore) setRunning(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok {
		j.Status = "running"
	}
}

// parseCompactOpts reads optional query parameters:
//
//	?partition=col=value         — scope to one partition
//	?target_file_size=1MB        — Pattern B threshold (KB/MB/GB suffixes)
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
	return opts
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
	job := s.jobs.create(tableName, "compact")

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
		s.logger.Info("compact job completed",
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
		)
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
	return opts
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
	job := s.jobs.create(tableName, "expire")
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
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job := s.jobs.create(tableName, "rewrite-manifests")
	s.logger.Info("rewrite-manifests job created", "job_id", job.ID, "table", tableName)

	go func() {
		s.jobs.setRunning(job.ID)
		ctx := context.Background()
		result, err := maintenance.RewriteManifests(ctx, s.cat, ident, maintenance.RewriteManifestsOptions{})
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
func (s *server) handleMaintainAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	expireOpts := parseExpireOpts(r)
	compactOpts := parseCompactOpts(r)
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job := s.jobs.create(tableName, "maintain")
	s.logger.Info("maintain job created", "job_id", job.ID, "table", tableName,
		"keep_last", expireOpts.KeepLast, "keep_within", expireOpts.KeepWithin)

	go s.runMaintainJob(job.ID, ident, expireOpts, compactOpts)

	writeJSON(w, http.StatusAccepted, job)
}

// maintainResult aggregates the outcomes of the three maintenance
// steps so the caller can see exactly what happened in each phase.
type maintainResult struct {
	Expire              *maintenance.ExpireResult              `json:"expire,omitempty"`
	RewriteManifests    *maintenance.RewriteManifestsResult    `json:"rewrite_manifests,omitempty"`
	Compact             *janitor.CompactTableResult            `json:"compact,omitempty"`
	PostCompactRewrite  *maintenance.RewriteManifestsResult    `json:"post_compact_rewrite,omitempty"`
	Steps               []string                               `json:"steps_completed"`
	TotalDurationMs     int64                                  `json:"total_duration_ms"`
}

func (s *server) runMaintainJob(jobID string, ident icebergtable.Identifier, expireOpts maintenance.ExpireOptions, compactOpts janitor.CompactOptions) {
	s.jobs.setRunning(jobID)
	started := time.Now()
	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	s.logger.Info("maintain job started", "job_id", jobID, "table", tableName)

	ctx := context.Background()
	mr := &maintainResult{}

	// Step 1: expire — drop old snapshots so the manifest list
	// we're about to consolidate doesn't include dead references.
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
	preRewrite, err := maintenance.RewriteManifests(ctx, s.cat, ident, maintenance.RewriteManifestsOptions{})
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

	// Step 3: compact ALL partitions with small files in parallel.
	// Runs fast because the manifest list was just consolidated.
	// CompactTable discovers partitions, fires a bounded worker pool
	// (8 concurrent), and compacts each independently. Single-
	// partition failures don't abort the others.
	compactResult, err := janitor.CompactTable(ctx, s.cat, ident, compactOpts)
	mr.Compact = compactResult
	if err != nil {
		mr.TotalDurationMs = time.Since(started).Milliseconds()
		s.jobs.complete(jobID, mr, fmt.Errorf("compact: %w", err))
		s.logger.Error("maintain job failed at compact", "job_id", jobID, "table", tableName, "err", err)
		return
	}
	mr.Steps = append(mr.Steps, "compact")
	s.logger.Info("maintain: compact done", "job_id", jobID, "table", tableName,
		"partitions_found", compactResult.PartitionsFound,
		"partitions_compacted", compactResult.PartitionsSucceeded,
		"partitions_failed", compactResult.PartitionsFailed,
		"elapsed_ms", compactResult.TotalDurationMs)

	// Step 4: rewrite-manifests (post-compact) — fold compact's new
	// micro-manifest back into the partition-organized layout so the
	// NEXT maintenance cycle starts with a clean manifest list.
	postRewrite, err := maintenance.RewriteManifests(ctx, s.cat, ident, maintenance.RewriteManifestsOptions{})
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

	mr.TotalDurationMs = time.Since(started).Milliseconds()
	s.jobs.complete(jobID, mr, nil)
	s.logger.Info("maintain job completed", "job_id", jobID, "table", tableName,
		"steps", mr.Steps, "total_ms", mr.TotalDurationMs)
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
