package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"

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

// handleCompactAsync accepts a compact request, starts the work in a
// goroutine, and returns 202 Accepted with a job ID immediately.
func (s *server) handleCompactAsync(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}

	tableName := fmt.Sprintf("%s.%s", ident[0], ident[1])
	job := s.jobs.create(tableName, "compact")

	s.logger.Info("compact job created",
		"job_id", job.ID,
		"namespace", ident[0],
		"table", ident[1],
	)

	// Run compact in a background goroutine.
	go s.runCompactJob(job.ID, ident)

	writeJSON(w, http.StatusAccepted, job)
}

func (s *server) runCompactJob(jobID string, ident icebergtable.Identifier) {
	s.jobs.setRunning(jobID)
	started := time.Now()
	s.logger.Info("compact job started",
		"job_id", jobID,
		"table", fmt.Sprintf("%s.%s", ident[0], ident[1]),
	)

	ctx := context.Background()
	result, err := janitor.Compact(ctx, s.cat, ident, janitor.CompactOptions{})

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
