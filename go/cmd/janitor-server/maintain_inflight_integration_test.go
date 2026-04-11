package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	_ "gocloud.dev/blob/fileblob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestMaintainInFlightGuard_Integration is the load-bearing test for
// the per-table dedup primitive. It constructs a real fileblob-backed
// server, seeds a real Iceberg table, then fires N concurrent HTTP
// POSTs to /v1/tables/{ns}/{name}/maintain and asserts that the
// server dedups them into a SINGLE job — all responses return the
// same job_id.
//
// This is the integration test that proves Run 18's self-collision
// pathology cannot reoccur: the old server would spawn N parallel
// maintain jobs on the same table and they'd fight for the same
// metadata.json CAS, exhausting retries and marking partitions
// failed. With the in-flight guard, the 2nd-Nth POSTs find an
// existing job in the store and return its ID instead of creating
// a new goroutine.
//
// The assertion is: exactly ONE unique job_id across all N
// responses AND exactly ONE row in the server's job store.
func TestMaintainInFlightGuard_Integration(t *testing.T) {
	// Seed a real fileblob warehouse with an Iceberg table.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "inflight", "events", 6, 10)

	// Build a server pointing at the same warehouse. newTestServer
	// creates its own TempDir, so we can't reuse it directly — we
	// build a server by hand rooted at the seeded warehouse.
	srv := &server{
		cat:     w.Cat,
		logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		started: time.Now(),
		jobs:    newJobStore(),
	}

	// Stand up a real HTTP server (not direct handler calls) so the
	// goroutines compete through Go's net/http dispatcher the same
	// way they would in production.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/maintain", srv.handleMaintainAsync)
	mux.HandleFunc("GET /v1/jobs/{id}", srv.handleJobStatus)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	const N = 16
	url := ts.URL + "/v1/tables/inflight/events/maintain"

	// Fire N concurrent POSTs. Each goroutine records the job_id
	// returned by the server. After all N complete, we assert
	// exactly one unique job_id.
	type result struct {
		status int
		jobID  string
		err    error
	}
	results := make([]result, N)
	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start // release all at once
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
			if err != nil {
				results[i] = result{err: err}
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				results[i] = result{err: err}
				return
			}
			defer resp.Body.Close()
			var body struct {
				JobID string `json:"job_id"`
			}
			if derr := json.NewDecoder(resp.Body).Decode(&body); derr != nil {
				results[i] = result{status: resp.StatusCode, err: derr}
				return
			}
			results[i] = result{status: resp.StatusCode, jobID: body.JobID}
		}()
	}
	close(start)
	wg.Wait()

	// Assert: every request got a 202 with a non-empty job_id, and
	// ALL job_ids are identical.
	var firstID string
	unique := map[string]int{}
	for i, r := range results {
		if r.err != nil {
			t.Errorf("request %d: unexpected error: %v", i, r.err)
			continue
		}
		if r.status != http.StatusAccepted {
			t.Errorf("request %d: status = %d, want 202", i, r.status)
		}
		if r.jobID == "" {
			t.Errorf("request %d: empty job_id", i)
			continue
		}
		if firstID == "" {
			firstID = r.jobID
		}
		unique[r.jobID]++
	}

	if len(unique) != 1 {
		t.Errorf("expected 1 unique job_id across %d concurrent maintain POSTs, got %d: %v",
			N, len(unique), unique)
	}
	if unique[firstID] != N {
		t.Errorf("expected all %d requests to share job_id %s, got %d", N, firstID, unique[firstID])
	}

	// Assert: the jobStore contains exactly ONE job for this table.
	// (Other operations — compact, expire — wouldn't be counted
	// because the scenario only POSTs /maintain.)
	srv.jobs.mu.RLock()
	jobCount := 0
	for _, j := range srv.jobs.jobs {
		if j.Operation == "maintain" && j.Table == "inflight.events" {
			jobCount++
		}
	}
	srv.jobs.mu.RUnlock()
	if jobCount != 1 {
		t.Errorf("jobStore contains %d maintain jobs for inflight.events, want 1", jobCount)
	}

	// Wait for the in-flight job to finish so the goroutine doesn't
	// outlive the test (would leak goroutine + file handles).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		j, ok := srv.jobs.get(firstID)
		if !ok {
			break
		}
		if j.Status == "completed" || j.Status == "failed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestMaintainInFlightGuard_SequentialAllowed verifies the flip side:
// after a maintain job COMPLETES, a subsequent POST for the same
// table must be allowed to start a FRESH job. This is what makes
// the benchmark's MAINTAIN_ROUNDS > 1 loop work correctly — each
// round must actually run.
func TestMaintainInFlightGuard_SequentialAllowed(t *testing.T) {
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "inflight", "sequential", 4, 5)

	srv := &server{
		cat:     w.Cat,
		logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		started: time.Now(),
		jobs:    newJobStore(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/maintain", srv.handleMaintainAsync)
	mux.HandleFunc("GET /v1/jobs/{id}", srv.handleJobStatus)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	url := ts.URL + "/v1/tables/inflight/sequential/maintain"

	firstID := postMaintain(t, url)
	waitForJobDone(t, srv, firstID, 30*time.Second)
	secondID := postMaintain(t, url)
	if secondID == "" {
		t.Fatal("second POST returned empty job_id")
	}
	if secondID == firstID {
		t.Errorf("expected fresh job on second POST after completion, got same ID %s", firstID)
	}
	waitForJobDone(t, srv, secondID, 30*time.Second)
}

func postMaintain(t *testing.T, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post status = %d, want 202", resp.StatusCode)
	}
	var body struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body.JobID
}

func waitForJobDone(t *testing.T, srv *server, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, ok := srv.jobs.get(id)
		if ok && (j.Status == "completed" || j.Status == "failed") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish within %v", id, timeout)
}
