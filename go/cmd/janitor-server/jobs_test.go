package main

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseFileSizeServer(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"1024", 1024, false},
		{"1KB", 1024, false},
		{"1kb", 1024, false},
		{"16MB", 16 * 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"  32MB  ", 32 * 1024 * 1024, false},
		{"", 0, true},
		{"abc", 0, true},
		{"-1", 0, true},
		{"0", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseFileSizeServer(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseFileSizeServer(%q) expected error, got %d", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseFileSizeServer(%q) unexpected error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseFileSizeServer(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseCompactOptsEmpty(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/tables/ns/t/compact", nil)
	opts := parseCompactOpts(r)
	if opts.PartitionTuple != nil {
		t.Errorf("empty query: PartitionTuple = %v, want nil", opts.PartitionTuple)
	}
	if opts.TargetFileSizeBytes != 0 {
		t.Errorf("empty query: TargetFileSizeBytes = %d, want 0", opts.TargetFileSizeBytes)
	}
}

func TestParseCompactOptsPartition(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/tables/ns/t/compact?partition=ss_store_sk=5", nil)
	opts := parseCompactOpts(r)
	if opts.PartitionTuple == nil {
		t.Fatal("expected PartitionTuple to be set")
	}
	if opts.PartitionTuple["ss_store_sk"] != "5" {
		t.Errorf("got %v, want {ss_store_sk: 5}", opts.PartitionTuple)
	}
}

func TestParseCompactOptsMalformedPartition(t *testing.T) {
	// partition without = should be silently ignored rather than
	// crash or produce {col: ""}.
	r := httptest.NewRequest("POST", "/v1/tables/ns/t/compact?partition=bogus", nil)
	opts := parseCompactOpts(r)
	if opts.PartitionTuple != nil {
		t.Errorf("malformed partition: got %v, want nil", opts.PartitionTuple)
	}
}

func TestParseCompactOptsTargetFileSize(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/tables/ns/t/compact?target_file_size=2MB", nil)
	opts := parseCompactOpts(r)
	if opts.TargetFileSizeBytes != 2*1024*1024 {
		t.Errorf("TargetFileSizeBytes = %d, want %d", opts.TargetFileSizeBytes, 2*1024*1024)
	}
}

func TestParseCompactOptsCombined(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/tables/ns/t/compact?partition=col=v&target_file_size=1GB", nil)
	opts := parseCompactOpts(r)
	if opts.PartitionTuple["col"] != "v" {
		t.Errorf("PartitionTuple = %v", opts.PartitionTuple)
	}
	if opts.TargetFileSizeBytes != 1024*1024*1024 {
		t.Errorf("TargetFileSizeBytes = %d", opts.TargetFileSizeBytes)
	}
}

func TestParseExpireOpts(t *testing.T) {
	cases := []struct {
		query          string
		wantKeepLast   int
		wantKeepWithin time.Duration
	}{
		{"", 0, 0},
		{"keep_last=10", 10, 0},
		{"keep_within=24h", 0, 24 * time.Hour},
		{"keep_last=5&keep_within=1h30m", 5, 90 * time.Minute},
		{"keep_last=-1", 0, 0},    // negative ignored
		{"keep_last=abc", 0, 0},   // non-numeric ignored
		{"keep_within=-1h", 0, 0}, // negative ignored
		{"keep_within=bogus", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/tables/ns/t/expire?"+tc.query, nil)
			opts := parseExpireOpts(r)
			if opts.KeepLast != tc.wantKeepLast {
				t.Errorf("KeepLast = %d, want %d", opts.KeepLast, tc.wantKeepLast)
			}
			if opts.KeepWithin != tc.wantKeepWithin {
				t.Errorf("KeepWithin = %v, want %v", opts.KeepWithin, tc.wantKeepWithin)
			}
		})
	}
}

func TestJobStoreCreateAndGet(t *testing.T) {
	s := newJobStore()
	j, _ := s.create("tpcds.store_sales", "compact")

	if j.ID == "" {
		t.Error("create: empty ID")
	}
	if j.Status != "pending" {
		t.Errorf("create: Status = %q, want pending", j.Status)
	}
	if j.Table != "tpcds.store_sales" {
		t.Errorf("create: Table = %q, want tpcds.store_sales", j.Table)
	}
	if j.Operation != "compact" {
		t.Errorf("create: Operation = %q, want compact", j.Operation)
	}
	if j.CreatedAt.IsZero() {
		t.Error("create: CreatedAt not set")
	}

	got, ok := s.get(j.ID)
	if !ok {
		t.Fatal("get: not found")
	}
	if got.ID != j.ID {
		t.Errorf("get: ID = %q, want %q", got.ID, j.ID)
	}
}

func TestJobStoreGetMissing(t *testing.T) {
	s := newJobStore()
	_, ok := s.get("nonexistent")
	if ok {
		t.Error("expected get(nonexistent) to return false")
	}
}

func TestJobStoreSetRunning(t *testing.T) {
	s := newJobStore()
	j, _ := s.create("ns.tbl", "compact")
	s.setRunning(j.ID)
	got, _ := s.get(j.ID)
	if got.Status != "running" {
		t.Errorf("Status after setRunning = %q, want running", got.Status)
	}
}

func TestJobStoreSetRunningMissing(t *testing.T) {
	// setRunning on unknown id should be a no-op, not panic.
	s := newJobStore()
	s.setRunning("nonexistent")
}

func TestJobStoreCompleteSuccess(t *testing.T) {
	s := newJobStore()
	j, _ := s.create("ns.tbl", "compact")
	result := map[string]any{"files": 42, "bytes": 1024}
	s.complete(j.ID, result, nil)

	got, _ := s.get(j.ID)
	if got.Status != "completed" {
		t.Errorf("Status = %q, want completed", got.Status)
	}
	if got.DoneAt == nil {
		t.Error("DoneAt not set")
	}
	if got.Error != "" {
		t.Errorf("Error = %q, want empty on success", got.Error)
	}
	if len(got.Result) == 0 {
		t.Error("Result payload not set")
	}
}

func TestJobStoreCompleteFailure(t *testing.T) {
	s := newJobStore()
	j, _ := s.create("ns.tbl", "compact")
	s.complete(j.ID, nil, errors.New("disk full"))

	got, _ := s.get(j.ID)
	if got.Status != "failed" {
		t.Errorf("Status = %q, want failed", got.Status)
	}
	if got.Error != "disk full" {
		t.Errorf("Error = %q, want 'disk full'", got.Error)
	}
}

func TestJobStoreCompleteMissing(t *testing.T) {
	// complete on unknown id should be a no-op, not panic.
	s := newJobStore()
	s.complete("nonexistent", nil, nil)
}

func TestJobStoreConcurrentAccess(t *testing.T) {
	// Smoke test for mutex correctness — if the store isn't thread-safe
	// this will either race (under -race) or corrupt state. Each
	// goroutine uses a different table so dedup never fires.
	s := newJobStore()
	const n = 50
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			j, _ := s.create(fmt.Sprintf("ns.t%d", i), "op")
			s.setRunning(j.ID)
			s.complete(j.ID, nil, nil)
			_, _ = s.get(j.ID)
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
}

// === In-flight guard tests ===
//
// These tests cover the per-table dedup primitive the server uses
// to prevent concurrent maintain POSTs from creating duplicate
// jobs that race for the same metadata.json CAS. This is
// load-bearing logic: without it, a retrying client can stack up
// N parallel maintain jobs on the same table and cause the
// self-collision pathology Run 18 surfaced.

func TestJobStoreCreate_IsNewOnFirstCall(t *testing.T) {
	s := newJobStore()
	j, isNew := s.create("ns.tbl", "maintain")
	if !isNew {
		t.Errorf("first create: isNew = false, want true")
	}
	if j.ID == "" {
		t.Errorf("first create: empty ID")
	}
	if j.Status != "pending" {
		t.Errorf("first create: status = %q, want pending", j.Status)
	}
}

func TestJobStoreCreate_DedupsInFlightMaintain(t *testing.T) {
	s := newJobStore()
	first, firstIsNew := s.create("ns.tbl", "maintain")
	if !firstIsNew {
		t.Fatalf("first create must be new")
	}
	second, secondIsNew := s.create("ns.tbl", "maintain")
	if secondIsNew {
		t.Errorf("second create on same (op,table): isNew = true, want false (dedup)")
	}
	if second.ID != first.ID {
		t.Errorf("dedup returned different job: first=%s second=%s", first.ID, second.ID)
	}
}

func TestJobStoreCreate_DedupsRunningMaintain(t *testing.T) {
	// Transition to running between the first and second create.
	// The second create must still dedup (running is an in-flight state).
	s := newJobStore()
	first, _ := s.create("ns.tbl", "maintain")
	s.setRunning(first.ID)
	second, isNew := s.create("ns.tbl", "maintain")
	if isNew {
		t.Errorf("second create on running job: isNew = true, want false")
	}
	if second.ID != first.ID {
		t.Errorf("dedup returned different job")
	}
}

func TestJobStoreCreate_NewJobAfterCompletion(t *testing.T) {
	// After a job completes, a new create for the same (op,table)
	// must be allowed to start a fresh job. The in-flight slot is
	// released on complete.
	s := newJobStore()
	first, _ := s.create("ns.tbl", "maintain")
	s.complete(first.ID, nil, nil)
	second, isNew := s.create("ns.tbl", "maintain")
	if !isNew {
		t.Errorf("create after complete: isNew = false, want true")
	}
	if second.ID == first.ID {
		t.Errorf("expected fresh job after complete, got same ID %s", first.ID)
	}
}

func TestJobStoreCreate_NewJobAfterFailure(t *testing.T) {
	// Same as completion — a failed job releases the in-flight slot.
	s := newJobStore()
	first, _ := s.create("ns.tbl", "maintain")
	s.complete(first.ID, nil, errors.New("disk full"))
	second, isNew := s.create("ns.tbl", "maintain")
	if !isNew {
		t.Errorf("create after fail: isNew = false, want true")
	}
	if second.ID == first.ID {
		t.Errorf("expected fresh job after fail, got same ID %s", first.ID)
	}
}

func TestJobStoreCreate_DifferentTablesDoNotDedup(t *testing.T) {
	// Same operation on different tables must not dedup — they're
	// independent CAS targets.
	s := newJobStore()
	a, aNew := s.create("ns.tblA", "maintain")
	b, bNew := s.create("ns.tblB", "maintain")
	if !aNew || !bNew {
		t.Errorf("independent tables should both be new: aNew=%v bNew=%v", aNew, bNew)
	}
	if a.ID == b.ID {
		t.Errorf("independent tables returned same job ID")
	}
}

func TestJobStoreCreate_DifferentOperationsDoNotDedup(t *testing.T) {
	// Same table with different operations must not dedup — compact
	// and maintain can run concurrently on the same table (in theory;
	// in practice the orchestrator avoids it, but the primitive
	// should allow it).
	s := newJobStore()
	a, aNew := s.create("ns.tbl", "maintain")
	b, bNew := s.create("ns.tbl", "compact")
	if !aNew || !bNew {
		t.Errorf("independent operations should both be new: aNew=%v bNew=%v", aNew, bNew)
	}
	if a.ID == b.ID {
		t.Errorf("independent operations returned same job ID")
	}
}

func TestJobStoreCreate_ConcurrentCreateSameTable(t *testing.T) {
	// Race-detector test (go test -race). Many goroutines hammer
	// create() for the same (op,table) simultaneously. At most ONE
	// should succeed with isNew=true; all others must dedup to that
	// same job_id. Also verifies the mutex correctness under
	// heavy contention.
	s := newJobStore()
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	newCount := int64(0)
	idCh := make(chan string, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			j, isNew := s.create("ns.tbl", "maintain")
			if isNew {
				atomic.AddInt64(&newCount, 1)
			}
			idCh <- j.ID
		}()
	}
	wg.Wait()
	close(idCh)

	if newCount != 1 {
		t.Errorf("expected exactly 1 isNew=true under concurrent create, got %d", newCount)
	}
	// All goroutines must see the SAME job ID.
	var firstID string
	for id := range idCh {
		if firstID == "" {
			firstID = id
		} else if id != firstID {
			t.Errorf("concurrent create returned different IDs: %s vs %s", firstID, id)
		}
	}
}

func TestJobStoreCreate_ConcurrentCreateAndComplete(t *testing.T) {
	// Scenario: client 1 creates a job. Client 2 starts racing to
	// create. Somewhere in that race, client 1's job completes.
	// The invariants: (a) no data race, (b) total new-job count
	// equals the number of completions + 1, (c) every returned
	// job ID corresponds to a real entry in the store.
	s := newJobStore()
	const rounds = 20
	const goroutines = 16
	var wg sync.WaitGroup
	newCount := int64(0)
	errs := make(chan error, rounds*goroutines)

	for r := 0; r < rounds; r++ {
		// Burst of concurrent creates.
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				j, isNew := s.create("ns.tbl", "maintain")
				if isNew {
					atomic.AddInt64(&newCount, 1)
				}
				if _, ok := s.get(j.ID); !ok {
					errs <- fmt.Errorf("returned job id %s not in store", j.ID)
					return
				}
			}()
		}
		wg.Wait()
		// Complete whatever's in-flight to free the slot for the next round.
		// Find the current in-flight job for maintain|ns.tbl.
		s.mu.RLock()
		inflightID := s.inflight["maintain|ns.tbl"]
		s.mu.RUnlock()
		if inflightID != "" {
			s.complete(inflightID, nil, nil)
		}
	}
	close(errs)
	for err := range errs {
		t.Errorf("concurrent create/complete: %v", err)
	}
	if newCount != int64(rounds) {
		t.Errorf("expected %d total new jobs across %d rounds, got %d", rounds, rounds, newCount)
	}
}
