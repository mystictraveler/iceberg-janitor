package main

import (
	"errors"
	"net/http/httptest"
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
	j := s.create("tpcds.store_sales", "compact")

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
	j := s.create("ns.tbl", "compact")
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
	j := s.create("ns.tbl", "compact")
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
	j := s.create("ns.tbl", "compact")
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
	// this will either race (under -race) or corrupt state.
	s := newJobStore()
	const n = 50
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			j := s.create("t", "op")
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
