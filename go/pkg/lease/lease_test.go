package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

// Test fixture: a fileblob bucket rooted at t.TempDir(). Closed
// automatically via t.Cleanup. Every test gets its own bucket so
// tests don't bleed state into each other.
func newBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := blob.OpenBucket(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestSystemOwner_StableAcrossCalls(t *testing.T) {
	a := SystemOwner()
	b := SystemOwner()
	if a != b {
		t.Errorf("SystemOwner not stable: %q vs %q", a, b)
	}
	if a == "" {
		t.Errorf("SystemOwner returned empty")
	}
}

func TestPath_Layout(t *testing.T) {
	got := Path("tpcds.store_sales", "maintain")
	want := "_janitor/state/leases/tpcds.store_sales/maintain.lease"
	if got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
}

// === TryAcquire happy path ===

func TestTryAcquire_FirstWriteSucceeds(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	l, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "test-owner-1",
		JobID: "job-aaa",
		Now:   now,
	})
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if l == nil {
		t.Fatal("nil lease")
	}
	if l.Owner != "test-owner-1" {
		t.Errorf("Owner = %q, want test-owner-1", l.Owner)
	}
	if l.JobID != "job-aaa" {
		t.Errorf("JobID = %q, want job-aaa", l.JobID)
	}
	if l.Nonce == "" {
		t.Errorf("empty Nonce")
	}
	if l.TTLSeconds != int(DefaultTTL.Seconds()) {
		t.Errorf("TTLSeconds = %d, want %d (default)", l.TTLSeconds, int(DefaultTTL.Seconds()))
	}
	if !l.AcquiredAt.Equal(now) {
		t.Errorf("AcquiredAt = %v, want %v", l.AcquiredAt, now)
	}
}

func TestTryAcquire_SecondWriteReturnsHeld(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	first, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-A",
		JobID: "job-1",
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	second, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-B",
		JobID: "job-2",
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second acquire: expected ErrLeaseHeld, got %v", err)
	}
	if second == nil {
		t.Fatal("expected existing lease in second result")
	}
	// Second result must be the FIRST lease, not B's attempt.
	if second.Owner != "owner-A" || second.JobID != "job-1" {
		t.Errorf("returned lease: owner=%q job=%q, want owner-A/job-1",
			second.Owner, second.JobID)
	}
	if second.Nonce != first.Nonce {
		t.Errorf("returned lease nonce mismatch: %q vs %q", second.Nonce, first.Nonce)
	}
}

func TestTryAcquire_RejectsEmptyOwner(t *testing.T) {
	bucket := newBucket(t)
	_, err := TryAcquire(context.Background(), bucket, Path("ns.tbl", "maintain"), Options{
		JobID: "job-1",
	})
	if err == nil {
		t.Fatal("expected error for empty Owner")
	}
}

func TestTryAcquire_RejectsEmptyJobID(t *testing.T) {
	bucket := newBucket(t)
	_, err := TryAcquire(context.Background(), bucket, Path("ns.tbl", "maintain"), Options{
		Owner: "owner-X",
	})
	if err == nil {
		t.Fatal("expected error for empty JobID")
	}
}

// === Read ===

func TestRead_NotFound(t *testing.T) {
	bucket := newBucket(t)
	_, err := Read(context.Background(), bucket, Path("ns.tbl", "maintain"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRead_RoundTrip(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	written, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-X", JobID: "job-Y", Now: now,
	})
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}

	read, err := Read(context.Background(), bucket, path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if read.Owner != written.Owner {
		t.Errorf("Owner roundtrip: got %q, want %q", read.Owner, written.Owner)
	}
	if read.JobID != written.JobID {
		t.Errorf("JobID roundtrip: got %q, want %q", read.JobID, written.JobID)
	}
	if read.Nonce != written.Nonce {
		t.Errorf("Nonce roundtrip: got %q, want %q", read.Nonce, written.Nonce)
	}
	if !read.AcquiredAt.Equal(written.AcquiredAt) {
		t.Errorf("AcquiredAt roundtrip: got %v, want %v", read.AcquiredAt, written.AcquiredAt)
	}
	if read.Path != path {
		t.Errorf("Path = %q, want %q", read.Path, path)
	}
}

// === Release ===

func TestRelease_DeletesLease(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	_, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-X", JobID: "job-Y",
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := Release(context.Background(), bucket, path); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if _, err := Read(context.Background(), bucket, path); !errors.Is(err, ErrNotFound) {
		t.Errorf("after Release: expected ErrNotFound, got %v", err)
	}
}

func TestRelease_IdempotentOnMissing(t *testing.T) {
	bucket := newBucket(t)
	if err := Release(context.Background(), bucket, Path("ns.tbl", "maintain")); err != nil {
		t.Errorf("Release on missing lease should be a no-op, got %v", err)
	}
}

func TestRelease_AllowsReacquire(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	first, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-A", JobID: "job-1",
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := Release(context.Background(), bucket, path); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-B", JobID: "job-2",
	})
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	if second.Nonce == first.Nonce {
		t.Errorf("re-acquired lease has same nonce as released one")
	}
	if second.Owner != "owner-B" {
		t.Errorf("re-acquired Owner = %q, want owner-B", second.Owner)
	}
}

// === IsStale ===

func TestIsStale_FreshLeaseNotStale(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	l := &Lease{AcquiredAt: now.Add(-30 * time.Second), TTLSeconds: 900}
	if l.IsStale(now) {
		t.Errorf("30s-old lease should not be stale")
	}
}

func TestIsStale_PastTTLButWithinGraceNotStale(t *testing.T) {
	// Lease TTL has expired but the takeover-grace window has not.
	// Don't take over yet — clock skew protection.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	l := &Lease{
		AcquiredAt: now.Add(-15 * time.Minute).Add(-30 * time.Second), // 30s past TTL
		TTLSeconds: 900,
	}
	if l.IsStale(now) {
		t.Errorf("lease 30s past TTL but inside grace window should not be stale")
	}
}

func TestIsStale_PastTTLPlusGraceIsStale(t *testing.T) {
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	l := &Lease{
		// 15 min TTL + 60s grace + 1s past = stale
		AcquiredAt: now.Add(-15 * time.Minute).Add(-61 * time.Second),
		TTLSeconds: 900,
	}
	if !l.IsStale(now) {
		t.Errorf("lease 61s past TTL+grace should be stale")
	}
}

func TestIsStale_NilLeaseIsStale(t *testing.T) {
	var l *Lease
	if !l.IsStale(time.Now()) {
		t.Errorf("nil lease should be considered stale")
	}
}

// === StaleTakeover ===

func TestStaleTakeover_SucceedsAfterStale(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	// Owner A acquires "16 minutes ago" — past TTL+grace.
	long := 16*time.Minute + 30*time.Second
	staleNow := time.Now().Add(-long)
	_, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "owner-A-dead", JobID: "job-A",
		Now: staleNow,
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Owner B reads, sees stale, takes over.
	existing, err := Read(context.Background(), bucket, path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !existing.IsStale(time.Now()) {
		t.Fatalf("expected lease to be stale, AcquiredAt=%v", existing.AcquiredAt)
	}

	taken, err := StaleTakeover(context.Background(), bucket, path, existing.Nonce, Options{
		Owner: "owner-B", JobID: "job-B",
	})
	if err != nil {
		t.Fatalf("StaleTakeover: %v", err)
	}
	if taken.Owner != "owner-B" || taken.JobID != "job-B" {
		t.Errorf("after takeover: owner=%q job=%q, want owner-B/job-B", taken.Owner, taken.JobID)
	}
	if taken.Nonce == existing.Nonce {
		t.Errorf("takeover lease nonce should differ from stale lease")
	}
}

// === Concurrent acquire race ===

func TestTryAcquire_ConcurrentRace_OneWinner(t *testing.T) {
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	const goroutines = 20
	winners := int64(0)
	losers := int64(0)
	var jobIDs []string
	var jobIDsMu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			l, err := TryAcquire(context.Background(), bucket, path, Options{
				Owner: "test-owner",
				JobID: fmtJobID(i),
			})
			if err == nil {
				atomic.AddInt64(&winners, 1)
				jobIDsMu.Lock()
				jobIDs = append(jobIDs, l.JobID)
				jobIDsMu.Unlock()
			} else if errors.Is(err, ErrLeaseHeld) {
				atomic.AddInt64(&losers, 1)
			} else {
				t.Errorf("goroutine %d: unexpected err: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Errorf("expected exactly 1 winner under %d-way race, got %d (losers=%d)",
			goroutines, winners, losers)
	}
	if losers != goroutines-1 {
		t.Errorf("expected %d losers, got %d", goroutines-1, losers)
	}
	if len(jobIDs) != 1 {
		t.Errorf("expected 1 winning JobID, got %d", len(jobIDs))
	}
}

// === StaleTakeover concurrent race ===

func TestStaleTakeover_ConcurrentRace_OneWinner(t *testing.T) {
	// Multiple replicas race to take over the same stale lease.
	// The verify-still-stale check (gated by the per-path mutex)
	// guarantees exactly one winner: the first goroutine into the
	// critical section verifies the stale nonce, deletes, and
	// creates a new lease. Subsequent goroutines acquire the
	// mutex, read the NEW lease, see the nonce has changed, and
	// return ErrLeaseHeld with the new lease.
	bucket := newBucket(t)
	path := Path("ns.tbl", "maintain")

	// Pre-populate a stale lease.
	stale := time.Now().Add(-30 * time.Minute)
	staleLease, err := TryAcquire(context.Background(), bucket, path, Options{
		Owner: "dead-owner", JobID: "dead-job", Now: stale,
	})
	if err != nil {
		t.Fatalf("seed stale lease: %v", err)
	}

	const goroutines = 16
	winners := int64(0)
	heldLosers := int64(0)
	var winningJobIDs []string
	var winningJobIDsMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			l, err := StaleTakeover(context.Background(), bucket, path, staleLease.Nonce, Options{
				Owner: "takeover-owner",
				JobID: fmtJobID(i),
			})
			if err == nil {
				atomic.AddInt64(&winners, 1)
				winningJobIDsMu.Lock()
				winningJobIDs = append(winningJobIDs, l.JobID)
				winningJobIDsMu.Unlock()
			} else if errors.Is(err, ErrLeaseHeld) {
				atomic.AddInt64(&heldLosers, 1)
			} else {
				t.Errorf("goroutine %d: unexpected err: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Errorf("expected exactly 1 takeover winner under %d-way race, got %d (winning IDs: %v)",
			goroutines, winners, winningJobIDs)
	}
	if heldLosers != goroutines-1 {
		t.Errorf("expected %d held losers, got %d", goroutines-1, heldLosers)
	}
}

// fmtJobID is a small helper to keep test brevity.
func fmtJobID(i int) string {
	return "job-" + string(rune('A'+i))
}
