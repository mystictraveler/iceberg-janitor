package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/jobrecord"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/lease"
)

// === persistent jobStore unit tests ===
//
// These tests prove the load-bearing cross-replica behavior of
// the persistent jobStore: two jobStore instances sharing the
// same fileblob bucket simulate two server replicas. The lease
// dedups across replicas; the persistent record lets either
// replica answer GET /v1/jobs/{id}; complete() releases the
// lease so a fresh maintain can start.

func newSharedBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := blob.OpenBucket(context.Background(), "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestPersistentJobStore_CreateWritesLeaseAndRecord(t *testing.T) {
	bucket := newSharedBucket(t)
	s := newPersistentJobStore(bucket)

	j, isNew := s.create("ns.tbl", "maintain")
	if !isNew {
		t.Fatal("first create: isNew=false, want true")
	}
	if j.ID == "" {
		t.Fatal("empty job ID")
	}
	if j.Owner == "" {
		t.Errorf("Owner not set on owned job")
	}

	// Lease file should exist on the bucket.
	leasePath := lease.Path("ns.tbl", "maintain")
	leaseObj, err := lease.Read(context.Background(), bucket, leasePath)
	if err != nil {
		t.Fatalf("Read lease: %v", err)
	}
	if leaseObj.JobID != j.ID {
		t.Errorf("lease.JobID = %q, want %q", leaseObj.JobID, j.ID)
	}

	// Job record should exist on the bucket.
	rec, err := jobrecord.Read(context.Background(), bucket, j.ID)
	if err != nil {
		t.Fatalf("Read jobrecord: %v", err)
	}
	if rec.Status != "pending" {
		t.Errorf("rec.Status = %q, want pending", rec.Status)
	}
	if rec.LeaseNonce != leaseObj.Nonce {
		t.Errorf("rec.LeaseNonce = %q, want %q", rec.LeaseNonce, leaseObj.Nonce)
	}
}

func TestPersistentJobStore_TwoReplicas_DedupCrossReplica(t *testing.T) {
	// THE LOAD-BEARING TEST: two jobStore instances sharing one
	// bucket simulate two replicas. Both POST /maintain for the
	// same table. Exactly ONE should win the lease; the other
	// should return the in-flight job's ID without spawning a
	// duplicate.
	bucket := newSharedBucket(t)
	replicaA := newPersistentJobStoreWithOwner(bucket, "replica-A")
	replicaB := newPersistentJobStoreWithOwner(bucket, "replica-B")

	// Replica A creates first.
	jA, newA := replicaA.create("ns.tbl", "maintain")
	if !newA {
		t.Fatal("replicaA: isNew=false, want true")
	}

	// Replica B tries to create the same. Should dedup to
	// replica A's job ID.
	jB, newB := replicaB.create("ns.tbl", "maintain")
	if newB {
		t.Errorf("replicaB: isNew=true, want false (lease already held by replica A)")
	}
	if jB.ID != jA.ID {
		t.Errorf("replicaB returned different ID: %q vs replicaA %q", jB.ID, jA.ID)
	}
	// The cached entry on replicaB should be marked as foreign
	// (Owner != replicaB.owner).
	if jB.Owner == replicaB.owner {
		t.Errorf("dedup result on replicaB has Owner=%q (this replica), expected foreign owner", jB.Owner)
	}
}

func TestPersistentJobStore_TwoReplicas_GetCanReadForeignJob(t *testing.T) {
	// Replica A creates a job. Replica B (which doesn't have it
	// in local cache) calls get() and falls back to S3, returning
	// the foreign job snapshot.
	bucket := newSharedBucket(t)
	replicaA := newPersistentJobStoreWithOwner(bucket, "replica-A")
	replicaB := newPersistentJobStoreWithOwner(bucket, "replica-B")

	jA, _ := replicaA.create("ns.tbl", "maintain")
	if jA.ID == "" {
		t.Fatal("replicaA create returned empty ID")
	}

	// Replica B has nothing in local cache.
	if _, ok := replicaB.jobs[jA.ID]; ok {
		t.Fatal("replicaB should not have replicaA's job in local cache")
	}

	// get() should fall back to S3.
	got, ok := replicaB.get(jA.ID)
	if !ok {
		t.Fatal("replicaB.get returned not-found for replicaA's job")
	}
	if got.ID != jA.ID {
		t.Errorf("replicaB got ID = %q, want %q", got.ID, jA.ID)
	}
	if got.Table != "ns.tbl" {
		t.Errorf("replicaB got Table = %q, want ns.tbl", got.Table)
	}
}

func TestPersistentJobStore_CompleteReleasesLease(t *testing.T) {
	// After complete(), the lease is released and a new create()
	// for the same (operation, table) should succeed.
	bucket := newSharedBucket(t)
	replicaA := newPersistentJobStore(bucket)

	first, _ := replicaA.create("ns.tbl", "maintain")
	replicaA.complete(first.ID, map[string]any{"ok": true}, nil)

	// Lease should be released.
	if _, err := lease.Read(context.Background(), bucket, lease.Path("ns.tbl", "maintain")); err == nil {
		t.Errorf("lease should be released after complete, but Read succeeded")
	}

	// Final job record should be visible to ANY replica with
	// status=completed.
	replicaB := newPersistentJobStoreWithOwner(bucket, "replica-B")
	got, ok := replicaB.get(first.ID)
	if !ok {
		t.Fatal("replicaB cannot read completed job")
	}
	if got.Status != "completed" {
		t.Errorf("replicaB sees Status = %q, want completed", got.Status)
	}

	// A fresh create() on the same table should succeed (lease
	// released) and produce a NEW job ID.
	second, isNew := replicaA.create("ns.tbl", "maintain")
	if !isNew {
		t.Errorf("second create after complete: isNew=false, want true")
	}
	if second.ID == first.ID {
		t.Errorf("second create produced same ID as first; expected fresh job")
	}
}

func TestPersistentJobStore_CompleteFailureWritesError(t *testing.T) {
	bucket := newSharedBucket(t)
	replicaA := newPersistentJobStore(bucket)

	first, _ := replicaA.create("ns.tbl", "maintain")
	replicaA.complete(first.ID, nil, errors.New("disk full"))

	// Failed status visible to other replicas.
	replicaB := newPersistentJobStoreWithOwner(bucket, "replica-B")
	got, ok := replicaB.get(first.ID)
	if !ok {
		t.Fatal("replicaB cannot read failed job")
	}
	if got.Status != "failed" {
		t.Errorf("Status = %q, want failed", got.Status)
	}
	if got.Error != "disk full" {
		t.Errorf("Error = %q, want 'disk full'", got.Error)
	}
}

func TestPersistentJobStore_ConcurrentCreateAcrossReplicas(t *testing.T) {
	// N replicas race to create the same (operation, table).
	// Exactly ONE wins the lease; all others return the in-flight
	// job's ID. This is the cross-replica equivalent of the
	// in-process TestJobStoreCreate_ConcurrentCreateSameTable.
	bucket := newSharedBucket(t)
	const replicas = 16
	stores := make([]*jobStore, replicas)
	for i := range stores {
		stores[i] = newPersistentJobStoreWithOwner(bucket, fmt.Sprintf("replica-%d", i))
	}

	winners := int64(0)
	losers := int64(0)
	idCh := make(chan string, replicas)
	var wg sync.WaitGroup
	wg.Add(replicas)
	start := make(chan struct{})

	for i := 0; i < replicas; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			j, isNew := stores[i].create("ns.tbl", "maintain")
			if isNew {
				atomic.AddInt64(&winners, 1)
			} else {
				atomic.AddInt64(&losers, 1)
			}
			idCh <- j.ID
		}()
	}
	close(start)
	wg.Wait()
	close(idCh)

	if winners != 1 {
		t.Errorf("expected exactly 1 winner across %d replicas, got %d (losers=%d)",
			replicas, winners, losers)
	}
	if losers != replicas-1 {
		t.Errorf("expected %d losers, got %d", replicas-1, losers)
	}

	// All N goroutines must see the same job ID.
	var firstID string
	count := 0
	for id := range idCh {
		count++
		if firstID == "" {
			firstID = id
		} else if id != firstID {
			t.Errorf("got different IDs across replicas: %q vs %q", firstID, id)
		}
	}
	if count != replicas {
		t.Errorf("collected %d ids, want %d", count, replicas)
	}
}

func TestPersistentJobStore_DifferentTablesDoNotDedup(t *testing.T) {
	// Two different tables are independent — each gets its own
	// lease. Cross-replica scenario.
	bucket := newSharedBucket(t)
	replicaA := newPersistentJobStoreWithOwner(bucket, "replica-A")
	replicaB := newPersistentJobStoreWithOwner(bucket, "replica-B")

	jA, newA := replicaA.create("ns.tableA", "maintain")
	jB, newB := replicaB.create("ns.tableB", "maintain")
	if !newA || !newB {
		t.Errorf("independent tables should both be new: newA=%v newB=%v", newA, newB)
	}
	if jA.ID == jB.ID {
		t.Errorf("independent tables produced same job ID")
	}
}
