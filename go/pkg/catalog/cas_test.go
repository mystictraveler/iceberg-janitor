package catalog

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestAtomicWriteLocalFileCAS verifies that the local-file CAS path used
// by DirectoryCatalog.atomicWriteMetadataJSON is genuinely atomic on
// POSIX: many goroutines racing to write the same target file result in
// exactly ONE successful write and the rest get a precondition error.
//
// This matters because gocloud.dev/blob's fileblob backend has a TOCTOU
// race in its IfNotExist handling (os.Stat then os.Rename). Our local
// CAS path bypasses fileblob and uses os.Link, which is atomic on
// local filesystems.
//
// Cloud backends (s3, gcs, azblob) are correct via gocloud.dev/blob's
// own IfNotExist because the underlying provider APIs are linearized
// per key. We don't unit-test the cloud path here because it requires a
// live cloud backend; the production loop verifies it via MinIO.
func TestAtomicWriteLocalFileCAS(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "metadata", "v42.metadata.json")
	absLoc := "file://" + target

	const writers = 32
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		successCount int
	)
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(idx int) {
			defer wg.Done()
			err := atomicWriteLocalBytes(absLoc, []byte("payload"))
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful CAS, got %d (out of %d writers racing on %q)",
			successCount, writers, target)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target file should exist after a successful CAS: %v", err)
	}

	// Subsequent writes to the same key must also fail (not just the
	// initial race).
	if err := atomicWriteLocalBytes(absLoc, []byte("after-the-fact")); err == nil {
		t.Fatalf("a write to an already-existing key should fail with CAS conflict")
	}
}
