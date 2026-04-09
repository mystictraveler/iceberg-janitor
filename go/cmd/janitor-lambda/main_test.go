package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestLambdaStubContract documents and enforces the current scaffold
// behavior of cmd/janitor-lambda: it MUST exit non-zero with a
// stderr message containing "scaffold only" so a misconfigured
// deployment that points at this binary fails fast and visibly
// rather than silently appearing to work.
//
// When the real Lambda handler lands (the design plan calls for it
// in Phase 4) this test should be replaced with handler-level tests
// against an in-process Lambda runtime emulator. The contract this
// test enforces is intentional: it is the only thing keeping the
// stub from accidentally becoming a no-op success path.
func TestLambdaStubContract(t *testing.T) {
	// Build the binary into the test's temp dir. Using `go run` would
	// also work but would re-compile every test invocation; building
	// once is faster and matches what the production deployment
	// would do.
	dir := t.TempDir()
	bin := filepath.Join(dir, "janitor-lambda")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Stderr = nil
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build janitor-lambda: %v\n%s", err, out)
	}

	cmd := exec.Command(bin)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit, got success")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected *exec.ExitError, got %T: %v", err, err)
	}
	if code := exitErr.ExitCode(); code != 2 {
		t.Errorf("exit code: got %d want 2", code)
	}
	if !strings.Contains(stderr.String(), "scaffold only") {
		t.Errorf("stderr does not document scaffold contract: %q", stderr.String())
	}
}
