package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRunStubContract documents and enforces the current scaffold
// behavior of cmd/janitor-lambda: run() MUST return exit code 2 with
// a stderr message containing "scaffold only" so a misconfigured
// deployment that points at this binary fails fast and visibly
// rather than silently appearing to work.
//
// When the real Lambda handler lands (the design plan calls for it
// in Phase 4) this test should be replaced with handler-level tests
// against an in-process Lambda runtime emulator. The contract this
// test enforces is intentional: it is the only thing keeping the
// stub from accidentally becoming a no-op success path.
func TestRunStubContract(t *testing.T) {
	var stderr bytes.Buffer
	code := run(&stderr)

	if code != 2 {
		t.Errorf("exit code: got %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "scaffold only") {
		t.Errorf("stderr does not document scaffold contract: %q", stderr.String())
	}
}
