// Command janitor-lambda is the AWS Lambda adapter for the janitor core.
// Lands in Phase 4.
package main

import (
	"fmt"
	"io"
	"os"
)

// run writes the scaffold banner and returns the exit code.
// Factored out of main so tests can exercise it in-process (the
// subprocess-based test can't produce coverage instrumentation).
func run(stderr io.Writer) int {
	fmt.Fprintln(stderr, "janitor-lambda: scaffold only; Lambda adapter lands in Phase 4")
	return 2
}

func main() {
	os.Exit(run(os.Stderr))
}
