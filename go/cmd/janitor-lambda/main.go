// Command janitor-lambda is the AWS Lambda adapter for the janitor core.
// Lands in Phase 4.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "janitor-lambda: scaffold only; Lambda adapter lands in Phase 4")
	os.Exit(2)
}
