// Command janitor-server is the Knative HTTP adapter for the janitor core.
// Lands in Phase 4.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "janitor-server: scaffold only; HTTP adapter lands in Phase 4")
	os.Exit(2)
}
