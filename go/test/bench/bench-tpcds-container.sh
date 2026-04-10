#!/usr/bin/env bash
# bench-tpcds-container.sh — Wrapper for running bench-tpcds.sh inside
# a container where binaries are pre-built and duckdb is installed.
set -euo pipefail

# Fake 'go' binary — bench-tpcds.sh calls `require go` and `go build`.
# Our container already has the binaries at /tmp/janitor-{cli,streamer}.
mkdir -p /tmp/fake-go-bin
cat > /tmp/fake-go-bin/go <<'FAKEGO'
#!/bin/bash
exit 0
FAKEGO
chmod +x /tmp/fake-go-bin/go
export PATH="/tmp/fake-go-bin:$PATH"

# Recreate the directory tree the bench script expects.
# SCRIPT_DIR resolves to /bench, so REPO_ROOT = /bench/../../.. = /
# and GO_DIR = /go. We create /go so `cd "$GO_DIR"` succeeds.
mkdir -p /go/test/bench
mkdir -p /results

# Override RESULTS_DIR so output goes somewhere we can find it.
export RESULTS_DIR=/results

exec /bench/bench-tpcds.sh "$@"
