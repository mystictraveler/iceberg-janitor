<!--
PR template for iceberg-janitor. Fill in every section; delete the
HTML comments on submit. The ship checklist at the bottom is
non-negotiable — every PR that touches the compaction / maintenance
path must check all gating items before merge.
-->

## Summary

<!-- One or two paragraphs. What changes, why. If this closes an
issue, link it here. -->

## Mechanism

<!-- Per-file walkthrough of what moved. New types, new APIs,
behaviour changes. Load-bearing file:line references welcome. -->

## Rationale

<!-- Why this shape? What alternatives were considered + why
rejected? If the change has user-visible semantics, describe them
here. For correctness-sensitive changes (master check, safety
gates, compaction paths), explicitly state the invariant you're
preserving. -->

## Ship checklist

Every item is **gating** — a PR that can't check all the boxes below
does not merge to `main`. Paste real numbers into the bench row so a
reviewer can see before/after, not just "I ran it".

- [ ] `cd go && go build ./...` — clean
- [ ] `cd go && go vet ./...` — clean
- [ ] `cd go && go test ./...` — all packages pass
- [ ] **New correctness tests** added at the same time as new code (per `feedback_write_tests.md`), listed here:
  - [ ]
- [ ] **MinIO deletes bench** — `WORKLOAD=deletes bash go/test/bench/bench.sh minio` passes (when change could affect the V2 delete read-through or the compact path generally):

  <!-- Fill in even for small-scope changes — one run is fine. -->
  ```
  files: 200 → 1 | rows: 10000 → 9950 | compact wall: <ms> | master: PASS
  ```

- [ ] **MinIO TPC-DS main bench (GATING)** — `bash go/test/bench/bench.sh minio` (default `WORKLOAD=tpcds`) completes end-to-end with no regression vs `main`. This is the A/B that drives the janitor's production claim — we cannot ship a change that regresses it:

  <!--
  How to run:
    bash go/test/bench/bench.sh minio
      (defaults: STREAM_DURATION_SECONDS=300, MAINTAIN_ROUNDS=2,
       QUERY_ITERATIONS=3, COMMITS_PER_MINUTE=60)
  Wall time: ~10 min.
  Output: bench-results/bench-summary-<ts>.txt + .csv

  What counts as no-regression (the gate):
    - With-janitor query times within 10% of baseline on each TPC-DS
      query
    - File reduction ratio within 10% of baseline (Run 20 shipped at
      192× on 3 fact tables)
    - Zero master-check failures across all maintain calls in the run
    - Zero row drift between with/without warehouses at any query
      iteration
    - No unexpected CB trips (CB2 loop, CB4 no-effectiveness, CB8
      consecutive failures)

  Paste the one-line summary from bench-summary-<ts>.txt below.
  -->
  ```
  run id: <ts> | streamer commits: <N> | file reduction: <X×> | query parity: <OK|deviations>
  ```

- [ ] `FEATURES.md` updated if this PR ships, partial-ships, changes state of, or refuses any feature
- [ ] Honest gaps / known limitations explicitly called out in the PR body above (never bury them in commit messages)
- [ ] Reviewer requested; no `DO NOT MERGE` blockers in the body

## Closes / refs

<!-- Related issues, design docs, prior PRs. -->
