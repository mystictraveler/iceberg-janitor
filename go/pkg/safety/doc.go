// Package safety hosts the master pre-commit consistency check that every
// maintenance op must pass before pkg/catalog.AtomicCommit. It also owns the
// recycle bin (_janitor/recycle/<run_id>/), the orphan-removal trust horizon,
// and the tags-and-branches expiration guard. Verification failure aborts
// the commit, releases the lease, and emits a structured Verification record
// to the snapshot summary and _janitor/results/<run_id>.json.
//
// The master check is row-count + structural-equivalence verification across
// nine invariants (I1..I9). It is mandatory and non-bypassable; --force does
// not disable it.
package safety
