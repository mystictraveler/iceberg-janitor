// Package state persists janitor bookkeeping (access counters, feedback-loop
// history, in-flight job markers) as a JSON blob per table at
// <warehouse>/_janitor/state/<table>.json, using pkg/blob conditional writes
// for atomicity. No external database required.
package state
