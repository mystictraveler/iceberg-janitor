// Package maintenance contains the four maintenance ops: compact data files,
// expire snapshots, remove orphan files, and rewrite manifests. Each op
// produces a new metadata.json that is committed atomically via pkg/catalog.
package maintenance
