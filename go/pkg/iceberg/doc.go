// Package iceberg is a thin wrapper over apache/iceberg-go for the specific
// reads the janitor needs (snapshot history, manifest list traversal, file
// stats). It exists to keep upstream churn from leaking into the rest of the
// codebase and to make swapping in custom implementations easier later.
package iceberg
