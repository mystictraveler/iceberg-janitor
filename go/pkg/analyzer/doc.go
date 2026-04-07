// Package analyzer produces a HealthReport for an Iceberg table by walking
// its current snapshot's manifest list and aggregating file-level stats.
// Port of src/iceberg_janitor/analyzer/health.py.
package analyzer
