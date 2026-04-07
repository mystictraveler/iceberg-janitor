// Package fileio adapts pkg/blob to the io.Reader/Writer surface that
// apache/iceberg-go expects, so the rest of the janitor can read and write
// Iceberg metadata and data files through a single multi-cloud abstraction.
package fileio
