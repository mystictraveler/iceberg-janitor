// Package blob is a thin multi-cloud object store abstraction over
// gocloud.dev/blob, exposing only the operations the janitor needs and
// normalizing conditional-write semantics across S3, MinIO, Azure Blob,
// and Google Cloud Storage.
package blob
