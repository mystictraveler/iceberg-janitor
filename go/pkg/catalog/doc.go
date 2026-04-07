// Package catalog implements a catalog-less, "FileIO catalog" view of an
// Iceberg warehouse. Tables are discovered by listing object-store prefixes;
// the current metadata.json is resolved via version-hint.text or by scanning
// metadata/v*.metadata.json. Atomicity for metadata-pointer swaps is provided
// by object-store conditional writes (PutIfNotExists / PutIfMatch), with no
// external coordination service.
package catalog
