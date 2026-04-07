package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"

	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	view "github.com/apache/iceberg-go/view"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	// Register the gocloud-backed io schemes (s3, s3a, gs, azblob, mem)
	// with iceberg-go's IO registry. The local file:// scheme is built
	// in. Without this blank import, NewFromLocation fails for any
	// non-file warehouse with "io scheme not registered".
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// DirectoryCatalog implements iceberg-go's catalog.Catalog interface against
// a warehouse object store with NO catalog service. The Iceberg metadata
// files in object storage ARE the catalog: tables are discovered by listing
// prefixes, the current metadata pointer is the highest-numbered
// v*.metadata.json file, and atomic commit is performed by writing
// v(N+1).metadata.json with conditional-write semantics.
//
// MVP scope: only the methods the janitor's compaction path actually
// touches are implemented (LoadTable, CommitTable, CheckTableExists,
// CatalogType). The rest return ErrNotImplemented. The intent is to grow
// this implementation as the rest of the janitor lands.
type DirectoryCatalog struct {
	name         string
	warehouseURL string         // gocloud.dev/blob URL of the warehouse bucket
	bucket       *blob.Bucket   // open bucket; caller must Close
	props        map[string]string
}

// NewDirectoryCatalog opens the warehouse bucket and returns a catalog
// that operates against it directly.
func NewDirectoryCatalog(ctx context.Context, name, warehouseURL string, props map[string]string) (*DirectoryCatalog, error) {
	bucket, err := blob.OpenBucket(ctx, warehouseURL)
	if err != nil {
		return nil, fmt.Errorf("opening warehouse bucket %q: %w", warehouseURL, err)
	}
	return &DirectoryCatalog{
		name:         name,
		warehouseURL: warehouseURL,
		bucket:       bucket,
		props:        props,
	}, nil
}

// Close releases the underlying bucket.
func (c *DirectoryCatalog) Close() error {
	if c.bucket != nil {
		return c.bucket.Close()
	}
	return nil
}

// Bucket returns the underlying gocloud.dev/blob.Bucket. Useful for
// callers that need to do raw blob operations (readiness probes,
// administrative inspection) against the same backend the catalog uses.
func (c *DirectoryCatalog) Bucket() *blob.Bucket { return c.bucket }

// WarehouseURL returns the URL the catalog was opened against.
func (c *DirectoryCatalog) WarehouseURL() string { return c.warehouseURL }

// Props returns a copy of the catalog's IO properties.
func (c *DirectoryCatalog) Props() map[string]string {
	out := make(map[string]string, len(c.props))
	for k, v := range c.props {
		out[k] = v
	}
	return out
}

// CatalogType returns the type identifier for this catalog implementation.
func (c *DirectoryCatalog) CatalogType() icebergcat.Type {
	return icebergcat.Type("directory")
}

// LoadTable resolves the table at <namespace>/<name> by walking the
// blob layout, finding the maximum v*.metadata.json, and loading it via
// iceberg-go.
func (c *DirectoryCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	prefix := identToPrefix(ident)
	tables, err := DiscoverTables(ctx, c.bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("discovering table %v: %w", ident, err)
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("%w: %v", icebergcat.ErrNoSuchTable, ident)
	}
	if len(tables) > 1 {
		return nil, fmt.Errorf("ambiguous identifier %v: matches %d tables", ident, len(tables))
	}
	loc := tables[0]
	metaURL, err := absoluteURL(c.warehouseURL, loc.CurrentMetadata)
	if err != nil {
		return nil, err
	}
	fsysF := icebergio.LoadFSFunc(c.props, metaURL)
	tbl, err := icebergtable.NewFromLocation(ctx, ident, metaURL, fsysF, c)
	if err != nil {
		return nil, fmt.Errorf("loading table %v: %w", ident, err)
	}
	return tbl, nil
}

// CommitTable is the load-bearing operation for compaction. It applies
// the requirements (preconditions) and updates (deltas) against the
// current table state and writes a new metadata.json at the next version.
//
// MVP NOTE: this implementation does NOT yet use conditional writes for
// atomicity. The directory catalog assumes single-writer for the MVP loop
// (which is true for the seed → compact → analyze test). Production
// hardening adds `If-None-Match: *` on the new metadata.json PUT to
// prevent two writers from racing on the same v(N+1) key, per the
// design plan.
func (c *DirectoryCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, icebergcat.ErrNoSuchTable) {
		return nil, "", err
	}

	var (
		baseMeta    icebergtable.Metadata
		metadataLoc string
	)
	if current != nil {
		for _, r := range reqs {
			if err := r.Validate(current.Metadata()); err != nil {
				return nil, "", fmt.Errorf("requirement validation failed: %w", err)
			}
		}
		baseMeta = current.Metadata()
		metadataLoc = current.MetadataLocation()
	} else {
		return nil, "", fmt.Errorf("%w: cannot create table via directory catalog (use the seed instead)", icebergcat.ErrNoSuchTable)
	}

	updated, err := icebergtable.UpdateTableMetadata(baseMeta, updates, metadataLoc)
	if err != nil {
		return nil, "", fmt.Errorf("applying updates: %w", err)
	}
	if updated.Equals(baseMeta) {
		// No changes — return current as-is.
		return baseMeta, metadataLoc, nil
	}

	// Compute the next metadata location: v(N+1).metadata.json under the
	// table's metadata/ prefix. We use iceberg-go's location provider so
	// the path matches what other Iceberg readers expect.
	provider, err := icebergtable.LoadLocationProvider(updated.Location(), updated.Properties())
	if err != nil {
		return nil, "", fmt.Errorf("loading location provider: %w", err)
	}
	nextVersion := nextMetadataVersion(metadataLoc) + 1
	newLoc, err := provider.NewTableMetadataFileLocation(nextVersion)
	if err != nil {
		return nil, "", fmt.Errorf("computing next metadata location: %w", err)
	}

	// Atomic conditional-write commit. This is the load-bearing primitive
	// that makes the no-catalog-service design correct under multi-writer
	// concurrency: PUT the new metadata.json with IfNotExist=true. If
	// another writer (us-on-retry, another janitor invocation, an external
	// Spark job) committed v(N+1) first, the underlying object store
	// returns PreconditionFailed (412 on S3, generation-mismatch on GCS,
	// ETag mismatch on Azure) and we surface it to the caller as an
	// error so the transaction can be retried against the new current.
	if err := c.atomicWriteMetadataJSON(ctx, updated, newLoc); err != nil {
		return nil, "", fmt.Errorf("atomic commit at %s: %w", newLoc, err)
	}
	return updated, newLoc, nil
}

// atomicWriteMetadataJSON serializes `meta` and writes it to `loc` with
// conditional-write semantics — the write succeeds only if the key does
// not yet exist. This is what makes the directory catalog safe for
// concurrent writers without an external coordination service.
//
// For cloud backends (s3://, gs://, azblob://) we use the underlying
// gocloud.dev/blob.Bucket's WriterOptions.IfNotExist, which maps to the
// provider's native conditional-write primitive (S3 If-None-Match, GCS
// generation-match, Azure ETag). All three are linearized per-key by
// the cloud provider and give us the genuine CAS semantics the design
// requires.
//
// For local file:// we bypass gocloud.dev/blob entirely. fileblob's
// IfNotExist implementation is racy on POSIX (it does os.Stat followed
// by os.Rename, which has a TOCTOU window). Instead we write to a
// uniquely-named temp file in the same directory and use os.Link, which
// is atomic on local filesystems: link(2) fails with EEXIST if the
// target exists, with no race window. This is the same pattern used by
// every "atomic file create" library on Unix.
func (c *DirectoryCatalog) atomicWriteMetadataJSON(ctx context.Context, meta icebergtable.Metadata, absLoc string) error {
	if strings.HasPrefix(c.warehouseURL, "file://") {
		if err := atomicWriteLocalMetadataJSON(absLoc, meta); err != nil {
			// Local CAS path returns "CAS conflict" in the message; map it to
			// the sentinel so retryable callers can detect it.
			if strings.Contains(err.Error(), "CAS conflict") {
				return fmt.Errorf("%w: %v", ErrCASConflict, err)
			}
			return err
		}
		return nil
	}
	// Cloud backends: gocloud.dev's IfNotExist maps to a native
	// conditional-write primitive that the underlying provider
	// linearizes per key.
	key, err := bucketRelativeKey(c.warehouseURL, absLoc)
	if err != nil {
		return err
	}
	w, err := c.bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: "application/json",
		IfNotExist:  true,
	})
	if err != nil {
		return fmt.Errorf("opening writer for %s: %w", key, err)
	}
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	if err := json.NewEncoder(w).Encode(meta); err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}
	if err := w.Close(); err != nil {
		// PreconditionFailed comes back here for losers. gocloud.dev maps
		// the underlying provider's conditional-write failure to
		// gcerrors.PreconditionFailed.
		w = nil
		if gcerrors.Code(err) == gcerrors.FailedPrecondition {
			return fmt.Errorf("%w: %v", ErrCASConflict, err)
		}
		return fmt.Errorf("closing writer for %s: %w", key, err)
	}
	w = nil
	return nil
}

// atomicWriteLocalMetadataJSON encodes the Metadata to JSON and hands it
// to atomicWriteLocalBytes for the actual filesystem-level CAS.
func atomicWriteLocalMetadataJSON(absLoc string, meta icebergtable.Metadata) error {
	payload, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}
	return atomicWriteLocalBytes(absLoc, payload)
}

// atomicWriteLocalBytes writes `payload` to the local file at `absLoc`
// with true POSIX-atomic exclusive creation via temp-file + os.Link.
// link(2) is the atomic primitive: it fails with EEXIST if the target
// already exists, with no race window. We unconditionally remove the
// temp file at the end so a partial write never accumulates garbage.
//
// This is the testable primitive — `cas_test.go` exercises it directly
// against many concurrent goroutines and asserts exactly one winner.
func atomicWriteLocalBytes(absLoc string, payload []byte) error {
	const filePrefix = "file://"
	if !strings.HasPrefix(absLoc, filePrefix) {
		return fmt.Errorf("atomicWriteLocalBytes: not a file:// URL: %s", absLoc)
	}
	target := strings.TrimPrefix(absLoc, filePrefix)

	// Make sure the target directory exists.
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	// Create a uniquely-named temp file in the SAME directory so the
	// link(2) crosses no filesystem boundaries.
	tmp, err := os.CreateTemp(dir, ".janitor-meta-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // best-effort cleanup

	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// The atomic step. Link returns EEXIST if target already exists.
	if err := os.Link(tmpPath, target); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("CAS conflict: %s already exists", target)
		}
		return fmt.Errorf("link temp -> %s: %w", target, err)
	}
	return nil
}

// bucketRelativeKey strips the warehouse URL's scheme + host prefix from
// an absolute object URL, returning the bucket-relative key.
func bucketRelativeKey(warehouseURL, absLoc string) (string, error) {
	switch {
	case strings.HasPrefix(warehouseURL, "file://"):
		base := strings.TrimPrefix(warehouseURL, "file://")
		if i := strings.Index(base, "?"); i >= 0 {
			base = base[:i]
		}
		base = strings.TrimSuffix(base, "/")
		// absLoc looks like file:///base/key
		full := strings.TrimPrefix(absLoc, "file://")
		if !strings.HasPrefix(full, base+"/") {
			return "", fmt.Errorf("absolute location %q is not under warehouse %q", absLoc, base)
		}
		return strings.TrimPrefix(full, base+"/"), nil
	case strings.HasPrefix(warehouseURL, "s3://"):
		rest := strings.TrimPrefix(warehouseURL, "s3://")
		bucketName := rest
		if i := strings.Index(rest, "?"); i >= 0 {
			bucketName = rest[:i]
		}
		bucketName = strings.TrimSuffix(bucketName, "/")
		// absLoc looks like s3://bucketName/key
		expectedPrefix := "s3://" + bucketName + "/"
		if !strings.HasPrefix(absLoc, expectedPrefix) {
			return "", fmt.Errorf("absolute location %q is not in bucket %q", absLoc, bucketName)
		}
		return strings.TrimPrefix(absLoc, expectedPrefix), nil
	default:
		return "", fmt.Errorf("unsupported warehouse URL scheme: %s", warehouseURL)
	}
}

// nextMetadataVersion parses the current metadata path and returns the
// version number it carries. The caller adds 1 to compute the next.
func nextMetadataVersion(currentLoc string) int {
	// Strip up to the last "/"
	i := strings.LastIndex(currentLoc, "/")
	if i < 0 {
		return 0
	}
	name := currentLoc[i+1:]
	v, _ := parseMetadataVersion(name)
	return v
}

// identToPrefix converts an iceberg-go Identifier (e.g., {"mvp", "events"})
// into a bucket-relative prefix the discover primitive can walk
// (e.g., "mvp.db/events"). This matches iceberg-go's default location
// provider, which puts namespaces under "<ns>.db/".
func identToPrefix(ident icebergtable.Identifier) string {
	if len(ident) < 2 {
		return strings.Join(ident, "/")
	}
	parts := make([]string, 0, len(ident))
	parts = append(parts, ident[0]+".db")
	parts = append(parts, ident[1:]...)
	return strings.Join(parts, "/")
}

// absoluteURL converts a warehouse URL plus a bucket-relative key into
// the absolute URL iceberg-go's IO registry expects. file:// preserves
// the path; s3:// keeps just the bucket name and appends the key.
func absoluteURL(warehouseURL, key string) (string, error) {
	switch {
	case strings.HasPrefix(warehouseURL, "file://"):
		base := strings.TrimPrefix(warehouseURL, "file://")
		if i := strings.Index(base, "?"); i >= 0 {
			base = base[:i]
		}
		base = strings.TrimSuffix(base, "/")
		return "file://" + base + "/" + key, nil
	case strings.HasPrefix(warehouseURL, "s3://"):
		rest := strings.TrimPrefix(warehouseURL, "s3://")
		bucketName := rest
		if i := strings.Index(rest, "?"); i >= 0 {
			bucketName = rest[:i]
		}
		bucketName = strings.TrimSuffix(bucketName, "/")
		return "s3://" + bucketName + "/" + key, nil
	default:
		return "", fmt.Errorf("unsupported warehouse URL scheme: %s", warehouseURL)
	}
}

// === unimplemented Catalog methods ===
//
// The MVP janitor only needs LoadTable + CommitTable. The rest of the
// catalog interface is stubbed and will land alongside the maintenance
// ops that need them. We return errors that explain the gap rather than
// silently doing nothing.

var errNotImplemented = errors.New("DirectoryCatalog: method not implemented in MVP")

// ErrCASConflict is returned by CommitTable when another writer
// committed v(N+1).metadata.json before us. Callers MUST retry from
// scratch: reload the current table state, rebuild the new metadata,
// retry the conditional write. errors.Is(err, ErrCASConflict) is the
// correct way to detect this case.
var ErrCASConflict = errors.New("metadata commit CAS conflict: another writer won the v(N+1) race")

func (c *DirectoryCatalog) CreateTable(ctx context.Context, identifier icebergtable.Identifier, schema *icebergpkg.Schema, opts ...icebergcat.CreateTableOpt) (*icebergtable.Table, error) {
	return nil, errNotImplemented
}

func (c *DirectoryCatalog) ListTables(ctx context.Context, namespace icebergtable.Identifier) iter.Seq2[icebergtable.Identifier, error] {
	return func(yield func(icebergtable.Identifier, error) bool) {
		// Walk the namespace via DiscoverTables and yield each.
		prefix := strings.Join(namespace, "/")
		tables, err := DiscoverTables(ctx, c.bucket, prefix)
		if err != nil {
			yield(nil, err)
			return
		}
		for _, t := range tables {
			parts := strings.Split(t.Prefix, "/")
			if !yield(icebergtable.Identifier(parts), nil) {
				return
			}
		}
	}
}

func (c *DirectoryCatalog) DropTable(ctx context.Context, identifier icebergtable.Identifier) error {
	return errNotImplemented
}

func (c *DirectoryCatalog) RenameTable(ctx context.Context, from, to icebergtable.Identifier) (*icebergtable.Table, error) {
	return nil, errNotImplemented
}

func (c *DirectoryCatalog) CheckTableExists(ctx context.Context, identifier icebergtable.Identifier) (bool, error) {
	prefix := identToPrefix(identifier)
	tables, err := DiscoverTables(ctx, c.bucket, prefix)
	if err != nil {
		return false, err
	}
	return len(tables) > 0, nil
}

func (c *DirectoryCatalog) ListNamespaces(ctx context.Context, parent icebergtable.Identifier) ([]icebergtable.Identifier, error) {
	return nil, errNotImplemented
}

func (c *DirectoryCatalog) CreateNamespace(ctx context.Context, namespace icebergtable.Identifier, props icebergpkg.Properties) error {
	return errNotImplemented
}

func (c *DirectoryCatalog) DropNamespace(ctx context.Context, namespace icebergtable.Identifier) error {
	return errNotImplemented
}

func (c *DirectoryCatalog) CheckNamespaceExists(ctx context.Context, namespace icebergtable.Identifier) (bool, error) {
	return false, errNotImplemented
}

func (c *DirectoryCatalog) LoadNamespaceProperties(ctx context.Context, namespace icebergtable.Identifier) (icebergpkg.Properties, error) {
	return nil, errNotImplemented
}

func (c *DirectoryCatalog) UpdateNamespaceProperties(ctx context.Context, namespace icebergtable.Identifier, removals []string, updates icebergpkg.Properties) (icebergcat.PropertiesUpdateSummary, error) {
	return icebergcat.PropertiesUpdateSummary{}, errNotImplemented
}

// View support is not part of the MVP.
func (c *DirectoryCatalog) CreateView(ctx context.Context, identifier icebergtable.Identifier, schema *icebergpkg.Schema, viewVersion view.Version, props icebergpkg.Properties) error {
	return errNotImplemented
}

func (c *DirectoryCatalog) DropView(ctx context.Context, identifier icebergtable.Identifier) error {
	return errNotImplemented
}

func (c *DirectoryCatalog) ListViews(ctx context.Context, namespace icebergtable.Identifier) iter.Seq2[icebergtable.Identifier, error] {
	return func(yield func(icebergtable.Identifier, error) bool) {}
}

func (c *DirectoryCatalog) LoadView(ctx context.Context, identifier icebergtable.Identifier) (view.Metadata, error) {
	return nil, errNotImplemented
}

func (c *DirectoryCatalog) CheckViewExists(ctx context.Context, identifier icebergtable.Identifier) (bool, error) {
	return false, nil
}
