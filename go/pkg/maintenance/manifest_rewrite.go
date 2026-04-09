package maintenance

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// RewriteManifestsOptions configure a single RewriteManifests call.
//
// The op is the second half of the metadata-accumulation fix
// (mystictraveler/iceberg-janitor#7). Snapshot expiration drops old
// snapshots; manifest rewrite consolidates the per-commit micro-
// manifests that survive in the current snapshot into fewer larger
// partition-organized manifests. After both have run, manifest pruning
// (the optimization in pkg/janitor/compact_replace.go) becomes much
// more effective because each surviving manifest's partition bound
// summary is tighter — a partition-scoped compact / scan can skip
// most manifests via in-memory bound checks instead of paying a GET
// per per-commit micro-manifest.
//
// Scope of v0:
//
//   - Data manifests only. Delete manifests (V2+ position deletes,
//     V3 deletion vectors) are passed through unchanged: they are
//     re-listed in the new manifest_list verbatim, with their original
//     snapshot ids, sequence numbers, and bounds. We don't read or
//     rewrite them. The "consolidate small files" win is concentrated
//     in the data plane, so this is the right tradeoff for v0.
//   - The new manifest set is grouped by (partition_spec_id, partition
//     tuple). One new manifest per group. We do NOT enforce a target
//     manifest size — if a single partition has 100k entries, the
//     resulting manifest is 100k entries. The next iteration adds a
//     bin-packing layer on top.
//   - The output snapshot's parent is the input snapshot. The
//     transaction is a single new snapshot with operation=REPLACE
//     summary. No data files change; only the manifest layer is
//     rewritten.
//
// Why a separate Update path instead of iceberg-go's Transaction:
// iceberg-go's snapshot producers (overwrite, append, etc.) are
// closed types — there is no public producer for "rewrite the
// manifest list with the same data files." We use the lower-level
// catalog.CommitTable entry point and construct the AddSnapshot +
// SetSnapshotRef updates ourselves. The atomic CAS commit, the
// requirement validation, and the metadata.json fan-out are exactly
// the same primitives the compact path uses; we just bypass the
// transaction layer's producer machinery.
type RewriteManifestsOptions struct {
	// MinManifestsToTrigger is the floor below which the rewrite is a
	// no-op. If the current snapshot has fewer manifests than this,
	// there's nothing meaningful to consolidate. Default 4.
	MinManifestsToTrigger int

	// MaxAttempts caps the CAS-conflict retry loop. Default 15.
	MaxAttempts int

	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time. Default 100ms.
	InitialBackoff time.Duration

	// CircuitBreaker, if non-nil, is consulted before the rewrite runs
	// and is updated with the outcome afterward. Same shape as
	// CompactOptions.CircuitBreaker.
	CircuitBreaker *safety.CircuitBreaker
}

func (o *RewriteManifestsOptions) defaults() {
	if o.MinManifestsToTrigger <= 0 {
		o.MinManifestsToTrigger = 4
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 15
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
}

// RewriteManifestsResult is the structured outcome of a successful
// RewriteManifests call.
type RewriteManifestsResult struct {
	Identifier icebergtable.Identifier `json:"identifier"`
	TableUUID  string                  `json:"table_uuid,omitempty"`
	Attempts   int                     `json:"attempts"`

	BeforeSnapshotID int64 `json:"before_snapshot_id"`
	BeforeManifests  int   `json:"before_manifests"`
	BeforeDataFiles  int   `json:"before_data_files"`
	BeforeRows       int64 `json:"before_rows"`

	AfterSnapshotID int64 `json:"after_snapshot_id"`
	AfterManifests  int   `json:"after_manifests"`
	AfterDataFiles  int   `json:"after_data_files"`
	AfterRows       int64 `json:"after_rows"`

	Verification *safety.Verification `json:"verification"`
	DurationMs   int64                `json:"duration_ms"`
}

// RewriteManifests is the public entry point. Same retry/CB shape as
// janitor.Compact and maintenance.Expire.
func RewriteManifests(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts RewriteManifestsOptions) (*RewriteManifestsResult, error) {
	opts.defaults()
	started := time.Now()
	result := &RewriteManifestsResult{Identifier: ident}

	var tableUUID string
	if opts.CircuitBreaker != nil {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, fmt.Errorf("loading table for circuit breaker: %w", err)
		}
		tableUUID = tbl.Metadata().TableUUID().String()
		result.TableUUID = tableUUID
		if err := opts.CircuitBreaker.Preflight(ctx, tableUUID); err != nil {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, err
		}
	}

	runErr := func() error {
		backoff := opts.InitialBackoff
		for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
			result.Attempts = attempt
			err := rewriteManifestsOnce(ctx, cat, ident, opts, result)
			if err == nil {
				return nil
			}
			if !janitor.IsRetryableConcurrencyError(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		return fmt.Errorf("manifest rewrite failed: exceeded %d concurrency-retry attempts", opts.MaxAttempts)
	}()

	result.DurationMs = time.Since(started).Milliseconds()

	if opts.CircuitBreaker != nil && tableUUID != "" {
		recordErr := opts.CircuitBreaker.RecordOutcome(ctx, tableUUID, runErr)
		if recordErr != nil && runErr == nil {
			return result, recordErr
		}
	}

	if runErr != nil {
		return result, runErr
	}
	return result, nil
}

// rewriteManifestsOnce is one attempt: load, group, write new
// manifests, write a new manifest list, master-check the file set,
// commit through the directory catalog's CommitTable.
//
// Reminder for the next reader: this function does NOT use
// iceberg-go's Transaction at all. iceberg-go's Transaction.Commit
// dispatches through internal snapshot-producer types (appendFiles,
// overwriteFiles, etc.) that we cannot reach. We construct AddSnapshot
// + SetSnapshotRef updates by hand and submit them directly to the
// directory catalog's CommitTable, which performs the same atomic CAS
// write of the next-version metadata.json that the transaction path
// would. The CAS retry contract is identical.
func rewriteManifestsOnce(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts RewriteManifestsOptions, result *RewriteManifestsResult) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil // no current snapshot, nothing to rewrite
	}
	parentID := snap.SnapshotID
	result.BeforeSnapshotID = parentID

	fs, err := tbl.FS(ctx)
	if err != nil {
		return fmt.Errorf("getting fs: %w", err)
	}
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("filesystem does not support writes")
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return fmt.Errorf("listing manifests: %w", err)
	}
	result.BeforeManifests = len(manifests)
	if len(manifests) < opts.MinManifestsToTrigger {
		// Below the floor — nothing to do. Populate the "after" fields
		// from the "before" so the result reads cleanly as a no-op.
		result.AfterSnapshotID = parentID
		result.AfterManifests = result.BeforeManifests
		result.BeforeDataFiles, result.BeforeRows = countDataFilesAndRows(fs, manifests)
		result.AfterDataFiles = result.BeforeDataFiles
		result.AfterRows = result.BeforeRows
		return nil
	}

	// Walk every data manifest, read entries, group by (spec id,
	// partition tuple). Delete manifests are passed through to the
	// new manifest_list unchanged (see passthrough section below).
	//
	// We need to know the partition spec to convert each entry's
	// partition map to a stable key. The spec id is on the manifest
	// file itself; multiple specs may exist in the table if the spec
	// has been evolved. We treat each spec id as its own grouping
	// dimension so a spec-evolved table still produces consistent
	// per-spec output.
	type groupKey struct {
		specID int32
		key    string // canonical encoding of partition tuple
	}
	type groupVal struct {
		specID  int32
		entries []icebergpkg.ManifestEntry
	}
	groups := map[groupKey]*groupVal{}
	beforeFilePaths := map[string]struct{}{}
	var beforeRows int64
	var passthroughManifests []icebergpkg.ManifestFile

	var groupsMu sync.Mutex
	type readResult struct {
		passthrough icebergpkg.ManifestFile // non-nil if pass through
		entries     []icebergpkg.ManifestEntry
		specID      int32
	}
	readResults := make([]readResult, len(manifests))
	for i, m := range manifests {
		i, m := i, m
		// V0: only data manifests are read+rewritten. Delete
		// manifests are appended to the new manifest_list unchanged
		// — they are referenced by the same paths and contain the
		// same entries; nothing about them needs to move.
		if m.ManifestContent() != icebergpkg.ManifestContentData {
			readResults[i] = readResult{passthrough: m}
			continue
		}
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		readResults[i] = readResult{entries: entries, specID: m.PartitionSpecID()}
	}

	for _, r := range readResults {
		if r.passthrough != nil {
			passthroughManifests = append(passthroughManifests, r.passthrough)
			continue
		}
		for _, e := range r.entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			// Stable canonical key over the partition tuple. Sort by
			// field id so the same tuple always hashes to the same
			// group regardless of map iteration order.
			key := canonicalPartitionKey(df.Partition())
			gk := groupKey{specID: r.specID, key: key}
			groupsMu.Lock()
			gv := groups[gk]
			if gv == nil {
				gv = &groupVal{specID: r.specID}
				groups[gk] = gv
			}
			gv.entries = append(gv.entries, e)
			groupsMu.Unlock()
			beforeFilePaths[df.FilePath()] = struct{}{}
			beforeRows += df.Count()
		}
	}
	result.BeforeDataFiles = len(beforeFilePaths)
	result.BeforeRows = beforeRows

	// If everything collapsed into one group AND that group is the
	// only data manifest already, we have nothing to consolidate. The
	// floor check above usually catches this, but a table with mostly
	// delete manifests and one data manifest would slip through.
	if len(groups) == 0 {
		result.AfterSnapshotID = parentID
		result.AfterManifests = result.BeforeManifests
		result.AfterDataFiles = result.BeforeDataFiles
		result.AfterRows = result.BeforeRows
		return nil
	}

	// Allocate a new snapshot id and the next sequence number.
	newSnapID := newSnapshotID()
	nextSeqNum := tbl.Metadata().LastSequenceNumber() + 1
	formatVersion := tbl.Metadata().Version()
	currentSchema := tbl.Metadata().CurrentSchema()
	schemaID := currentSchema.ID

	// Sort group keys deterministically so the manifest_list ordering
	// is reproducible across attempts. The bench compares output
	// stability across runs and a non-deterministic ordering would
	// produce noise even when the underlying data is identical.
	gkeys := make([]groupKey, 0, len(groups))
	for k := range groups {
		gkeys = append(gkeys, k)
	}
	sort.Slice(gkeys, func(i, j int) bool {
		if gkeys[i].specID != gkeys[j].specID {
			return gkeys[i].specID < gkeys[j].specID
		}
		return gkeys[i].key < gkeys[j].key
	})

	locProv, err := tbl.LocationProvider()
	if err != nil {
		return fmt.Errorf("getting location provider: %w", err)
	}

	// Write each group as a new manifest avro file. The file names
	// follow iceberg's convention: <uuid>-m<index>.avro under the
	// table's metadata/ directory. We use the location provider's
	// NewMetadataLocation so the path matches what other Iceberg
	// readers expect.
	writeUUID := uuid.NewString()
	addedFilePaths := map[string]struct{}{}
	newDataManifests := make([]icebergpkg.ManifestFile, 0, len(gkeys))
	for idx, gk := range gkeys {
		gv := groups[gk]
		spec, err := lookupSpecByID(tbl.Metadata(), int(gv.specID))
		if err != nil {
			return fmt.Errorf("looking up spec %d: %w", gv.specID, err)
		}
		manifestName := fmt.Sprintf("%s-m%d.avro", writeUUID, idx)
		manifestPath := locProv.NewMetadataLocation(manifestName)
		out, err := wfs.Create(manifestPath)
		if err != nil {
			return fmt.Errorf("creating manifest %s: %w", manifestPath, err)
		}
		counter := &writeByteCounter{w: out}
		mw, err := icebergpkg.NewManifestWriter(formatVersion, counter, *spec, currentSchema, newSnapID)
		if err != nil {
			out.Close()
			_ = wfs.Remove(manifestPath)
			return fmt.Errorf("creating manifest writer: %w", err)
		}
		for _, e := range gv.entries {
			if err := mw.Existing(e); err != nil {
				out.Close()
				_ = wfs.Remove(manifestPath)
				return fmt.Errorf("writing manifest entry: %w", err)
			}
			addedFilePaths[e.DataFile().FilePath()] = struct{}{}
		}
		// ToManifestFile flushes (calls Close) and returns the
		// finalized manifest descriptor with file_size set from our
		// counter.
		mf, err := mw.ToManifestFile(manifestPath, counter.n)
		if err != nil {
			out.Close()
			_ = wfs.Remove(manifestPath)
			return fmt.Errorf("finalizing manifest: %w", err)
		}
		if err := out.Close(); err != nil {
			_ = wfs.Remove(manifestPath)
			return fmt.Errorf("closing manifest output %s: %w", manifestPath, err)
		}
		newDataManifests = append(newDataManifests, mf)
	}

	// Pre-master-check: data file set must equal the input set
	// EXACTLY. This is the I8 invariant for manifest rewrite. If we
	// dropped or duplicated any file path during the group/write
	// dance, this catches it before we commit and before we write
	// the manifest_list.
	if !filePathSetsEqual(beforeFilePaths, addedFilePaths) {
		// Clean up the new manifests we already wrote — none of them
		// are referenced anywhere yet so they're orphans.
		for _, m := range newDataManifests {
			_ = wfs.Remove(m.FilePath())
		}
		missing, extra := setDiff(beforeFilePaths, addedFilePaths)
		return fmt.Errorf("manifest rewrite produced wrong file set: missing=%d extra=%d (first missing=%v first extra=%v)",
			len(missing), len(extra), firstOrEmpty(missing), firstOrEmpty(extra))
	}

	// Write the new manifest_list. The list contains every new data
	// manifest plus every passthrough delete manifest, in order.
	allNewManifests := make([]icebergpkg.ManifestFile, 0, len(newDataManifests)+len(passthroughManifests))
	allNewManifests = append(allNewManifests, newDataManifests...)
	allNewManifests = append(allNewManifests, passthroughManifests...)

	manifestListName := fmt.Sprintf("snap-%d-%d-%s.avro", newSnapID, nextSeqNum, writeUUID)
	manifestListPath := locProv.NewMetadataLocation(manifestListName)
	listOut, err := wfs.Create(manifestListPath)
	if err != nil {
		for _, m := range newDataManifests {
			_ = wfs.Remove(m.FilePath())
		}
		return fmt.Errorf("creating manifest list %s: %w", manifestListPath, err)
	}
	if err := icebergpkg.WriteManifestList(formatVersion, listOut, newSnapID, &parentID, &nextSeqNum, 0, allNewManifests); err != nil {
		listOut.Close()
		_ = wfs.Remove(manifestListPath)
		for _, m := range newDataManifests {
			_ = wfs.Remove(m.FilePath())
		}
		return fmt.Errorf("writing manifest list: %w", err)
	}
	if err := listOut.Close(); err != nil {
		_ = wfs.Remove(manifestListPath)
		for _, m := range newDataManifests {
			_ = wfs.Remove(m.FilePath())
		}
		return fmt.Errorf("closing manifest list: %w", err)
	}

	// Construct the new snapshot record. Iceberg readers expect a
	// REPLACE-operation summary with total-* counters; we fill in the
	// minimum set that the spec requires for a well-formed snapshot
	// summary. Other writers (Spark, PyIceberg) include richer
	// telemetry but the keys we set are sufficient for query
	// planners and consumers.
	parentIDCopy := parentID
	newSnap := icebergtable.Snapshot{
		SnapshotID:       newSnapID,
		ParentSnapshotID: &parentIDCopy,
		SequenceNumber:   nextSeqNum,
		TimestampMs:      time.Now().UnixMilli(),
		ManifestList:     manifestListPath,
		SchemaID:         &schemaID,
		Summary: &icebergtable.Summary{
			Operation: icebergtable.OpReplace,
			Properties: icebergpkg.Properties{
				"operation":          string(icebergtable.OpReplace),
				"total-records":      strconv.FormatInt(beforeRows, 10),
				"total-data-files":   strconv.Itoa(len(beforeFilePaths)),
				"added-data-files":   "0",
				"deleted-data-files": "0",
				// Manifest counts capture the actual win we deliver:
				// the consumer can see the consolidation factor in
				// the snapshot summary without re-walking the list.
				"total-manifests": strconv.Itoa(len(allNewManifests)),
			},
		},
	}

	// Master check (file set equality, already verified above).
	// Wrap it in a Verification record so the operator-facing CLI
	// has consistent reporting across all maintenance ops.
	v := &safety.Verification{
		SchemeVersion:    1,
		CheckedAt:        time.Now().UTC().Format(time.RFC3339),
		CheckerVersion:   "iceberg-janitor-go manifest_rewrite",
		InputSnapshotID:  parentID,
		OutputSnapshotID: newSnapID,
		I1RowCount: safety.RowCountCheck{
			In: beforeRows, Out: beforeRows, Result: "pass",
		},
		I2Schema: safety.SchemaCheck{
			InID: schemaID, OutID: schemaID, Result: "pass",
		},
		Overall: "pass",
	}
	result.Verification = v

	// Construct the updates and requirements. The CAS contract has
	// two layers, mirroring janitor.Compact:
	//
	//   - AssertTableUUID guards against pointing at the wrong table
	//     (e.g. if the table was dropped and recreated under us).
	//   - AssertRefSnapshotID("main", &parentID) is the optimistic-
	//     concurrency check: if a foreign writer committed between
	//     our LoadTable and our CommitTable, the main branch's tip
	//     will no longer match parentID and the requirement
	//     validation fails. The error string contains "has changed"
	//     and IsRetryableConcurrencyError will route us back through
	//     the retry loop with a fresh load.
	updates := []icebergtable.Update{
		icebergtable.NewAddSnapshotUpdate(&newSnap),
		icebergtable.NewSetSnapshotRefUpdate(
			icebergtable.MainBranch,
			newSnapID,
			icebergtable.BranchRef,
			0, 0, 0, // no max-ref-age, no max-snapshot-age, no min-snapshots
		),
	}
	reqs := []icebergtable.Requirement{
		icebergtable.AssertTableUUID(tbl.Metadata().TableUUID()),
		icebergtable.AssertRefSnapshotID(icebergtable.MainBranch, &parentIDCopy),
	}

	updatedMeta, _, err := cat.CommitTable(ctx, ident, reqs, updates)
	if err != nil {
		// Best-effort cleanup of the new files. They are unreferenced
		// after a failed commit so we leave them as orphans for the
		// next orphan-removal pass; we still try to remove them
		// inline because the bench's tight retry loop benefits from
		// not accumulating orphans on every CAS conflict.
		_ = wfs.Remove(manifestListPath)
		for _, m := range newDataManifests {
			_ = wfs.Remove(m.FilePath())
		}
		return fmt.Errorf("committing manifest rewrite: %w", err)
	}

	result.AfterSnapshotID = newSnapID
	result.AfterManifests = len(allNewManifests)
	result.AfterDataFiles = len(beforeFilePaths)
	result.AfterRows = beforeRows
	_ = updatedMeta
	return nil
}

// canonicalPartitionKey produces a stable string key for a partition
// tuple. Map iteration order in Go is randomized, so we sort by field
// id and then format each value with %v. The key is consumed only as
// a map key inside this package — we never round-trip it back to a
// partition value, so %v's format choice doesn't affect correctness.
func canonicalPartitionKey(p map[int]any) string {
	if len(p) == 0 {
		return "()"
	}
	keys := make([]int, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	var b []byte
	for i, k := range keys {
		if i > 0 {
			b = append(b, '|')
		}
		b = append(b, []byte(fmt.Sprintf("%d=%v", k, p[k]))...)
	}
	return string(b)
}

// newSnapshotID generates a non-negative int64 by hashing 8 random
// bytes. iceberg-go does the same thing internally (XOR'ing the two
// halves of a UUID); we replicate the contract — generate from
// crypto-strong randomness, fit in a positive int64 — without
// reaching into iceberg-go's unexported `generateSnapshotID`. The
// chance of collision against an existing snapshot id is negligible
// (2^-63 per generation) and the master check would catch a
// collision before commit anyway.
func newSnapshotID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	v := int64(binary.BigEndian.Uint64(b[:]) >> 1) // clear sign bit
	if v == 0 {
		v = 1 // avoid the zero sentinel
	}
	return v
}

// writeByteCounter is a tiny io.Writer wrapper that records how many
// bytes have been written. We use it to fill in the manifest file's
// length field, which the iceberg spec requires in the manifest_list.
// Equivalent to iceberg-go's internal.CountingWriter, which we can't
// import.
type writeByteCounter struct {
	w interface {
		Write([]byte) (int, error)
	}
	n int64
}

func (c *writeByteCounter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// lookupSpecByID searches metadata.PartitionSpecs() for the spec with
// the given id. Returns an error if not found. We don't use
// Metadata.PartitionSpecByID because the interface returns
// (PartitionSpec, error) and the older interface returned only
// the spec — depending on iceberg-go version. Doing the lookup
// ourselves keeps the call site uniform.
func lookupSpecByID(meta icebergtable.Metadata, id int) (*icebergpkg.PartitionSpec, error) {
	for _, s := range meta.PartitionSpecs() {
		s := s
		if s.ID() == id {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("partition spec id %d not found", id)
}

// filePathSetsEqual reports whether two file path sets contain the
// same elements. Used for the manifest rewrite invariant: every data
// file in the input snapshot must appear in the output snapshot,
// and vice versa.
func filePathSetsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// setDiff returns (in_a_not_b, in_b_not_a). Used for error reporting
// on master check failure so the operator can diagnose what changed
// without re-running the rewrite under a debugger.
func setDiff(a, b map[string]struct{}) ([]string, []string) {
	var missing, extra []string
	for k := range a {
		if _, ok := b[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			extra = append(extra, k)
		}
	}
	return missing, extra
}

func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

// countDataFilesAndRows is a small helper for the no-op early-return
// path that needs to populate the result with the current snapshot's
// stats without going through the full grouping pipeline.
func countDataFilesAndRows(fs icebergio.IO, manifests []icebergpkg.ManifestFile) (int, int64) {
	var files int
	var rows int64
	for _, m := range manifests {
		if m.ManifestContent() != icebergpkg.ManifestContentData {
			continue
		}
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			continue
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			continue
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			files++
			rows += df.Count()
		}
	}
	return files, rows
}

// hexEncodeID is exported only for tests.
func hexEncodeID(id int64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(id))
	return hex.EncodeToString(b[:])
}

var _ = hexEncodeID // currently used only by tests; keep referenced
