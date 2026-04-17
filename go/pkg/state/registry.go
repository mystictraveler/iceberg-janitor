package state

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"golang.org/x/sync/errgroup"
)

// Warehouse-level table registry.
//
// Sharded per-table: each table has its own file at
// _janitor/registry/<table_uuid>.json. This avoids the single-file
// contention problem that a monolithic registry.json would have with
// 3+ server replicas. Per-table writes are already serialized by the
// existing per-table lease, so no new coordination primitive is
// needed.
//
// The scheduler reads the registry prefix listing (~500ms for 10K
// entries, vs ~25s for a full warehouse metadata.json prefix scan)
// and selects tables whose next_maintain_after has passed.

// RegistryEntry is the per-table record persisted at
// _janitor/registry/<table_uuid>.json. Updated by the maintain
// handler after each maintenance cycle.
type RegistryEntry struct {
	TableUUID        string    `json:"table_uuid"`
	Namespace        string    `json:"namespace"`
	Table            string    `json:"table"`
	Class            string    `json:"class"`
	LastSeen         time.Time `json:"last_seen"`
	LastMaintained   time.Time `json:"last_maintained"`
	NextMaintainAt   time.Time `json:"next_maintain_at"`
	MetadataLocation string    `json:"metadata_location,omitempty"`
	BeforeFiles      int       `json:"before_files,omitempty"`
	AfterFiles       int       `json:"after_files,omitempty"`
	LastError        string    `json:"last_error,omitempty"`

	// SchemaID is the current-schema-id of the table at the time of
	// the last successful maintain. Updated on every sweep. When a
	// subsequent sweep finds a different schema-id, the scheduler
	// knows the table evolved its schema — the registry entry is
	// updated so the mixed-schema compaction guard
	// (pkg/janitor/schema_group.go) can fire correctly on the next
	// maintain cycle rather than wasting a round discovering the
	// mismatch at stitch time.
	SchemaID int `json:"schema_id"`

	// SnapshotID is the snapshot-id the table was at when the last
	// successful maintain completed. Used by the scheduler to detect
	// whether the table has new commits since last maintain: if the
	// current snapshot-id matches SnapshotID, no foreign writer
	// committed and maintenance is skippable regardless of cadence.
	// This is the "nothing to do" fast path that avoids loading
	// table metadata entirely.
	SnapshotID int64 `json:"snapshot_id,omitempty"`
}

// RegistryMeta is a lightweight warehouse-level metadata file at
// _janitor/registry/_meta.json. Records when the last full prefix
// scan happened so the scheduler knows whether to re-scan.
type RegistryMeta struct {
	LastFullScan   time.Time `json:"last_full_scan"`
	ScanDurationMs int64     `json:"scan_duration_ms"`
	TableCount     int       `json:"table_count"`
}

const (
	registryPrefix = "_janitor/registry/"
	registryMeta   = "_janitor/registry/_meta.json"
)

// RegistryKey returns the S3 key for a table's registry entry.
func RegistryKey(tableUUID string) string {
	return path.Join(registryPrefix, tableUUID+".json")
}

// LoadRegistryEntry reads one table's registry entry. Returns nil +
// nil if the entry doesn't exist (table not yet registered).
func LoadRegistryEntry(ctx context.Context, bucket *blob.Bucket, tableUUID string) (*RegistryEntry, error) {
	key := RegistryKey(tableUUID)
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("opening registry entry %s: %w", key, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading registry entry %s: %w", key, err)
	}
	var entry RegistryEntry
	if err := json.Unmarshal(body, &entry); err != nil {
		return nil, fmt.Errorf("parsing registry entry %s: %w", key, err)
	}
	return &entry, nil
}

// SaveRegistryEntry writes one table's registry entry. Overwrites
// any existing entry (last-writer-wins, but the per-table lease
// serializes concurrent maintain calls for the same table so races
// are structurally prevented).
func SaveRegistryEntry(ctx context.Context, bucket *blob.Bucket, entry *RegistryEntry) error {
	key := RegistryKey(entry.TableUUID)
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshalling registry entry: %w", err)
	}
	return bucket.WriteAll(ctx, key, body, nil)
}

// DeleteRegistryEntry removes a table's registry entry. Used when a
// table is dropped from the warehouse and the full-scan detects its
// metadata is gone.
func DeleteRegistryEntry(ctx context.Context, bucket *blob.Bucket, tableUUID string) error {
	return bucket.Delete(ctx, RegistryKey(tableUUID))
}

// ListRegistryEntries reads ALL registry entries by prefix-listing
// _janitor/registry/ and loading each one. Returns entries sorted by
// NextMaintainAt ascending (most overdue first), which is the
// natural iteration order for the scheduler.
//
// Cost: one ListObjectsV2 prefix listing (~10 pages for 10K tables)
// + one GET per table (~20ms each, 32-way parallel). For 10K tables
// total wall time is ~500ms listing + ~6s for 10K GETs at 32
// concurrency. Compare: full warehouse prefix scan is ~25s listing
// alone.
func ListRegistryEntries(ctx context.Context, bucket *blob.Bucket) ([]*RegistryEntry, error) {
	var keys []string
	iter := bucket.List(&blob.ListOptions{Prefix: registryPrefix})
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			break
		}
		if obj.IsDir || strings.HasSuffix(obj.Key, "_meta.json") {
			continue
		}
		keys = append(keys, obj.Key)
	}

	entries := make([]*RegistryEntry, len(keys))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	for i, key := range keys {
		i, key := i, key
		g.Go(func() error {
			r, err := bucket.NewReader(gctx, key, nil)
			if err != nil {
				return nil // skip unreadable entries
			}
			defer r.Close()
			body, err := io.ReadAll(r)
			if err != nil {
				return nil
			}
			var entry RegistryEntry
			if err := json.Unmarshal(body, &entry); err != nil {
				return nil
			}
			entries[i] = &entry
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Filter out nil entries (skipped) and sort by NextMaintainAt.
	var result []*RegistryEntry
	for _, e := range entries {
		if e != nil {
			result = append(result, e)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].NextMaintainAt.Before(result[j].NextMaintainAt)
	})
	return result, nil
}

// DueForMaintenance returns the subset of entries where
// now >= NextMaintainAt, capped at maxTables. The input must be
// sorted by NextMaintainAt ascending (as ListRegistryEntries
// returns). This is the "what should the scheduler work on this
// cycle" selection.
func DueForMaintenance(entries []*RegistryEntry, now time.Time, maxTables int) []*RegistryEntry {
	var due []*RegistryEntry
	for _, e := range entries {
		if len(due) >= maxTables {
			break
		}
		if !now.Before(e.NextMaintainAt) {
			due = append(due, e)
		}
	}
	return due
}

// NextMaintainTime computes when a table should next be maintained
// based on its workload class. This is the cadence contract the
// scheduler enforces.
func NextMaintainTime(class string, lastMaintained time.Time) time.Time {
	switch class {
	case "streaming":
		return lastMaintained.Add(5 * time.Minute)
	case "batch":
		return lastMaintained.Add(1 * time.Hour)
	case "slow_changing":
		return lastMaintained.Add(24 * time.Hour)
	case "dormant":
		return lastMaintained.Add(7 * 24 * time.Hour)
	default:
		return lastMaintained.Add(1 * time.Hour)
	}
}

// LoadRegistryMeta reads the warehouse-level metadata file. Returns
// zero-value RegistryMeta if the file doesn't exist (= never
// scanned).
func LoadRegistryMeta(ctx context.Context, bucket *blob.Bucket) (RegistryMeta, error) {
	r, err := bucket.NewReader(ctx, registryMeta, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return RegistryMeta{}, nil
		}
		return RegistryMeta{}, fmt.Errorf("opening registry meta: %w", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return RegistryMeta{}, fmt.Errorf("reading registry meta: %w", err)
	}
	var meta RegistryMeta
	if err := json.Unmarshal(body, &meta); err != nil {
		return RegistryMeta{}, fmt.Errorf("parsing registry meta: %w", err)
	}
	return meta, nil
}

// SaveRegistryMeta writes the warehouse-level metadata file.
func SaveRegistryMeta(ctx context.Context, bucket *blob.Bucket, meta RegistryMeta) error {
	body, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshalling registry meta: %w", err)
	}
	return bucket.WriteAll(ctx, registryMeta, body, nil)
}
