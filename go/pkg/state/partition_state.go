package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// PartitionState records the janitor's per-partition history. It is
// the persistent companion to TableState — TableState tracks the
// table as a whole (last run, failure counter, pause status), this
// tracks each partition's last rewrite/compact so the cold-loop
// stale-rewrite trigger can fire.
//
// Stored at _janitor/state/<table_uuid>/partitions.json (one file per
// table containing all partitions). Bounded size: ~1KB per partition,
// so a 1000-partition table is ~1MB on the wire.
type PartitionState struct {
	TableUUID  string                       `json:"table_uuid"`
	UpdatedAt  time.Time                    `json:"updated_at"`
	Partitions map[string]PartitionRecord  `json:"partitions"`
}

// PartitionRecord is the per-partition janitor history. The cold loop
// reads LastRewriteAt and computes age vs the StaleRewriteAge
// threshold for the table's workload class.
type PartitionRecord struct {
	// PartitionKey is redundant with the map key but stored for the
	// case where a caller iterates the records as a slice.
	PartitionKey string `json:"partition_key"`

	// LastRewriteAt is when this partition was last successfully
	// compacted or stitched. Zero value means "never", which makes
	// the stale-rewrite trigger fire on the first cold-loop pass.
	LastRewriteAt time.Time `json:"last_rewrite_at"`

	// LastRewriteFiles is the file count after the last rewrite, for
	// rough drift detection (if the partition has grown 10x since
	// the last rewrite, it probably needs another).
	LastRewriteFiles int `json:"last_rewrite_files"`

	// LastRewriteBytes is the total partition size after the last
	// rewrite. Same drift-detection use as LastRewriteFiles.
	LastRewriteBytes int64 `json:"last_rewrite_bytes"`

	// LastRewriteOp tells you whether this partition was last touched
	// by full compact, delta stitch, or manifest rewrite. Useful for
	// auditing the hot vs cold loop activity per partition.
	LastRewriteOp string `json:"last_rewrite_op"`
}

// PartitionStateKey is the bucket-relative key for a table's
// partition-state file. Same naming convention as StateKey.
func PartitionStateKey(tableUUID string) string {
	return path.Join("_janitor", "state", tableUUID, "partitions.json")
}

// LoadPartitionState reads the per-partition state file. If the file
// does not exist, returns an empty PartitionState (with TableUUID set)
// and nil error — first-run tables have no partition history.
func LoadPartitionState(ctx context.Context, bucket *blob.Bucket, tableUUID string) (*PartitionState, error) {
	key := PartitionStateKey(tableUUID)
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return &PartitionState{TableUUID: tableUUID, Partitions: map[string]PartitionRecord{}}, nil
		}
		return nil, fmt.Errorf("opening partition state %s: %w", key, err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading partition state %s: %w", key, err)
	}
	if len(body) == 0 {
		return &PartitionState{TableUUID: tableUUID, Partitions: map[string]PartitionRecord{}}, nil
	}
	var s PartitionState
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("decoding partition state %s: %w", key, err)
	}
	if s.TableUUID == "" {
		s.TableUUID = tableUUID
	}
	if s.Partitions == nil {
		s.Partitions = map[string]PartitionRecord{}
	}
	return &s, nil
}

// SavePartitionState writes the per-partition state file. Same
// last-writer-wins semantics as Save — the lease primitive will
// exclude concurrent writers when it lands.
func SavePartitionState(ctx context.Context, bucket *blob.Bucket, s *PartitionState) error {
	if s == nil {
		return errors.New("SavePartitionState: nil state")
	}
	if s.TableUUID == "" {
		return errors.New("SavePartitionState: empty TableUUID")
	}
	s.UpdatedAt = time.Now()
	key := PartitionStateKey(s.TableUUID)
	body, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding partition state: %w", err)
	}
	w, err := bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("opening writer for %s: %w", key, err)
	}
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return fmt.Errorf("writing partition state %s: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing partition state %s: %w", key, err)
	}
	return nil
}

// Record sets the rewrite record for a partition. Used by the hot/cold
// loops on successful compaction or delta stitch.
func (s *PartitionState) Record(partitionKey, op string, files int, bytes int64) {
	if s.Partitions == nil {
		s.Partitions = map[string]PartitionRecord{}
	}
	s.Partitions[partitionKey] = PartitionRecord{
		PartitionKey:     partitionKey,
		LastRewriteAt:    time.Now(),
		LastRewriteFiles: files,
		LastRewriteBytes: bytes,
		LastRewriteOp:    op,
	}
}

// LastRewriteAges returns a map of partition key → age since last
// rewrite, suitable for passing to AnalyzePartitions's
// LastRewriteAges option. Partitions with no record are absent.
func (s *PartitionState) LastRewriteAges(now time.Time) map[string]time.Duration {
	out := make(map[string]time.Duration, len(s.Partitions))
	for k, r := range s.Partitions {
		if r.LastRewriteAt.IsZero() {
			continue
		}
		out[k] = now.Sub(r.LastRewriteAt)
	}
	return out
}
