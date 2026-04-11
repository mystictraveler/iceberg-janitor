package classify

import "time"

// MaintainMode is the maintenance loop the orchestrator should run for
// a table given its workload class. The two loops have very different
// CAS-pressure characteristics:
//
//   - HOT: small, frequent delta-stitch operations on actively-written
//     partitions. Optimized to minimize the writer-fight CAS race
//     window. Used by streaming tables.
//
//   - COLD: long-running, full per-partition compaction across the
//     whole table. Tested every cycle but only fired for partitions
//     whose triggers (small_files / metadata_ratio / stale_rewrite)
//     are met. No CAS urgency — runs sequentially. Used by batch /
//     slow_changing tables, and by streaming tables on a slower
//     parallel cadence.
//
//   - FULL: legacy single-pass full table compaction. Behaves the
//     same as COLD but without per-partition trigger filtering.
//     Kept for explicit override and tests.
type MaintainMode string

const (
	ModeHot  MaintainMode = "hot"
	ModeCold MaintainMode = "cold"
	ModeFull MaintainMode = "full"
)

// MaintainOptions is the maintenance plan for a table given its
// workload class. The orchestrator computes this from the table's
// classification (no caller knobs needed) and passes it to the
// hot/cold loops.
type MaintainOptions struct {
	Mode                MaintainMode
	KeepLastSnapshots   int
	KeepWithin          time.Duration
	TargetFileSizeBytes int64
	SmallFileThreshold  int           // per-partition trigger
	StaleRewriteAge     time.Duration // per-partition trigger
	WriteBufferSeconds  int
}

// ClassToOptions maps a workload class to its default maintenance plan.
// The orchestrator uses this so callers don't need to pass any knobs to
// the maintain endpoint — the server figures out what to do based on
// the table's classification.
func ClassToOptions(class WorkloadClass) MaintainOptions {
	t := ClassDefaults[class]
	switch class {
	case ClassStreaming:
		return MaintainOptions{
			Mode:                ModeHot,
			KeepLastSnapshots:   5,
			KeepWithin:          1 * time.Hour,
			TargetFileSizeBytes: 64 * 1024 * 1024, // 64 MiB
			SmallFileThreshold:  t.SmallFileTrigger,
			StaleRewriteAge:     30 * time.Minute,
			WriteBufferSeconds:  t.WriteBufferSeconds,
		}
	case ClassBatch:
		return MaintainOptions{
			Mode:                ModeCold,
			KeepLastSnapshots:   10,
			KeepWithin:          24 * time.Hour,
			TargetFileSizeBytes: 128 * 1024 * 1024, // 128 MiB
			SmallFileThreshold:  t.SmallFileTrigger,
			StaleRewriteAge:     6 * time.Hour,
			WriteBufferSeconds:  t.WriteBufferSeconds,
		}
	case ClassSlowChanging:
		return MaintainOptions{
			Mode:                ModeCold,
			KeepLastSnapshots:   20,
			KeepWithin:          7 * 24 * time.Hour,
			TargetFileSizeBytes: 256 * 1024 * 1024, // 256 MiB
			SmallFileThreshold:  t.SmallFileTrigger,
			StaleRewriteAge:     7 * 24 * time.Hour,
			WriteBufferSeconds:  0,
		}
	case ClassDormant:
		// Dormant: still test, but only compact if triggers fire.
		// The orchestrator typically skips dormant tables entirely;
		// this is the option set for an explicit operator-forced run.
		return MaintainOptions{
			Mode:                ModeCold,
			KeepLastSnapshots:   50,
			KeepWithin:          30 * 24 * time.Hour,
			TargetFileSizeBytes: 512 * 1024 * 1024, // 512 MiB
			SmallFileThreshold:  t.SmallFileTrigger,
			StaleRewriteAge:     30 * 24 * time.Hour,
			WriteBufferSeconds:  0,
		}
	default:
		// Unknown class falls back to a conservative cold cycle.
		return MaintainOptions{
			Mode:                ModeCold,
			KeepLastSnapshots:   10,
			KeepWithin:          24 * time.Hour,
			TargetFileSizeBytes: 128 * 1024 * 1024,
			SmallFileThreshold:  100,
			StaleRewriteAge:     24 * time.Hour,
			WriteBufferSeconds:  0,
		}
	}
}
