package analyzer

import (
	"testing"
	"time"
)

func TestNeedsCompaction(t *testing.T) {
	cases := []struct {
		name       string
		smallFile  bool
		metaReduce bool
		stale      bool
		want       bool
	}{
		{"none", false, false, false, false},
		{"small_only", true, false, false, true},
		{"meta_only", false, true, false, true},
		{"stale_only", false, false, true, true},
		{"small_and_meta", true, true, false, true},
		{"all_three", true, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := PartitionHealth{
				NeedsSmallFileCompact:  tc.smallFile,
				NeedsMetadataReduction: tc.metaReduce,
				NeedsStaleRewrite:      tc.stale,
			}
			if got := p.NeedsCompaction(); got != tc.want {
				t.Errorf("NeedsCompaction()=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartitionHealthOptionsDefaults(t *testing.T) {
	var o PartitionHealthOptions
	o.defaults()
	if o.SmallFileThresholdBytes != 64*1024*1024 {
		t.Errorf("SmallFileThresholdBytes default = %d, want %d", o.SmallFileThresholdBytes, 64*1024*1024)
	}
	if o.SmallFileTrigger != 50 {
		t.Errorf("SmallFileTrigger default = %d, want 50", o.SmallFileTrigger)
	}
	if o.FileCountTrigger != 200 {
		t.Errorf("FileCountTrigger default = %d, want 200", o.FileCountTrigger)
	}
	if o.HotWindowSnapshots != 5 {
		t.Errorf("HotWindowSnapshots default = %d, want 5", o.HotWindowSnapshots)
	}
	if o.StaleRewriteAge != 24*time.Hour {
		t.Errorf("StaleRewriteAge default = %v, want 24h", o.StaleRewriteAge)
	}
}

func TestPartitionHealthOptionsDefaultsRespectsNonZero(t *testing.T) {
	o := PartitionHealthOptions{
		SmallFileThresholdBytes: 1024,
		SmallFileTrigger:        7,
		FileCountTrigger:        13,
		HotWindowSnapshots:      99,
		StaleRewriteAge:         5 * time.Minute,
	}
	o.defaults()
	if o.SmallFileThresholdBytes != 1024 {
		t.Errorf("SmallFileThresholdBytes overwritten: got %d", o.SmallFileThresholdBytes)
	}
	if o.SmallFileTrigger != 7 {
		t.Errorf("SmallFileTrigger overwritten: got %d", o.SmallFileTrigger)
	}
	if o.FileCountTrigger != 13 {
		t.Errorf("FileCountTrigger overwritten: got %d", o.FileCountTrigger)
	}
	if o.HotWindowSnapshots != 99 {
		t.Errorf("HotWindowSnapshots overwritten: got %d", o.HotWindowSnapshots)
	}
	if o.StaleRewriteAge != 5*time.Minute {
		t.Errorf("StaleRewriteAge overwritten: got %v", o.StaleRewriteAge)
	}
}

func TestPartitionKeyDeterministic(t *testing.T) {
	// Different map insertion orders must produce identical keys.
	a := partitionKey(map[string]string{"region": "us", "date": "2026-04-10"})
	b := partitionKey(map[string]string{"date": "2026-04-10", "region": "us"})
	if a != b {
		t.Errorf("partitionKey non-deterministic: %q vs %q", a, b)
	}
	want := "date=2026-04-10/region=us"
	if a != want {
		t.Errorf("partitionKey = %q, want %q", a, want)
	}
}

func TestPartitionKeyEmpty(t *testing.T) {
	if got := partitionKey(nil); got != "(unpartitioned)" {
		t.Errorf("nil tuple: got %q, want (unpartitioned)", got)
	}
	if got := partitionKey(map[string]string{}); got != "(unpartitioned)" {
		t.Errorf("empty tuple: got %q, want (unpartitioned)", got)
	}
}

func TestPartitionKeySingleField(t *testing.T) {
	got := partitionKey(map[string]string{"ss_store_sk": "5"})
	if got != "ss_store_sk=5" {
		t.Errorf("single field: got %q, want ss_store_sk=5", got)
	}
}

func TestPartitionKeyStableSortOrder(t *testing.T) {
	// Keys should sort by column name, not by insertion order or value.
	got := partitionKey(map[string]string{
		"zulu":  "A",
		"alpha": "B",
		"mike":  "C",
	})
	want := "alpha=B/mike=C/zulu=A"
	if got != want {
		t.Errorf("partitionKey = %q, want %q", got, want)
	}
}

func TestComputeTriggers_StaleRewriteBootstrap(t *testing.T) {
	// The regression test for the chicken-and-egg bug: a cold table
	// that has never been janitored should still fire stale-rewrite
	// once its commits age past the threshold. The old implementation
	// required a prior rewrite in LastRewriteAges to fire, which
	// meant a quiet small table could never trigger cold compaction.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      10,
		LatestCommitAt: now.Add(-48 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
		LastRewriteAges:      nil, // NEVER janitored
	}
	computeTriggers(ph, opts, now)
	if !ph.NeedsStaleRewrite {
		t.Error("bootstrap case: expected NeedsStaleRewrite=true on a 48h-old partition with no prior rewrite")
	}
}

func TestComputeTriggers_StaleRewriteWithinCooldown(t *testing.T) {
	// If the janitor rewrote this partition recently, don't fire
	// stale-rewrite again — we'd just re-do the work.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      10,
		LatestCommitAt: now.Add(-48 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
		LastRewriteAges: map[string]time.Duration{
			"region=us": 2 * time.Hour, // cooldown: janitored 2h ago
		},
	}
	computeTriggers(ph, opts, now)
	if ph.NeedsStaleRewrite {
		t.Error("cooldown case: expected NeedsStaleRewrite=false on recently-rewritten partition")
	}
}

func TestComputeTriggers_StaleRewriteExpiredCooldown(t *testing.T) {
	// Cooldown has expired — fire again.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      10,
		LatestCommitAt: now.Add(-48 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
		LastRewriteAges: map[string]time.Duration{
			"region=us": 36 * time.Hour, // cooldown expired
		},
	}
	computeTriggers(ph, opts, now)
	if !ph.NeedsStaleRewrite {
		t.Error("expired-cooldown case: expected NeedsStaleRewrite=true")
	}
}

func TestComputeTriggers_StaleRewriteBelowMinFiles(t *testing.T) {
	// A 1-file partition that's been quiet for a year should NOT
	// fire stale-rewrite — nothing to compact.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      1,
		LatestCommitAt: now.Add(-365 * 24 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
	}
	computeTriggers(ph, opts, now)
	if ph.NeedsStaleRewrite {
		t.Error("below-min-files case: expected NeedsStaleRewrite=false for 1-file partition")
	}
}

func TestComputeTriggers_StaleRewriteNotOldEnough(t *testing.T) {
	// Commit age is under the threshold — don't fire.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      10,
		LatestCommitAt: now.Add(-6 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
	}
	computeTriggers(ph, opts, now)
	if ph.NeedsStaleRewrite {
		t.Error("recent-commit case: expected NeedsStaleRewrite=false for 6h-old commit")
	}
}

func TestComputeTriggers_StaleRewriteNoCommitTime(t *testing.T) {
	// If LatestCommitAt is zero (v1 manifests without AddedSnapshotID,
	// or expired snapshots) the trigger must NOT fire — fail-safe.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey: "region=us",
		FileCount:    100,
		// LatestCommitAt is zero
	}
	opts := &PartitionHealthOptions{
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
	}
	computeTriggers(ph, opts, now)
	if ph.NeedsStaleRewrite {
		t.Error("no-commit-time case: expected NeedsStaleRewrite=false (fail-safe)")
	}
}

func TestComputeTriggers_SmallFileTriggerIndependent(t *testing.T) {
	// SmallFileCompact and StaleRewrite can fire independently.
	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	ph := &PartitionHealth{
		PartitionKey:   "region=us",
		FileCount:      100,
		SmallFileCount: 75,
		LatestCommitAt: now.Add(-48 * time.Hour),
	}
	opts := &PartitionHealthOptions{
		SmallFileTrigger:     50,
		FileCountTrigger:     200,
		StaleRewriteAge:      24 * time.Hour,
		MinStaleRewriteFiles: 2,
	}
	computeTriggers(ph, opts, now)
	if !ph.NeedsSmallFileCompact {
		t.Error("expected NeedsSmallFileCompact=true for 75 small files > 50 threshold")
	}
	if !ph.NeedsStaleRewrite {
		t.Error("expected NeedsStaleRewrite=true")
	}
	if ph.NeedsMetadataReduction {
		t.Error("100 files < 200 FileCountTrigger; NeedsMetadataReduction should be false")
	}
}

func TestAnalyzerOptionsDefaults(t *testing.T) {
	var o AnalyzerOptions
	o.defaults()
	if o.SmallFileThresholdBytes != 64*1024*1024 {
		t.Errorf("SmallFileThresholdBytes default = %d, want %d", o.SmallFileThresholdBytes, 64*1024*1024)
	}
	if o.MetadataRatioWarn != 0.05 {
		t.Errorf("MetadataRatioWarn default = %v, want 0.05", o.MetadataRatioWarn)
	}
	if o.MetadataRatioCritical != 0.10 {
		t.Errorf("MetadataRatioCritical default = %v, want 0.10", o.MetadataRatioCritical)
	}
	if o.SmallFileRatioWarn != 0.30 {
		t.Errorf("SmallFileRatioWarn default = %v, want 0.30", o.SmallFileRatioWarn)
	}
	if o.SmallFileRatioCritical != 0.60 {
		t.Errorf("SmallFileRatioCritical default = %v, want 0.60", o.SmallFileRatioCritical)
	}
}
