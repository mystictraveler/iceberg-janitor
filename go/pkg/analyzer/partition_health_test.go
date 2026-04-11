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
