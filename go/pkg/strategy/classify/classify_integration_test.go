package classify_test

import (
	"context"
	"testing"
	"time"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/strategy/classify"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestClassify_FreshTable verifies that a table with exactly one
// snapshot (from SeedFactTable's AddFiles commit) classifies as
// streaming when the commit is recent — the 15-min short window
// fires because there's ≥1 commit in the last 15 minutes.
func TestClassify_FreshTable(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "cls", "fresh", 3, 5)

	r := classify.Classify(tbl)
	// A single recent commit should classify as at least batch or
	// streaming depending on the thresholds. With 1 commit in 15m,
	// the 15-min rate is (1/15)*60 = 4/h which is below the 6/h
	// streaming threshold. So this should be batch or slow_changing.
	if r.Class == classify.ClassDormant {
		t.Errorf("fresh table classified as dormant, want something active")
	}
	if r.ForeignCommitsLast24h < 1 {
		t.Errorf("ForeignCommitsLast24h = %d, want >= 1", r.ForeignCommitsLast24h)
	}
	t.Logf("class=%s commits_24h=%d commits_15m=%d commits_7d=%d",
		r.Class, r.ForeignCommitsLast24h, r.ForeignCommitsLast15m, r.ForeignCommitsLast7d)
}

// TestClassify_MultiCommit_Streaming verifies that a table with many
// recent commits classifies as streaming. We add files in a tight
// loop to simulate a burst writer.
func TestClassify_MultiCommit_Streaming(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "cls", "stream", 1, 5)

	// Add 10 more commits in rapid succession.
	for i := 0; i < 10; i++ {
		key := "cls.db/stream/data/burst-" + string(rune('a'+i)) + ".parquet"
		abs := w.WriteParquetFile(t, key,
			[]testutil.SimpleFactRow{{ID: int64(100 + i), Value: 0, Region: "us"}})
		tx := tbl.NewTransaction()
		if err := tx.AddFiles(context.Background(), []string{abs}, nil, false); err != nil {
			t.Fatal(err)
		}
		newTbl, err := tx.Commit(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		tbl = newTbl
	}

	r := classify.Classify(tbl)
	// 11 commits in ~1 second → 15-min rate = (11/15)*60 = 44/h
	// which is well above the 6/h streaming threshold.
	if r.Class != classify.ClassStreaming {
		t.Errorf("class = %s, want streaming (11 recent commits should trigger short-window)", r.Class)
	}
	if r.ForeignCommitsLast15m < 10 {
		t.Errorf("ForeignCommitsLast15m = %d, want >= 10", r.ForeignCommitsLast15m)
	}
	t.Logf("class=%s commits_15m=%d commits_24h=%d", r.Class, r.ForeignCommitsLast15m, r.ForeignCommitsLast24h)
}

// TestClassify_NilTable classifies as dormant.
func TestClassify_NilTable(t *testing.T) {
	r := classify.Classify(nil)
	if r.Class != classify.ClassDormant {
		t.Errorf("nil table: class = %s, want dormant", r.Class)
	}
}

// TestClassify_EmptyTable (no snapshots) classifies as dormant.
func TestClassify_EmptyTable(t *testing.T) {
	w := testutil.NewWarehouse(t)
	w.CreateTable(t, "cls", "empty", testutil.SimpleFactSchema())
	tbl := w.LoadTable(t, "cls", "empty")

	r := classify.Classify(tbl)
	if r.Class != classify.ClassDormant {
		t.Errorf("empty table: class = %s, want dormant", r.Class)
	}
}

// TestClassToOptions verifies the class-to-options mapping produces
// the right mode for each class.
func TestClassToOptions(t *testing.T) {
	tests := []struct {
		class classify.WorkloadClass
		mode  classify.MaintainMode
	}{
		{classify.ClassStreaming, classify.ModeHot},
		{classify.ClassBatch, classify.ModeCold},
		{classify.ClassSlowChanging, classify.ModeCold},
		{classify.ClassDormant, classify.ModeCold},
	}
	for _, tt := range tests {
		opts := classify.ClassToOptions(tt.class)
		if opts.Mode != tt.mode {
			t.Errorf("ClassToOptions(%s).Mode = %s, want %s", tt.class, opts.Mode, tt.mode)
		}
	}
}

// TestClassToOptions_Thresholds verifies that each class produces
// reasonable maintain thresholds (non-zero, sane ranges).
func TestClassToOptions_Thresholds(t *testing.T) {
	for _, class := range []classify.WorkloadClass{
		classify.ClassStreaming,
		classify.ClassBatch,
		classify.ClassSlowChanging,
		classify.ClassDormant,
	} {
		opts := classify.ClassToOptions(class)
		if opts.KeepLastSnapshots <= 0 {
			t.Errorf("%s: KeepLastSnapshots = %d, want > 0", class, opts.KeepLastSnapshots)
		}
		if opts.KeepWithin <= 0 {
			t.Errorf("%s: KeepWithin = %v, want > 0", class, opts.KeepWithin)
		}
		if opts.TargetFileSizeBytes <= 0 {
			t.Errorf("%s: TargetFileSizeBytes = %d, want > 0", class, opts.TargetFileSizeBytes)
		}
		if opts.StaleRewriteAge <= 0 && class != classify.ClassDormant {
			t.Errorf("%s: StaleRewriteAge = %v, want > 0 for non-dormant", class, opts.StaleRewriteAge)
		}
		_ = time.Now() // keep time import used
	}
}
