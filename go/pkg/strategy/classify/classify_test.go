package classify

import (
	"testing"
	"time"
)

func TestClassifyFromCountsShortWindowStreaming(t *testing.T) {
	// Fresh table: 40 commits in last 15 min, only 40 in last 24h
	// (the bench run scenario). 24h rate = 1.67/h (below streaming
	// threshold), but 15m rate = 160/h (well above). Short-window
	// fast path should pick streaming.
	r := Result{
		ForeignCommitsLast15m: 40,
		ForeignCommitsLast24h: 40,
		ForeignCommitsLast7d:  40,
	}
	got := classifyFromCounts(r, time.Now())
	if got != ClassStreaming {
		t.Errorf("short-window burst: got %q, want streaming", got)
	}
}

func TestClassifyFromCountsShortWindowMinimum(t *testing.T) {
	// Single recent commit should NOT trigger streaming (false-positive
	// guard). 1 commit / 15 min = 4/h, below the 6/h threshold even
	// before the 2-commit floor kicks in.
	r := Result{
		ForeignCommitsLast15m: 1,
		ForeignCommitsLast24h: 1,
		ForeignCommitsLast7d:  1,
	}
	got := classifyFromCounts(r, time.Now())
	if got == ClassStreaming {
		t.Errorf("single recent commit: got streaming, want batch/slow_changing")
	}
}

func TestClassifyFromCountsShortWindowDoesNotOverrideDormant(t *testing.T) {
	// Dormant check runs before the short-window path. A table with
	// zero 7d commits must stay dormant even if ForeignCommitsLast15m
	// is somehow non-zero (shouldn't happen, but defensive).
	r := Result{
		ForeignCommitsLast15m: 0,
		ForeignCommitsLast24h: 0,
		ForeignCommitsLast7d:  0,
	}
	got := classifyFromCounts(r, time.Now())
	if got != ClassDormant {
		t.Errorf("zero 7d: got %q, want dormant", got)
	}
}

func TestClassifyFromCountsDormant(t *testing.T) {
	r := Result{ForeignCommitsLast24h: 0, ForeignCommitsLast7d: 0}
	got := classifyFromCounts(r, time.Now())
	if got != ClassDormant {
		t.Errorf("zero commits in 7d: got %q, want %q", got, ClassDormant)
	}
}

func TestClassifyFromCountsStreaming(t *testing.T) {
	// > 6 commits/hour over 24h → 144+ commits in 24h
	r := Result{ForeignCommitsLast24h: 200, ForeignCommitsLast7d: 1000}
	got := classifyFromCounts(r, time.Now())
	if got != ClassStreaming {
		t.Errorf("200 commits in 24h: got %q, want %q", got, ClassStreaming)
	}
}

func TestClassifyFromCountsStreamingBoundary(t *testing.T) {
	// 145 commits / 24h = 6.04/hr → streaming
	r := Result{ForeignCommitsLast24h: 145, ForeignCommitsLast7d: 500}
	got := classifyFromCounts(r, time.Now())
	if got != ClassStreaming {
		t.Errorf("just above 6/h: got %q, want %q", got, ClassStreaming)
	}
}

func TestClassifyFromCountsBatch(t *testing.T) {
	// 2 commits/hour × 24 = 48 commits/24h → batch
	r := Result{ForeignCommitsLast24h: 48, ForeignCommitsLast7d: 300}
	got := classifyFromCounts(r, time.Now())
	if got != ClassBatch {
		t.Errorf("48 commits in 24h: got %q, want %q", got, ClassBatch)
	}
}

func TestClassifyFromCountsSlowChanging(t *testing.T) {
	// 0.5 commits in 24h = 0.0208/hr → below batch (> 0.04/hr), so slow_changing
	r := Result{ForeignCommitsLast24h: 0, ForeignCommitsLast7d: 3}
	got := classifyFromCounts(r, time.Now())
	if got != ClassSlowChanging {
		t.Errorf("0.5 commits in 24h: got %q, want %q", got, ClassSlowChanging)
	}
}

func TestClassifyFromCountsBatchBoundary(t *testing.T) {
	// 1 commit/hour × 24 = 24 commits/24h → batch
	r := Result{ForeignCommitsLast24h: 24, ForeignCommitsLast7d: 100}
	got := classifyFromCounts(r, time.Now())
	if got != ClassBatch {
		t.Errorf("24 commits in 24h: got %q, want %q", got, ClassBatch)
	}
}

func TestDefaultsForKnownClasses(t *testing.T) {
	classes := []WorkloadClass{ClassStreaming, ClassBatch, ClassSlowChanging, ClassDormant}
	for _, c := range classes {
		t.Run(string(c), func(t *testing.T) {
			got := DefaultsFor(c)
			want := ClassDefaults[c]
			// Spot-check one field to confirm we got the right entry.
			if got.SmallFileTrigger != want.SmallFileTrigger {
				t.Errorf("DefaultsFor(%s).SmallFileTrigger = %d, want %d", c, got.SmallFileTrigger, want.SmallFileTrigger)
			}
		})
	}
}

func TestDefaultsForUnknownClass(t *testing.T) {
	got := DefaultsFor(WorkloadClass("nonexistent"))
	want := ClassDefaults[ClassDormant]
	if got.SmallFileTrigger != want.SmallFileTrigger {
		t.Errorf("unknown class should return dormant defaults: got %+v, want %+v", got, want)
	}
}

func TestClassDefaultsMonotonic(t *testing.T) {
	// Streaming should have the tightest thresholds (smallest trigger
	// counts) and dormant should have the loosest. This enforces the
	// design intent across future edits.
	s := ClassDefaults[ClassStreaming]
	b := ClassDefaults[ClassBatch]
	d := ClassDefaults[ClassDormant]
	if s.SmallFileTrigger >= b.SmallFileTrigger {
		t.Errorf("streaming trigger (%d) should be < batch trigger (%d)", s.SmallFileTrigger, b.SmallFileTrigger)
	}
	if b.SmallFileTrigger > d.SmallFileTrigger {
		t.Errorf("batch trigger (%d) should be <= dormant trigger (%d)", b.SmallFileTrigger, d.SmallFileTrigger)
	}
}
