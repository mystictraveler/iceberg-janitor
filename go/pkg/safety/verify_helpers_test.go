package safety

import (
	"sort"
	"testing"
)

// These tests cover the pure helpers inside verify.go that do not need
// a real iceberg-go Table fixture. End-to-end VerifyCompactionConsistency
// and VerifyExpireConsistency require the bench/integration harness.

func TestCompareColumnCountsAllEqual(t *testing.T) {
	in := map[int]int64{1: 100, 2: 200, 3: 0}
	out := map[int]int64{1: 100, 2: 200, 3: 0}
	got := compareColumnCounts(in, out)
	if got.Result != "pass" {
		t.Errorf("Result=%q, want pass", got.Result)
	}
	if got.Checked != 3 {
		t.Errorf("Checked=%d, want 3", got.Checked)
	}
	if got.Passed != 3 {
		t.Errorf("Passed=%d, want 3", got.Passed)
	}
	if len(got.FailedColumns) != 0 {
		t.Errorf("FailedColumns=%v, want empty", got.FailedColumns)
	}
}

func TestCompareColumnCountsOneMismatch(t *testing.T) {
	in := map[int]int64{1: 100, 2: 200}
	out := map[int]int64{1: 100, 2: 199}
	got := compareColumnCounts(in, out)
	if got.Result != "fail" {
		t.Errorf("Result=%q, want fail", got.Result)
	}
	if got.Passed != 1 {
		t.Errorf("Passed=%d, want 1", got.Passed)
	}
	if len(got.FailedColumns) != 1 || got.FailedColumns[0] != 2 {
		t.Errorf("FailedColumns=%v, want [2]", got.FailedColumns)
	}
	if got.Reason == "" {
		t.Errorf("expected non-empty Reason")
	}
}

func TestCompareColumnCountsMissingFromOut(t *testing.T) {
	// Column 3 was in `in` but not in `out` — must fail because we
	// compare strict equality on every column in the union of both maps.
	in := map[int]int64{1: 100, 2: 200, 3: 50}
	out := map[int]int64{1: 100, 2: 200}
	got := compareColumnCounts(in, out)
	if got.Result != "fail" {
		t.Errorf("Result=%q, want fail", got.Result)
	}
	if got.Checked != 3 {
		t.Errorf("Checked=%d, want 3", got.Checked)
	}
	// Column 3 should fail (in[3]=50, out[3]=0).
	found := false
	for _, c := range got.FailedColumns {
		if c == 3 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected col 3 in FailedColumns, got %v", got.FailedColumns)
	}
}

func TestCompareColumnCountsEmpty(t *testing.T) {
	got := compareColumnCounts(nil, nil)
	if got.Result != "pass" {
		t.Errorf("empty maps: Result=%q, want pass", got.Result)
	}
	if got.Checked != 0 {
		t.Errorf("Checked=%d, want 0", got.Checked)
	}
}

func TestCompareBoundsPresenceAllPresent(t *testing.T) {
	in := map[int]bool{1: true, 2: true, 3: true}
	out := map[int]bool{1: true, 2: true, 3: true}
	got := compareBoundsPresence(in, out)
	if got.Result != "pass" {
		t.Errorf("Result=%q, want pass", got.Result)
	}
	if got.InColumns != 3 || got.OutColumns != 3 {
		t.Errorf("InColumns=%d OutColumns=%d, want 3/3", got.InColumns, got.OutColumns)
	}
}

func TestCompareBoundsPresenceMissingInOut(t *testing.T) {
	in := map[int]bool{1: true, 2: true, 3: true}
	out := map[int]bool{1: true, 2: true}
	got := compareBoundsPresence(in, out)
	if got.Result != "fail" {
		t.Errorf("Result=%q, want fail", got.Result)
	}
	if len(got.MissingInOut) != 1 || got.MissingInOut[0] != 3 {
		t.Errorf("MissingInOut=%v, want [3]", got.MissingInOut)
	}
}

func TestCompareBoundsPresenceSpurious(t *testing.T) {
	// Staged output has bounds for a column the input didn't.
	in := map[int]bool{1: true}
	out := map[int]bool{1: true, 2: true}
	got := compareBoundsPresence(in, out)
	if got.Result != "fail" {
		t.Errorf("Result=%q, want fail", got.Result)
	}
	if len(got.SpuriousInOut) != 1 || got.SpuriousInOut[0] != 2 {
		t.Errorf("SpuriousInOut=%v, want [2]", got.SpuriousInOut)
	}
}

func TestCompareBoundsPresenceEmpty(t *testing.T) {
	got := compareBoundsPresence(nil, nil)
	if got.Result != "pass" {
		t.Errorf("empty maps: Result=%q, want pass", got.Result)
	}
}

func TestUnionKeysDisjoint(t *testing.T) {
	a := map[int]int64{1: 10, 2: 20}
	b := map[int]int64{3: 30, 4: 40}
	got := unionKeys(a, b)
	sort.Ints(got)
	want := []int{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("got[%d]=%d, want %d", i, got[i], k)
		}
	}
}

func TestUnionKeysOverlap(t *testing.T) {
	a := map[int]int64{1: 10, 2: 20}
	b := map[int]int64{2: 25, 3: 30}
	got := unionKeys(a, b)
	sort.Ints(got)
	want := []int{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, k := range want {
		if got[i] != k {
			t.Errorf("got[%d]=%d, want %d", i, got[i], k)
		}
	}
}

func TestUnionKeysEmpty(t *testing.T) {
	got := unionKeys(nil, nil)
	if len(got) != 0 {
		t.Errorf("empty: got %v, want []", got)
	}
}
