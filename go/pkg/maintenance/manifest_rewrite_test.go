package maintenance

import (
	"testing"
	"time"
)

func TestRewriteManifestsOptionsDefaults(t *testing.T) {
	in := RewriteManifestsOptions{}
	in.defaults()
	if in.MinManifestsToTrigger != 4 {
		t.Errorf("MinManifestsToTrigger: got %d want 4", in.MinManifestsToTrigger)
	}
	if in.MaxAttempts != 15 {
		t.Errorf("MaxAttempts: got %d want 15", in.MaxAttempts)
	}
	if in.InitialBackoff != 100*time.Millisecond {
		t.Errorf("InitialBackoff: got %v want 100ms", in.InitialBackoff)
	}

	override := RewriteManifestsOptions{MinManifestsToTrigger: 16, MaxAttempts: 3}
	override.defaults()
	if override.MinManifestsToTrigger != 16 {
		t.Errorf("MinManifestsToTrigger override lost: got %d want 16", override.MinManifestsToTrigger)
	}
	if override.MaxAttempts != 3 {
		t.Errorf("MaxAttempts override lost: got %d want 3", override.MaxAttempts)
	}
	if override.InitialBackoff != 100*time.Millisecond {
		t.Errorf("InitialBackoff partial: got %v want 100ms", override.InitialBackoff)
	}
}

// TestCanonicalPartitionKey verifies that the partition-tuple key
// function is order-independent (Go map iteration is randomized) and
// distinguishes distinct tuples. This is the load-bearing property
// for the manifest rewrite grouping: if the key is order-dependent,
// the same tuple could land in two different groups across runs.
func TestCanonicalPartitionKey(t *testing.T) {
	// Two maps with the same field/value pairs constructed in
	// different orders must produce the same key. We can't force a
	// particular iteration order in Go, but we can repeat the test
	// many times against fresh maps to make a transposition extremely
	// unlikely to slip through.
	a := map[int]any{1: int64(5), 2: "us", 3: int32(2026)}
	b := map[int]any{3: int32(2026), 1: int64(5), 2: "us"}
	keyA := canonicalPartitionKey(a)
	for i := 0; i < 100; i++ {
		if got := canonicalPartitionKey(b); got != keyA {
			t.Fatalf("partition key not stable: %q vs %q", keyA, got)
		}
	}

	// Distinct tuples must produce distinct keys.
	c := map[int]any{1: int64(5), 2: "ca", 3: int32(2026)}
	if canonicalPartitionKey(c) == keyA {
		t.Errorf("canonicalPartitionKey collapsed distinct tuples: %v vs %v", a, c)
	}

	// Empty tuple is its own key (unpartitioned table).
	if canonicalPartitionKey(nil) != "()" {
		t.Errorf("empty tuple: got %q want %q", canonicalPartitionKey(nil), "()")
	}
}

func TestFilePathSetsEqual(t *testing.T) {
	mk := func(paths ...string) map[string]struct{} {
		m := map[string]struct{}{}
		for _, p := range paths {
			m[p] = struct{}{}
		}
		return m
	}
	cases := []struct {
		name string
		a, b map[string]struct{}
		want bool
	}{
		{"both empty", mk(), mk(), true},
		{"identical", mk("a", "b"), mk("a", "b"), true},
		{"different size", mk("a"), mk("a", "b"), false},
		{"same size different elements", mk("a", "c"), mk("a", "b"), false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := filePathSetsEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("filePathSetsEqual(%v, %v): got %v want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestNewSnapshotIDPositive(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := newSnapshotID()
		if id <= 0 {
			t.Fatalf("newSnapshotID returned non-positive: %d", id)
		}
	}
}
