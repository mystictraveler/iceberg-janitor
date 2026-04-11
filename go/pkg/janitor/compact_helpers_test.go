package janitor

import (
	"testing"
	"time"
)

func TestCompactOptionsDefaults(t *testing.T) {
	var o CompactOptions
	o.defaults()
	if o.MaxAttempts != 15 {
		t.Errorf("MaxAttempts default = %d, want 15", o.MaxAttempts)
	}
	if o.InitialBackoff != 100*time.Millisecond {
		t.Errorf("InitialBackoff default = %v, want 100ms", o.InitialBackoff)
	}
	if o.MaxBackoff != 5*time.Second {
		t.Errorf("MaxBackoff default = %v, want 5s", o.MaxBackoff)
	}
}

func TestCompactOptionsDefaultsRespectsNonZero(t *testing.T) {
	o := CompactOptions{
		MaxAttempts:    99,
		InitialBackoff: 42 * time.Millisecond,
		MaxBackoff:     7 * time.Second,
	}
	o.defaults()
	if o.MaxAttempts != 99 {
		t.Errorf("MaxAttempts overwritten: got %d", o.MaxAttempts)
	}
	if o.InitialBackoff != 42*time.Millisecond {
		t.Errorf("InitialBackoff overwritten: got %v", o.InitialBackoff)
	}
	if o.MaxBackoff != 7*time.Second {
		t.Errorf("MaxBackoff overwritten: got %v", o.MaxBackoff)
	}
}

func TestPartitionToTuple(t *testing.T) {
	fieldIDToName := map[int]string{
		1: "ss_store_sk",
		2: "ss_sold_date_sk",
	}
	p := map[int]any{1: 5, 2: 2458850}
	got := partitionToTuple(p, fieldIDToName)
	if got["ss_store_sk"] != "5" {
		t.Errorf("ss_store_sk = %q, want 5", got["ss_store_sk"])
	}
	if got["ss_sold_date_sk"] != "2458850" {
		t.Errorf("ss_sold_date_sk = %q, want 2458850", got["ss_sold_date_sk"])
	}
}

func TestPartitionToTupleUnknownFieldID(t *testing.T) {
	// A field id not in the map should be silently dropped (rather
	// than panic or produce a key like "" — this matters because
	// old partition specs leave stale ids after partition-spec
	// evolution).
	fieldIDToName := map[int]string{1: "region"}
	p := map[int]any{1: "us", 999: "stale"}
	got := partitionToTuple(p, fieldIDToName)
	if len(got) != 1 {
		t.Errorf("got %d entries, want 1 (stale id dropped)", len(got))
	}
	if got["region"] != "us" {
		t.Errorf("region = %q, want us", got["region"])
	}
}

func TestPartitionToTupleEmpty(t *testing.T) {
	got := partitionToTuple(map[int]any{}, map[int]string{})
	if len(got) != 0 {
		t.Errorf("empty input: got %v", got)
	}
}

func TestCanonicalKeyDeterministic(t *testing.T) {
	// Same partition, different map insertion orders, must produce
	// the same canonicalKey — otherwise dedup in discoverSmallFilePartitions
	// breaks.
	a := canonicalKey(map[int]any{1: "us", 2: 2024})
	b := canonicalKey(map[int]any{2: 2024, 1: "us"})
	if a != b {
		t.Errorf("canonicalKey non-deterministic: %q vs %q", a, b)
	}
}

func TestCanonicalKeySortedByFieldID(t *testing.T) {
	// Sort order is by field ID ascending.
	got := canonicalKey(map[int]any{3: "c", 1: "a", 2: "b"})
	want := partKey("1=a|2=b|3=c")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCanonicalKeyEmpty(t *testing.T) {
	got := canonicalKey(map[int]any{})
	if got != "" {
		t.Errorf("empty partition: got %q, want empty", got)
	}
}

func TestCanonicalKeyDifferentValuesDifferentKeys(t *testing.T) {
	a := canonicalKey(map[int]any{1: "us"})
	b := canonicalKey(map[int]any{1: "eu"})
	if a == b {
		t.Errorf("different values should produce different keys: both %q", a)
	}
}
