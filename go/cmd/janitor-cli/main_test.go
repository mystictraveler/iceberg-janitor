package main

import (
	"strings"
	"testing"
)

// TestParseFileSize covers the human-readable size parser used by
// `compact --target-file-size`. The parser is the only place in the
// CLI where Pattern B's threshold gets bound to operator intent, so
// regressions here would silently break the bench's invocation
// (TARGET_FILE_SIZE=1MB).
func TestParseFileSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"1024", 1024, false},
		{"1KB", 1024, false},
		{"1kb", 1024, false},
		{" 1MB ", 1024 * 1024, false},
		{"512MB", 512 * 1024 * 1024, false},
		{"2GB", 2 * 1024 * 1024 * 1024, false},
		{"1mb", 1024 * 1024, false},

		{"", 0, true},
		{"0", 0, true},
		{"-1", 0, true},
		{"abc", 0, true},
		{"10TB", 0, true}, // unsupported suffix
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseFileSize(tc.in)
			if tc.err {
				if err == nil {
					t.Errorf("parseFileSize(%q): expected error, got %d", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseFileSize(%q): unexpected error: %v", tc.in, err)
				return
			}
			if got != tc.want {
				t.Errorf("parseFileSize(%q): got %d want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestParsePartitionFilter verifies that --partition col=value is
// parsed into the type-general PartitionTuple shape that pkg/janitor
// expects. The CLI must not type-narrow values; it forwards raw
// strings and lets compactOnce parse against the table's schema at
// runtime. (See feedback on type-narrow assumptions in
// pkg/janitor/compact_partition_types.go.)
func TestParsePartitionFilter(t *testing.T) {
	cases := []struct {
		in     string
		col    string
		val    string
		errSub string
	}{
		{"ss_store_sk=5", "ss_store_sk", "5", ""},
		{"event_date=2026-04-08", "event_date", "2026-04-08", ""},
		{"region=us-east-1", "region", "us-east-1", ""},
		{"flag=true", "flag", "true", ""},
		// Quoted strings are not interpreted; the value is taken
		// verbatim modulo whitespace trimming around the equals sign.
		{"name=Alice Smith", "name", "Alice Smith", ""},

		{"no-equals", "", "", "expected col=value"},
		{"=value", "", "", "expected col=value"},
		{"key=", "", "", "expected col=value"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			got, err := parsePartitionFilter(tc.in)
			if tc.errSub != "" {
				if err == nil {
					t.Errorf("parsePartitionFilter(%q): expected error, got %v", tc.in, got)
					return
				}
				if !strings.Contains(err.Error(), tc.errSub) {
					t.Errorf("parsePartitionFilter(%q): error %q does not contain %q", tc.in, err.Error(), tc.errSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePartitionFilter(%q): unexpected error: %v", tc.in, err)
			}
			if len(got) != 1 {
				t.Fatalf("parsePartitionFilter(%q): want 1 entry, got %d (%v)", tc.in, len(got), got)
			}
			if v, ok := got[tc.col]; !ok || v != tc.val {
				t.Errorf("parsePartitionFilter(%q): want %s=%q, got %v", tc.in, tc.col, tc.val, got)
			}
		})
	}
}

// TestHumanBytes spot-checks the formatter used in compact / expire /
// rewrite-manifests output so a refactor doesn't accidentally swap
// powers of 1000 and 1024.
func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{2 * 1024 * 1024, "2.0 MiB"},
		{3 * 1024 * 1024 * 1024, "3.0 GiB"},
	}
	for _, tc := range cases {
		got := humanBytes(tc.in)
		if got != tc.want {
			t.Errorf("humanBytes(%d): got %q want %q", tc.in, got, tc.want)
		}
	}
}
