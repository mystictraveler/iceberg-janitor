package catalog

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

// TestDiscoverTables exercises the v0 discovery primitive against an
// in-process fileblob fixture. No Docker, no network.
func TestDiscoverTables(t *testing.T) {
	dir := t.TempDir()

	// Create a fake warehouse layout:
	//   db1/events/metadata/v1.metadata.json
	//   db1/events/metadata/v2.metadata.json
	//   db1/events/metadata/v3.metadata.json   (current)
	//   db1/events/data/00000-abc.parquet
	//   db1/users/metadata/v1.metadata.json    (current)
	//   db2/orders/metadata/00007-deadbeef.metadata.json   (current; Iceberg java style)
	//   db2/empty_dir/                         (no metadata, must NOT appear)
	//   _janitor/state/foo.json                (must NOT appear)
	files := []string{
		"db1/events/metadata/v1.metadata.json",
		"db1/events/metadata/v2.metadata.json",
		"db1/events/metadata/v3.metadata.json",
		"db1/events/data/00000-abc.parquet",
		"db1/users/metadata/v1.metadata.json",
		"db2/orders/metadata/00007-deadbeef.metadata.json",
		"db2/empty_dir/.gitkeep",
		"_janitor/state/foo.json",
	}
	for _, f := range files {
		writeFixture(t, dir, f, []byte("{}"))
	}

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "file://"+dir)
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	defer bucket.Close()

	tables, err := DiscoverTables(ctx, bucket, "")
	if err != nil {
		t.Fatalf("DiscoverTables: %v", err)
	}

	// Sort by Prefix for stable comparison.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Prefix < tables[j].Prefix
	})

	want := []TableLocation{
		{Prefix: "db1/events", CurrentVersion: 3, CurrentMetadata: "db1/events/metadata/v3.metadata.json"},
		{Prefix: "db1/users", CurrentVersion: 1, CurrentMetadata: "db1/users/metadata/v1.metadata.json"},
		{Prefix: "db2/orders", CurrentVersion: 7, CurrentMetadata: "db2/orders/metadata/00007-deadbeef.metadata.json"},
	}
	if len(tables) != len(want) {
		t.Fatalf("got %d tables, want %d: %+v", len(tables), len(want), tables)
	}
	for i, w := range want {
		if tables[i] != w {
			t.Errorf("table %d: got %+v, want %+v", i, tables[i], w)
		}
	}
}

func TestParseMetadataVersion(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
		ok   bool
	}{
		{"v form", "v42.metadata.json", 42, true},
		{"v form padded", "v0007.metadata.json", 7, true},
		{"java form", "00007-deadbeef-1234.metadata.json", 7, true},
		{"bare int form", "42.metadata.json", 42, true},
		{"not metadata", "v42.json", 0, false},
		{"no version", "metadata.json", 0, false},
		{"data file", "00000-abc.parquet", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseMetadataVersion(c.in)
			if ok != c.ok || got != c.want {
				t.Errorf("parseMetadataVersion(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.ok)
			}
		})
	}
}

func writeFixture(t *testing.T, root, relPath string, content []byte) {
	t.Helper()
	full := filepath.Join(root, relPath)
	if err := mkdirAll(filepath.Dir(full)); err != nil {
		t.Fatalf("mkdir %q: %v", full, err)
	}
	if err := writeFile(full, content); err != nil {
		t.Fatalf("write %q: %v", full, err)
	}
}
