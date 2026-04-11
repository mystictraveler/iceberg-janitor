package catalog

import (
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"
)

func TestIdentToPrefix(t *testing.T) {
	cases := []struct {
		name string
		in   icebergtable.Identifier
		want string
	}{
		{"two_parts", icebergtable.Identifier{"mvp", "events"}, "mvp.db/events"},
		{"three_parts", icebergtable.Identifier{"tpcds", "store_sales", "region"}, "tpcds.db/store_sales/region"},
		{"one_part", icebergtable.Identifier{"orphan"}, "orphan"},
		{"empty", icebergtable.Identifier{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := identToPrefix(tc.in)
			if got != tc.want {
				t.Errorf("identToPrefix(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestAbsoluteURLFile(t *testing.T) {
	cases := []struct {
		name, wh, key, want string
	}{
		{"simple", "file:///tmp/wh", "mvp.db/events/metadata/v1.metadata.json", "file:///tmp/wh/mvp.db/events/metadata/v1.metadata.json"},
		{"trailing_slash", "file:///tmp/wh/", "a/b", "file:///tmp/wh/a/b"},
		{"with_query", "file:///tmp/wh?region=us-east-1", "key", "file:///tmp/wh/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AbsoluteURL(tc.wh, tc.key)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("AbsoluteURL(%q, %q) = %q, want %q", tc.wh, tc.key, got, tc.want)
			}
		})
	}
}

func TestAbsoluteURLS3(t *testing.T) {
	cases := []struct {
		name, wh, key, want string
	}{
		{"simple", "s3://my-bucket", "tpcds.db/store_sales/metadata/v1.metadata.json", "s3://my-bucket/tpcds.db/store_sales/metadata/v1.metadata.json"},
		{"with_region", "s3://my-bucket?region=us-east-1", "key", "s3://my-bucket/key"},
		{"trailing_slash", "s3://my-bucket/", "key", "s3://my-bucket/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := AbsoluteURL(tc.wh, tc.key)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("AbsoluteURL(%q, %q) = %q, want %q", tc.wh, tc.key, got, tc.want)
			}
		})
	}
}

func TestAbsoluteURLUnsupportedScheme(t *testing.T) {
	_, err := AbsoluteURL("http://bucket/", "key")
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestBucketRelativeKeyFile(t *testing.T) {
	got, err := bucketRelativeKey("file:///tmp/wh", "file:///tmp/wh/mvp.db/events/metadata/v1.metadata.json")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := "mvp.db/events/metadata/v1.metadata.json"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBucketRelativeKeyFileWithQuery(t *testing.T) {
	got, err := bucketRelativeKey("file:///tmp/wh?region=us-east-1", "file:///tmp/wh/a/b")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "a/b" {
		t.Errorf("got %q, want a/b", got)
	}
}

func TestBucketRelativeKeyFileNotUnderWarehouse(t *testing.T) {
	_, err := bucketRelativeKey("file:///tmp/wh", "file:///other/location/file.json")
	if err == nil {
		t.Fatal("expected error for absLoc not under warehouse")
	}
}

func TestBucketRelativeKeyS3(t *testing.T) {
	got, err := bucketRelativeKey("s3://my-bucket?region=us-east-1", "s3://my-bucket/tpcds.db/store_sales/metadata/v42.metadata.json")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := "tpcds.db/store_sales/metadata/v42.metadata.json"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBucketRelativeKeyS3WrongBucket(t *testing.T) {
	_, err := bucketRelativeKey("s3://bucket-a", "s3://bucket-b/key")
	if err == nil {
		t.Fatal("expected error for cross-bucket location")
	}
}

func TestBucketRelativeKeyUnsupportedScheme(t *testing.T) {
	_, err := bucketRelativeKey("http://bucket", "http://bucket/key")
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
}

func TestNextMetadataVersion(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"file:///tmp/wh/mvp.db/events/metadata/v1.metadata.json", 1},
		{"file:///tmp/wh/mvp.db/events/metadata/v42.metadata.json", 42},
		{"s3://bucket/tpcds.db/store_sales/metadata/v1234.metadata.json", 1234},
		{"no-slashes-here", 0},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := nextMetadataVersion(tc.in)
			if got != tc.want {
				t.Errorf("nextMetadataVersion(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
