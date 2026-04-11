package main

import "testing"

func TestSplitWarehousePrefix(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantBucket string
		wantPrefix string
	}{
		{"no_path", "s3://my-bucket", "s3://my-bucket", ""},
		{"simple_prefix", "s3://my-bucket/warehouse", "s3://my-bucket", "warehouse/"},
		{"nested_prefix", "s3://my-bucket/team/warehouse", "s3://my-bucket", "team/warehouse/"},
		{"trailing_slash", "s3://my-bucket/warehouse/", "s3://my-bucket", "warehouse/"},
		{"file_scheme_passthrough", "file:///tmp/warehouse", "file:///tmp/warehouse", ""},
		{"not_a_url", "not-a-url", "not-a-url", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, prefix := splitWarehousePrefix(tc.in)
			if bucket != tc.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tc.wantBucket)
			}
			if prefix != tc.wantPrefix {
				t.Errorf("prefix = %q, want %q", prefix, tc.wantPrefix)
			}
		})
	}
}
