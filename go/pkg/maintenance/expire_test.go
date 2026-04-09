package maintenance

import (
	"testing"
	"time"
)

// TestExpireOptionsDefaults verifies that the option-defaulting helper
// fills in the four fields the rest of the package depends on. The
// other expire behavior (CAS retry, master check, transaction stage)
// is exercised end-to-end by the bench rather than by a unit test;
// constructing a multi-snapshot iceberg table from scratch in a unit
// test would duplicate fixtures the bench harness already maintains.
func TestExpireOptionsDefaults(t *testing.T) {
	cases := []struct {
		name string
		in   ExpireOptions
		want ExpireOptions
	}{
		{
			name: "all zero -> defaults",
			in:   ExpireOptions{},
			want: ExpireOptions{
				KeepLast:       5,
				KeepWithin:     7 * 24 * time.Hour,
				MaxAttempts:    15,
				InitialBackoff: 100 * time.Millisecond,
			},
		},
		{
			name: "operator overrides preserved",
			in: ExpireOptions{
				KeepLast:       3,
				KeepWithin:     time.Hour,
				MaxAttempts:    7,
				InitialBackoff: 50 * time.Millisecond,
			},
			want: ExpireOptions{
				KeepLast:       3,
				KeepWithin:     time.Hour,
				MaxAttempts:    7,
				InitialBackoff: 50 * time.Millisecond,
			},
		},
		{
			name: "partial override gets remaining defaults",
			in:   ExpireOptions{KeepLast: 10},
			want: ExpireOptions{
				KeepLast:       10,
				KeepWithin:     7 * 24 * time.Hour,
				MaxAttempts:    15,
				InitialBackoff: 100 * time.Millisecond,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in
			got.defaults()
			if got.KeepLast != tc.want.KeepLast {
				t.Errorf("KeepLast: got %d want %d", got.KeepLast, tc.want.KeepLast)
			}
			if got.KeepWithin != tc.want.KeepWithin {
				t.Errorf("KeepWithin: got %v want %v", got.KeepWithin, tc.want.KeepWithin)
			}
			if got.MaxAttempts != tc.want.MaxAttempts {
				t.Errorf("MaxAttempts: got %d want %d", got.MaxAttempts, tc.want.MaxAttempts)
			}
			if got.InitialBackoff != tc.want.InitialBackoff {
				t.Errorf("InitialBackoff: got %v want %v", got.InitialBackoff, tc.want.InitialBackoff)
			}
		})
	}
}
