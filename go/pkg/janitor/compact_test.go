package janitor

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
)

// TestIsRetryableConcurrencyError pins the retry-condition predicate
// against both layers at which optimistic-concurrency conflicts can
// surface. This is the regression test for issue #1: the original
// implementation only matched catalog.ErrCASConflict and missed the
// iceberg-go "branch main has changed" error pattern, which caused
// almost every compaction to fail on the first try under realistic
// streaming load.
func TestIsRetryableConcurrencyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated error", errors.New("disk full"), false},
		{"random table error", errors.New("table not found"), false},

		// Layer 1: directory catalog's per-key CAS failure.
		{
			"ErrCASConflict",
			catalog.ErrCASConflict,
			true,
		},
		{
			"wrapped ErrCASConflict",
			fmt.Errorf("commit table: %w", catalog.ErrCASConflict),
			true,
		},

		// Layer 2: iceberg-go's Requirement.Validate failures. Six
		// variants exist in iceberg-go's table/requirements.go; the
		// matcher should catch all of them.
		{
			"branch main has changed (the bench-discovered case)",
			errors.New("requirement validation failed: requirement failed: branch main has changed: expected id 11720088221680, found 677484782707726476"),
			true,
		},
		{
			"current schema id has changed",
			errors.New("Requirement failed: current schema id has changed: expected 0, found 1"),
			true,
		},
		{
			"last assigned field id has changed",
			errors.New("Requirement failed: last assigned field id has changed: expected 23, found 24"),
			true,
		},
		{
			"default spec id has changed",
			errors.New("Requirement failed: default spec id has changed: expected 0, found 1"),
			true,
		},

		// Negative: similar-looking but NOT concurrency conflicts.
		{
			"table uuid mismatch (programming bug, not a race)",
			errors.New("Requirement failed: table uuid does not match"),
			false, // "does not match" not "has changed"
		},
		{
			"plain 'has changed' without requirement context",
			errors.New("file size has changed"),
			false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsRetryableConcurrencyError(c.err); got != c.want {
				t.Errorf("IsRetryableConcurrencyError(%q) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
