package maintenance

import (
	"bytes"
	"sort"
	"testing"
)

func TestSetDiffDisjoint(t *testing.T) {
	a := map[string]struct{}{"x": {}, "y": {}}
	b := map[string]struct{}{"p": {}, "q": {}}
	missing, extra := setDiff(a, b)
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) != 2 || missing[0] != "x" || missing[1] != "y" {
		t.Errorf("missing = %v, want [x y]", missing)
	}
	if len(extra) != 2 || extra[0] != "p" || extra[1] != "q" {
		t.Errorf("extra = %v, want [p q]", extra)
	}
}

func TestSetDiffOverlap(t *testing.T) {
	a := map[string]struct{}{"x": {}, "y": {}, "z": {}}
	b := map[string]struct{}{"y": {}, "z": {}, "w": {}}
	missing, extra := setDiff(a, b)
	if len(missing) != 1 || missing[0] != "x" {
		t.Errorf("missing = %v, want [x]", missing)
	}
	if len(extra) != 1 || extra[0] != "w" {
		t.Errorf("extra = %v, want [w]", extra)
	}
}

func TestSetDiffIdentical(t *testing.T) {
	a := map[string]struct{}{"x": {}, "y": {}}
	b := map[string]struct{}{"x": {}, "y": {}}
	missing, extra := setDiff(a, b)
	if len(missing) != 0 {
		t.Errorf("missing = %v, want []", missing)
	}
	if len(extra) != 0 {
		t.Errorf("extra = %v, want []", extra)
	}
}

func TestSetDiffEmpty(t *testing.T) {
	missing, extra := setDiff(nil, nil)
	if len(missing) != 0 || len(extra) != 0 {
		t.Errorf("nil/nil: missing=%v extra=%v", missing, extra)
	}
}

func TestFirstOrEmpty(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want string
	}{
		{"nil", nil, ""},
		{"empty", []string{}, ""},
		{"single", []string{"alpha"}, "alpha"},
		{"multi", []string{"first", "second"}, "first"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := firstOrEmpty(tc.in); got != tc.want {
				t.Errorf("firstOrEmpty(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestHexEncodeID(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0000000000000000"},
		{1, "0000000000000001"},
		{255, "00000000000000ff"},
		{-1, "ffffffffffffffff"},
		{int64(0x7FFFFFFFFFFFFFFF), "7fffffffffffffff"},
	}
	for _, tc := range cases {
		got := hexEncodeID(tc.in)
		if got != tc.want {
			t.Errorf("hexEncodeID(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWriteByteCounter(t *testing.T) {
	var buf bytes.Buffer
	c := &writeByteCounter{w: &buf}

	n, err := c.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 5 {
		t.Errorf("first Write n = %d, want 5", n)
	}
	if c.n != 5 {
		t.Errorf("accumulator after first Write = %d, want 5", c.n)
	}

	n, err = c.Write([]byte(" world"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 6 {
		t.Errorf("second Write n = %d, want 6", n)
	}
	if c.n != 11 {
		t.Errorf("accumulator after second Write = %d, want 11", c.n)
	}

	if buf.String() != "hello world" {
		t.Errorf("buffer = %q, want 'hello world'", buf.String())
	}
}
