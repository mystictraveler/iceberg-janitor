package janitor_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// BenchmarkCompact_SmallFiles drives janitor.Compact against a table
// seeded with N small parquet files and measures wall time, CPU,
// and allocations. This is the workload that matters most for real
// streaming benches: many small files per commit, compact merges
// them into fewer target-sized files.
//
// Run:
//
//	go test -bench BenchmarkCompact -cpuprofile /tmp/cpu.out -memprofile /tmp/mem.out ./pkg/janitor/
//	go tool pprof -top /tmp/cpu.out
//	go tool pprof -top /tmp/mem.out
//
// The benchmark re-seeds the table inside b.StopTimer so the setup
// doesn't contaminate the timed region. Only the janitor.Compact
// call is measured.
func BenchmarkCompact_SmallFiles(b *testing.B) {
	cases := []struct {
		name        string
		numFiles    int
		rowsPerFile int
	}{
		{"50f_10r", 50, 10},
		{"200f_10r", 200, 10},
		{"500f_10r", 500, 10},
		{"100f_100r", 100, 100},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				w := testutil.NewWarehouse(b)
				_, _ = w.SeedFactTable(b, "bench", "events", tc.numFiles, tc.rowsPerFile)
				b.StartTimer()

				_, err := janitor.Compact(context.Background(), w.Cat,
					icebergtable.Identifier{"bench", "events"},
					janitor.CompactOptions{
						TargetFileSizeBytes: 128 * 1024 * 1024,
					})
				if err != nil {
					b.Fatalf("Compact: %v", err)
				}
			}
		})
	}
}

// BenchmarkCompact_ManifestWalk isolates the manifest-read cost —
// before compacting, janitor walks every manifest once to count the
// current snapshot's stats. With hundreds of micro-manifests from
// streaming commits this walk is often the second-largest cost
// after the parquet stitch. This benchmark uses a many-file table
// with the compactor target set high so the actual merge work is
// small, leaving manifest walk + CAS commit as the primary cost.
func BenchmarkCompact_ManifestWalk(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		w := testutil.NewWarehouse(b)
		_, _ = w.SeedFactTable(b, "bench", "manifests", 300, 5)
		b.StartTimer()

		_, err := janitor.Compact(context.Background(), w.Cat,
			icebergtable.Identifier{"bench", "manifests"},
			janitor.CompactOptions{
				TargetFileSizeBytes: 128 * 1024 * 1024,
			})
		if err != nil {
			b.Fatalf("Compact: %v", err)
		}
	}
}
