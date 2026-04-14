"""Compare JMeter CSV results from before and after compaction runs.

Reads the JMeter CSV result files and produces a side-by-side comparison
report showing the impact of compaction on query performance.

Usage:
    python compare-results.py \
        --before results/before-compaction.csv \
        --after  results/after-compaction.csv \
        --output results/comparison-report.txt
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SamplerStats:
    """Aggregated statistics for a single sampler (query type)."""

    label: str
    times: list[int] = field(default_factory=list)
    successes: int = 0
    failures: int = 0
    bytes_total: int = 0
    start_ts: int = 0
    end_ts: int = 0

    def add(self, elapsed: int, success: bool, nbytes: int, timestamp: int) -> None:
        self.times.append(elapsed)
        if success:
            self.successes += 1
        else:
            self.failures += 1
        self.bytes_total += nbytes
        if self.start_ts == 0 or timestamp < self.start_ts:
            self.start_ts = timestamp
        if timestamp + elapsed > self.end_ts:
            self.end_ts = timestamp + elapsed

    @property
    def count(self) -> int:
        return len(self.times)

    @property
    def avg(self) -> float:
        return sum(self.times) / len(self.times) if self.times else 0

    @property
    def median(self) -> float:
        return _percentile(self.times, 50)

    @property
    def p90(self) -> float:
        return _percentile(self.times, 90)

    @property
    def p95(self) -> float:
        return _percentile(self.times, 95)

    @property
    def p99(self) -> float:
        return _percentile(self.times, 99)

    @property
    def min_time(self) -> int:
        return min(self.times) if self.times else 0

    @property
    def max_time(self) -> int:
        return max(self.times) if self.times else 0

    @property
    def error_pct(self) -> float:
        total = self.successes + self.failures
        return (self.failures / total * 100) if total > 0 else 0

    @property
    def throughput(self) -> float:
        """Requests per second."""
        duration_s = (self.end_ts - self.start_ts) / 1000.0 if self.end_ts > self.start_ts else 1
        return self.count / duration_s


def _percentile(data: list[int], pct: float) -> float:
    if not data:
        return 0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100.0
    f = int(k)
    c = f + 1 if f + 1 < len(s) else f
    return s[f] + (k - f) * (s[c] - s[f])


def parse_jmeter_csv(path: Path) -> dict[str, SamplerStats]:
    """Parse a JMeter CSV results file into per-label stats."""
    stats: dict[str, SamplerStats] = {}
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            label = row.get("label", "unknown")
            elapsed = int(row.get("elapsed", 0))
            success = row.get("success", "true").lower() == "true"
            nbytes = int(row.get("bytes", 0))
            timestamp = int(row.get("timeStamp", 0))
            if label not in stats:
                stats[label] = SamplerStats(label=label)
            stats[label].add(elapsed, success, nbytes, timestamp)
    return stats


def format_change(before: float, after: float) -> str:
    """Format a percentage change with arrow indicator."""
    if before == 0:
        return "N/A"
    pct = ((after - before) / before) * 100
    arrow = "v" if pct < 0 else "^" if pct > 0 else "="
    return f"{pct:+.1f}% {arrow}"


def generate_report(
    before: dict[str, SamplerStats],
    after: dict[str, SamplerStats],
) -> str:
    """Generate a human-readable comparison report."""
    lines: list[str] = []
    sep = "=" * 90
    thin_sep = "-" * 90

    lines.append(sep)
    lines.append("  ICEBERG QUERY BENCHMARK - COMPACTION IMPACT REPORT")
    lines.append(sep)
    lines.append("")

    # Collect all labels from both runs
    all_labels = sorted(set(list(before.keys()) + list(after.keys())))

    # Per-query comparison
    for label in all_labels:
        b = before.get(label)
        a = after.get(label)

        lines.append(thin_sep)
        lines.append(f"  Query: {label}")
        lines.append(thin_sep)

        header = f"  {'Metric':<25} {'Before':>12} {'After':>12} {'Change':>15}"
        lines.append(header)
        lines.append(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*15}")

        metrics = [
            ("Samples", "count", False),
            ("Avg (ms)", "avg", True),
            ("Median (ms)", "median", True),
            ("P90 (ms)", "p90", True),
            ("P95 (ms)", "p95", True),
            ("P99 (ms)", "p99", True),
            ("Min (ms)", "min_time", True),
            ("Max (ms)", "max_time", True),
            ("Error %", "error_pct", True),
            ("Throughput (req/s)", "throughput", False),
        ]

        for name, attr, lower_is_better in metrics:
            bv = getattr(b, attr) if b else 0
            av = getattr(a, attr) if a else 0

            if isinstance(bv, float):
                bv_str = f"{bv:.1f}"
                av_str = f"{av:.1f}"
            else:
                bv_str = str(bv)
                av_str = str(av)

            change = format_change(bv, av)
            lines.append(f"  {name:<25} {bv_str:>12} {av_str:>12} {change:>15}")

        lines.append("")

    # Summary
    lines.append(sep)
    lines.append("  SUMMARY")
    lines.append(sep)

    total_before_avg = 0.0
    total_after_avg = 0.0
    count = 0
    for label in all_labels:
        b = before.get(label)
        a = after.get(label)
        if b and a:
            total_before_avg += b.avg
            total_after_avg += a.avg
            count += 1

    if count > 0:
        avg_before = total_before_avg / count
        avg_after = total_after_avg / count
        overall_change = ((avg_after - avg_before) / avg_before * 100) if avg_before else 0

        lines.append(f"  Average response time across all queries:")
        lines.append(f"    Before compaction: {avg_before:.1f} ms")
        lines.append(f"    After compaction:  {avg_after:.1f} ms")
        lines.append(f"    Change:            {overall_change:+.1f}%")
        lines.append("")

        if overall_change < -10:
            lines.append("  RESULT: Compaction significantly improved query performance.")
        elif overall_change < 0:
            lines.append("  RESULT: Compaction provided a modest improvement in query performance.")
        elif overall_change < 10:
            lines.append("  RESULT: Compaction had minimal impact on query performance.")
        else:
            lines.append("  RESULT: Query performance degraded after compaction (unexpected).")
    else:
        lines.append("  No matching queries found between before and after runs.")

    lines.append("")
    lines.append(sep)
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare JMeter results before and after Iceberg compaction",
    )
    parser.add_argument(
        "--before",
        required=True,
        type=Path,
        help="Path to before-compaction JMeter CSV results",
    )
    parser.add_argument(
        "--after",
        required=True,
        type=Path,
        help="Path to after-compaction JMeter CSV results",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for comparison report (default: stdout)",
    )
    args = parser.parse_args()

    if not args.before.exists():
        print(f"ERROR: Before results file not found: {args.before}", file=sys.stderr)
        sys.exit(1)
    if not args.after.exists():
        print(f"ERROR: After results file not found: {args.after}", file=sys.stderr)
        sys.exit(1)

    before_stats = parse_jmeter_csv(args.before)
    after_stats = parse_jmeter_csv(args.after)

    report = generate_report(before_stats, after_stats)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report)
        print(f"Report written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
