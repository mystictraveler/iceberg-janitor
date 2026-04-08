package janitor

// Type-aware partition value handling for compactOnce.
//
// The CLI accepts `--partition col=value` where `value` is a raw
// string with no embedded type hint. The iceberg type of the
// partition column is what determines how to interpret that string,
// how to compare it against another value, and how to decode bound
// bytes from a manifest_list summary. This file is the single place
// in pkg/janitor that knows about iceberg's primitive type lattice;
// the rest of compact_replace.go just calls into these helpers.
//
// Why this lives in its own file:
//   - The type switch is long (16 iceberg primitive types). Keeping
//     it in compact_replace.go would bury the actual compaction
//     logic.
//   - The helpers are reusable. The future on-commit dispatcher
//     (Pattern C, issue #3) will receive partition values as raw
//     strings from S3 EventBridge events and needs the same parser.
//     Snapshot expiration and orphan removal will need the same
//     comparator helpers when their per-partition predicates land.
//   - The helpers are unit-testable in isolation, which the rest of
//     compact_replace.go currently is not.

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// epochUTC is the iceberg date/timestamp epoch (1970-01-01 UTC),
// reproduced here because iceberg-go's epochTM is unexported.
var epochUTC = time.Unix(0, 0).UTC()

// partitionLiteralBinding is the result of parsing one CLI partition
// constraint. It carries everything compactOnce's two predicates need
// to filter without re-resolving the column or re-parsing the value:
//
//   - PartFieldID: the partition field id, used as the key in
//     DataFile.Partition() (which returns map[int]any) and as the
//     position in ManifestFile.Partitions() (after spec ordering).
//   - SpecPosition: the index of this partition field within
//     spec.Fields(), used to look up the corresponding FieldSummary
//     in a ManifestFile's Partitions() slice.
//   - ResultType: the iceberg type of the partition value AFTER the
//     transform (e.g. for a bucket(N) transform, this is Int32Type
//     even when the source column is StringType). This is the type
//     of the bound bytes in the manifest summaries and the type of
//     the values in DataFile.Partition().
//   - Want: the user-supplied target value, parsed and wrapped in an
//     iceberg.Literal of ResultType.
type partitionLiteralBinding struct {
	PartFieldID  int
	SpecPosition int
	ResultType   icebergpkg.Type
	Want         icebergpkg.Literal
}

// buildPartitionLiterals parses each entry in `rawTuple` (raw CLI
// strings keyed by source-column name) into a typed
// partitionLiteralBinding. The result is a slice (one per
// constraint) so callers can iterate it cheaply in their per-file or
// per-manifest hot loops.
//
// Errors are returned for:
//   - column name not in the schema
//   - column not present as a partition source in the spec
//   - the partition column's result type not being one we know how
//     to parse from a string yet (see parsePartitionLiteral for the
//     supported set; unsupported types fall through to "no
//     pruning" naturally — we return a binding with ResultType set
//     so the matcher can still skip files that don't match by
//     literal-equality, just without manifest-level pruning)
//
// On nil/empty rawTuple, returns nil — meaning whole-table
// compaction with no partition constraints.
func buildPartitionLiterals(rawTuple map[string]string, schema *icebergpkg.Schema, spec icebergpkg.PartitionSpec) ([]partitionLiteralBinding, error) {
	if len(rawTuple) == 0 {
		return nil, nil
	}
	// Materialize the spec fields once so we can index by position.
	var specFields []icebergpkg.PartitionField
	for f := range spec.Fields() {
		specFields = append(specFields, f)
	}
	bindings := make([]partitionLiteralBinding, 0, len(rawTuple))
	for col, raw := range rawTuple {
		schemaField, ok := schema.FindFieldByName(col)
		if !ok {
			return nil, fmt.Errorf("partition column %q not found in schema", col)
		}
		var partField *icebergpkg.PartitionField
		var specPos int
		for i := range specFields {
			if specFields[i].SourceID == schemaField.ID {
				partField = &specFields[i]
				specPos = i
				break
			}
		}
		if partField == nil {
			return nil, fmt.Errorf("schema column %q (id=%d) is not a partition source", col, schemaField.ID)
		}
		resultType := partField.Transform.ResultType(schemaField.Type)
		want, err := parsePartitionLiteral(raw, resultType)
		if err != nil {
			return nil, fmt.Errorf("parsing --partition %s=%q as %s: %w", col, raw, resultType, err)
		}
		bindings = append(bindings, partitionLiteralBinding{
			PartFieldID:  partField.FieldID,
			SpecPosition: specPos,
			ResultType:   resultType,
			Want:         want,
		})
	}
	return bindings, nil
}

// parsePartitionLiteral interprets a raw CLI string as a value of
// the given iceberg type and returns the corresponding Literal.
//
// Supported types and their string formats:
//
//	bool        true / false / 1 / 0
//	int / long  decimal integer (strconv.ParseInt)
//	float / dbl decimal float   (strconv.ParseFloat)
//	string      pass-through
//	binary      base64-encoded
//	fixed       base64-encoded; length must match the FixedType
//	uuid        canonical UUID string (uuid.Parse)
//	date        YYYY-MM-DD
//	time        HH:MM:SS or HH:MM:SS.ffffff (microsecond precision)
//	timestamp   RFC3339 or YYYY-MM-DD HH:MM:SS (microsecond precision)
//	timestamptz same as timestamp
//	timestamp_ns / timestamptz_ns: nanosecond-precision RFC3339Nano
//
// Decimal is intentionally NOT supported yet — it requires a scale
// argument from the type and either iceberg-go's decimal package or
// a third-party big.Float-based parser. Decimals as partition
// columns are uncommon (high cardinality is the norm); falling back
// to "no constraint" via an error here is the conservative choice.
// When a real decimal-partitioned workload comes up, this is the
// natural place to add it.
func parsePartitionLiteral(raw string, t icebergpkg.Type) (icebergpkg.Literal, error) {
	switch t.(type) {
	case icebergpkg.BooleanType:
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "true", "1", "t", "yes", "y":
			return icebergpkg.NewLiteral(true), nil
		case "false", "0", "f", "no", "n":
			return icebergpkg.NewLiteral(false), nil
		default:
			return nil, fmt.Errorf("invalid boolean %q", raw)
		}
	case icebergpkg.Int32Type:
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(int32(v)), nil
	case icebergpkg.Int64Type:
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(v), nil
	case icebergpkg.Float32Type:
		v, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(float32(v)), nil
	case icebergpkg.Float64Type:
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(v), nil
	case icebergpkg.StringType:
		return icebergpkg.NewLiteral(raw), nil
	case icebergpkg.BinaryType:
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("expected base64-encoded binary: %w", err)
		}
		return icebergpkg.NewLiteral(b), nil
	case icebergpkg.FixedType:
		ft := t.(icebergpkg.FixedType)
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("expected base64-encoded fixed: %w", err)
		}
		if len(b) != ft.Len() {
			return nil, fmt.Errorf("fixed length %d does not match column length %d", len(b), ft.Len())
		}
		return icebergpkg.NewLiteral(b), nil
	case icebergpkg.UUIDType:
		u, err := uuid.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid uuid: %w", err)
		}
		return icebergpkg.NewLiteral(u), nil
	case icebergpkg.DateType:
		days, err := parseDateDays(raw)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(icebergpkg.Date(days)), nil
	case icebergpkg.TimeType:
		micros, err := parseTimeMicros(raw)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(icebergpkg.Time(micros)), nil
	case icebergpkg.TimestampType, icebergpkg.TimestampTzType:
		micros, err := parseTimestampMicros(raw)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(icebergpkg.Timestamp(micros)), nil
	case icebergpkg.TimestampNsType, icebergpkg.TimestampTzNsType:
		nanos, err := parseTimestampNanos(raw)
		if err != nil {
			return nil, err
		}
		return icebergpkg.NewLiteral(icebergpkg.TimestampNano(nanos)), nil
	default:
		return nil, fmt.Errorf("unsupported partition column type %s for --partition flag", t)
	}
}

// parseDateDays accepts "YYYY-MM-DD" and returns the number of days
// since the iceberg epoch (1970-01-01 UTC). Iceberg's Date type is
// int32 days-since-epoch.
func parseDateDays(s string) (int32, error) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("expected YYYY-MM-DD: %w", err)
	}
	days := t.Sub(epochUTC) / (24 * time.Hour)
	return int32(days), nil
}

// parseTimeMicros accepts "HH:MM:SS" or "HH:MM:SS.ffffff" (where
// ffffff is microseconds) and returns microseconds since midnight.
// Iceberg's Time type is int64 microseconds since midnight.
func parseTimeMicros(s string) (int64, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{"15:04:05.000000", "15:04:05.000", "15:04:05"} {
		t, err := time.Parse(layout, s)
		if err == nil {
			return int64(t.Sub(t.Truncate(24*time.Hour)) / time.Microsecond), nil
		}
	}
	return 0, fmt.Errorf("expected HH:MM:SS or HH:MM:SS.ffffff: %q", s)
}

// parseTimestampMicros accepts RFC3339 / RFC3339Nano / "YYYY-MM-DD
// HH:MM:SS" and returns microseconds since unix epoch. Iceberg's
// Timestamp and TimestampTz types are int64 microseconds since
// epoch.
func parseTimestampMicros(s string) (int64, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000000", "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.UnixMicro(), nil
		}
	}
	return 0, fmt.Errorf("expected RFC3339 timestamp: %q", s)
}

// parseTimestampNanos: same as parseTimestampMicros but nanoseconds.
// Iceberg's TimestampNs and TimestampTzNs types are int64 nanoseconds.
func parseTimestampNanos(s string) (int64, error) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000000000", "2006-01-02T15:04:05"} {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t.UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("expected RFC3339Nano timestamp: %q", s)
}

// valueToLiteral wraps a Go value (the kind that comes out of
// DataFile.Partition()) in an iceberg.Literal of the given type.
// Returns ok=false when the Go type doesn't match what iceberg uses
// for that iceberg type — that's a programming error and the caller
// should fall back to "include the file" rather than guess.
//
// The Go type ↔ iceberg type mapping mirrors what iceberg-go's
// partitionFieldStats[T] uses internally (manifest.go:893-931):
//   bool        ↔ BoolLiteral
//   int32       ↔ Int32Literal
//   int64       ↔ Int64Literal
//   float32     ↔ Float32Literal
//   float64     ↔ Float64Literal
//   string      ↔ StringLiteral
//   []byte      ↔ BinaryLiteral or FixedLiteral (binary chosen here;
//                same compare semantics)
//   uuid.UUID   ↔ UUIDLiteral
//   iceberg.Date / .Time / .Timestamp / .TimestampNano ↔ corresponding Literal
//   iceberg.Decimal ↔ DecimalLiteral
func valueToLiteral(v any, t icebergpkg.Type) (icebergpkg.Literal, bool) {
	if v == nil {
		return nil, false
	}
	switch t.(type) {
	case icebergpkg.BooleanType:
		if x, ok := v.(bool); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.Int32Type:
		if x, ok := v.(int32); ok {
			return icebergpkg.NewLiteral(x), true
		}
		if x, ok := v.(int); ok {
			return icebergpkg.NewLiteral(int32(x)), true
		}
	case icebergpkg.Int64Type:
		if x, ok := v.(int64); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.Float32Type:
		if x, ok := v.(float32); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.Float64Type:
		if x, ok := v.(float64); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.StringType:
		if x, ok := v.(string); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.BinaryType, icebergpkg.FixedType:
		if x, ok := v.([]byte); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.UUIDType:
		if x, ok := v.(uuid.UUID); ok {
			return icebergpkg.NewLiteral(x), true
		}
	case icebergpkg.DateType:
		if x, ok := v.(icebergpkg.Date); ok {
			return icebergpkg.NewLiteral(x), true
		}
		if x, ok := v.(int32); ok {
			return icebergpkg.NewLiteral(icebergpkg.Date(x)), true
		}
	case icebergpkg.TimeType:
		if x, ok := v.(icebergpkg.Time); ok {
			return icebergpkg.NewLiteral(x), true
		}
		if x, ok := v.(int64); ok {
			return icebergpkg.NewLiteral(icebergpkg.Time(x)), true
		}
	case icebergpkg.TimestampType, icebergpkg.TimestampTzType:
		if x, ok := v.(icebergpkg.Timestamp); ok {
			return icebergpkg.NewLiteral(x), true
		}
		if x, ok := v.(int64); ok {
			return icebergpkg.NewLiteral(icebergpkg.Timestamp(x)), true
		}
	case icebergpkg.TimestampNsType, icebergpkg.TimestampTzNsType:
		if x, ok := v.(icebergpkg.TimestampNano); ok {
			return icebergpkg.NewLiteral(x), true
		}
		if x, ok := v.(int64); ok {
			return icebergpkg.NewLiteral(icebergpkg.TimestampNano(x)), true
		}
	case icebergpkg.DecimalType:
		if x, ok := v.(icebergpkg.Decimal); ok {
			return icebergpkg.NewLiteral(x), true
		}
	}
	return nil, false
}

// compareLiterals returns a negative number when a < b, zero when
// a == b, and a positive number when a > b. The iceberg type `t`
// determines the comparator used (each iceberg type has its own
// totally-ordered comparator from iceberg-go's getComparator[T]).
//
// Returns ok=false for types we don't have a comparator for yet
// (currently: only DecimalType is unsupported, because the
// DecimalLiteral comparator is sensitive to scale and decimal
// partition columns are uncommon enough to defer). The caller's
// safe fallback is to include the manifest / data file rather than
// guess at ordering.
//
// This is the polymorphic-compare primitive that buildManifestPredicate
// uses to check whether a target value is within a manifest's
// [lower, upper] partition bounds. Implementing it as a single
// helper here keeps the type-switch in one place — the rest of the
// compaction code stays type-agnostic.
func compareLiterals(a, b icebergpkg.Literal, t icebergpkg.Type) (int, bool) {
	switch t.(type) {
	case icebergpkg.BooleanType:
		la, oa := a.(icebergpkg.BoolLiteral)
		lb, ob := b.(icebergpkg.BoolLiteral)
		if !oa || !ob {
			return 0, false
		}
		// false < true; otherwise equal.
		var av, bv int
		if bool(la) {
			av = 1
		}
		if bool(lb) {
			bv = 1
		}
		return av - bv, true
	case icebergpkg.Int32Type:
		la, oa := a.(icebergpkg.Int32Literal)
		lb, ob := b.(icebergpkg.Int32Literal)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int32(la), int32(lb)), true
	case icebergpkg.Int64Type:
		la, oa := a.(icebergpkg.Int64Literal)
		lb, ob := b.(icebergpkg.Int64Literal)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int64(la), int64(lb)), true
	case icebergpkg.Float32Type:
		la, oa := a.(icebergpkg.Float32Literal)
		lb, ob := b.(icebergpkg.Float32Literal)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(float32(la), float32(lb)), true
	case icebergpkg.Float64Type:
		la, oa := a.(icebergpkg.Float64Literal)
		lb, ob := b.(icebergpkg.Float64Literal)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(float64(la), float64(lb)), true
	case icebergpkg.StringType:
		la, oa := a.(icebergpkg.StringLiteral)
		lb, ob := b.(icebergpkg.StringLiteral)
		if !oa || !ob {
			return 0, false
		}
		return strings.Compare(string(la), string(lb)), true
	case icebergpkg.BinaryType, icebergpkg.FixedType:
		var ab, bb []byte
		switch x := a.(type) {
		case icebergpkg.BinaryLiteral:
			ab = []byte(x)
		case icebergpkg.FixedLiteral:
			ab = []byte(x)
		default:
			return 0, false
		}
		switch x := b.(type) {
		case icebergpkg.BinaryLiteral:
			bb = []byte(x)
		case icebergpkg.FixedLiteral:
			bb = []byte(x)
		default:
			return 0, false
		}
		return bytesCompare(ab, bb), true
	case icebergpkg.UUIDType:
		la, oa := a.(icebergpkg.UUIDLiteral)
		lb, ob := b.(icebergpkg.UUIDLiteral)
		if !oa || !ob {
			return 0, false
		}
		ua, ub := uuid.UUID(la), uuid.UUID(lb)
		return bytesCompare(ua[:], ub[:]), true
	case icebergpkg.DateType:
		la, oa := a.(icebergpkg.DateLiteral)
		lb, ob := b.(icebergpkg.DateLiteral)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int32(icebergpkg.Date(la)), int32(icebergpkg.Date(lb))), true
	case icebergpkg.TimeType:
		la, oa := a.(icebergpkg.TimeLiteral)
		lb, ob := b.(icebergpkg.TimeLiteral)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int64(icebergpkg.Time(la)), int64(icebergpkg.Time(lb))), true
	case icebergpkg.TimestampType, icebergpkg.TimestampTzType:
		la, oa := a.(icebergpkg.TimestampLiteral)
		lb, ob := b.(icebergpkg.TimestampLiteral)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int64(icebergpkg.Timestamp(la)), int64(icebergpkg.Timestamp(lb))), true
	case icebergpkg.TimestampNsType, icebergpkg.TimestampTzNsType:
		la, oa := a.(icebergpkg.TimestampNsLiteral)
		lb, ob := b.(icebergpkg.TimestampNsLiteral)
		if !oa || !ob {
			return 0, false
		}
		return cmpOrdered(int64(icebergpkg.TimestampNano(la)), int64(icebergpkg.TimestampNano(lb))), true
	case icebergpkg.DecimalType:
		// DecimalLiteral compares require equal scale; iceberg-go's
		// own comparator handles this internally but exposes only the
		// generic Comparator[Decimal]. Conservative fallback: refuse
		// to compare and let the caller include the manifest rather
		// than guess. Decimal partition columns are uncommon (high
		// cardinality is the norm). When a real workload exercises
		// this, the right fix is to type-assert to TypedLiteral[Decimal]
		// and use its Comparator() — that's the same shape as the
		// other cases above, just with iceberg.Decimal as T.
		return 0, false
	}
	return 0, false
}

// cmpOrdered is the standard "compare two ordered Go values" pattern,
// inlined here so we don't pull in the cmp package just for one
// helper. Returns negative / zero / positive.
func cmpOrdered[T int32 | int64 | float32 | float64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// bytesCompare wraps bytes.Compare so the file doesn't import
// "bytes" just for one call (every other byte op in this file is
// already through encoding/base64).
func bytesCompare(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}
