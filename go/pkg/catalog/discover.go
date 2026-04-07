package catalog

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gocloud.dev/blob"
)

// TableLocation identifies an Iceberg table by its prefix in the warehouse
// and the highest-numbered metadata.json file currently present.
type TableLocation struct {
	// Prefix is the bucket-relative path of the table directory, with no
	// leading or trailing slash. e.g. "db/events".
	Prefix string

	// CurrentMetadata is the bucket-relative path of the highest-numbered
	// v*.metadata.json file currently present in <Prefix>/metadata/.
	// Empty if the table has no metadata file (which means it isn't a
	// table — discovery should not return such entries).
	CurrentMetadata string

	// CurrentVersion is the integer version parsed out of CurrentMetadata.
	CurrentVersion int
}

// DiscoverTables walks the bucket under the given root prefix and returns
// every Iceberg table found. A directory is recognized as a table if and
// only if it contains a "metadata/" subdirectory with at least one file
// matching the pattern "v<int>.metadata.json".
//
// This is the v0 discovery primitive: it does NOT load metadata, parse
// schemas, or interact with the catalog interface. It just answers the
// question "where are the tables in this bucket?"
//
// rootPrefix may be empty (whole bucket) or a path like "warehouse/" or
// "warehouse/db/". If non-empty it must end in "/".
func DiscoverTables(ctx context.Context, bucket *blob.Bucket, rootPrefix string) ([]TableLocation, error) {
	if rootPrefix != "" && !strings.HasSuffix(rootPrefix, "/") {
		rootPrefix += "/"
	}

	var tables []TableLocation
	if err := walkForTables(ctx, bucket, rootPrefix, &tables); err != nil {
		return nil, err
	}
	return tables, nil
}

// walkForTables recursively descends from `prefix`, looking for directories
// that contain a metadata/ subdirectory with v*.metadata.json files. When it
// finds one, the containing directory is recorded as a table and recursion
// stops for that branch (Iceberg tables don't nest).
func walkForTables(ctx context.Context, bucket *blob.Bucket, prefix string, out *[]TableLocation) error {
	// Check whether this prefix itself is a table by looking for a metadata
	// subdirectory with at least one v*.metadata.json file.
	current, version, err := findCurrentMetadata(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("checking for metadata under %q: %w", prefix, err)
	}
	if current != "" {
		// strip trailing slash for the Prefix field
		tablePrefix := strings.TrimSuffix(prefix, "/")
		*out = append(*out, TableLocation{
			Prefix:          tablePrefix,
			CurrentMetadata: current,
			CurrentVersion:  version,
		})
		return nil
	}

	// Not a table itself — descend into immediate subdirectories.
	iter := bucket.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("listing %q: %w", prefix, err)
		}
		if !obj.IsDir {
			continue
		}
		// Skip the janitor's own state prefix if we hit it.
		if strings.HasSuffix(obj.Key, "/_janitor/") || strings.HasSuffix(obj.Key, "/metadata/") || strings.HasSuffix(obj.Key, "/data/") {
			continue
		}
		if err := walkForTables(ctx, bucket, obj.Key, out); err != nil {
			return err
		}
	}
}

// findCurrentMetadata lists <tablePrefix>metadata/ and returns the path of
// the highest-numbered v*.metadata.json file along with its parsed version.
// Returns ("", 0, nil) if there is no metadata directory or no v*.metadata.json
// files in it.
func findCurrentMetadata(ctx context.Context, bucket *blob.Bucket, tablePrefix string) (string, int, error) {
	metadataPrefix := tablePrefix + "metadata/"
	iter := bucket.List(&blob.ListOptions{
		Prefix: metadataPrefix,
	})
	var bestPath string
	bestVersion := -1
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, err
		}
		if obj.IsDir {
			continue
		}
		name := strings.TrimPrefix(obj.Key, metadataPrefix)
		v, ok := parseMetadataVersion(name)
		if !ok {
			continue
		}
		if v > bestVersion {
			bestVersion = v
			bestPath = obj.Key
		}
	}
	if bestVersion < 0 {
		return "", 0, nil
	}
	return bestPath, bestVersion, nil
}

// parseMetadataVersion parses a name like "v42.metadata.json" or
// "00042-1234abcd.metadata.json" and returns the integer version and true on
// success. The Iceberg spec actually permits both naming conventions; for
// the v0 discovery primitive we accept both but the v* form is the most
// common.
func parseMetadataVersion(name string) (int, bool) {
	if !strings.HasSuffix(name, ".metadata.json") {
		return 0, false
	}
	stem := strings.TrimSuffix(name, ".metadata.json")
	// "v<int>" form
	if strings.HasPrefix(stem, "v") {
		var v int
		if _, err := fmt.Sscanf(stem, "v%d", &v); err == nil {
			return v, true
		}
	}
	// "<int>-<uuid>" form (Iceberg spec, used by the Java writer)
	if i := strings.IndexByte(stem, '-'); i > 0 {
		var v int
		if _, err := fmt.Sscanf(stem[:i], "%d", &v); err == nil {
			return v, true
		}
	}
	// bare "<int>" form
	var v int
	if _, err := fmt.Sscanf(stem, "%d", &v); err == nil {
		return v, true
	}
	return 0, false
}
