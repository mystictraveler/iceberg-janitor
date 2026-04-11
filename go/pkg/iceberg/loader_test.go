package iceberg

import (
	"context"
	"strings"
	"testing"
)

func TestLoadTableEmptyLocation(t *testing.T) {
	_, err := LoadTable(context.Background(), "", nil)
	if err == nil {
		t.Fatal("expected error on empty metadata location, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error message = %q, want it to mention 'empty'", err.Error())
	}
}

func TestLoadTableNonExistentPath(t *testing.T) {
	// Valid scheme, nonexistent path — iceberg-go's loader should fail
	// with a wrapped error that our LoadTable prefixes with "loading
	// iceberg table at ...".
	_, err := LoadTable(context.Background(), "file:///tmp/nonexistent-janitor-loader-test-"+t.Name()+"/metadata.json", nil)
	if err == nil {
		t.Fatal("expected error on nonexistent file, got nil")
	}
	if !strings.Contains(err.Error(), "loading iceberg table") {
		t.Errorf("error message = %q, want prefix 'loading iceberg table'", err.Error())
	}
}
