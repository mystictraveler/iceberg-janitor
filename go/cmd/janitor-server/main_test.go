package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "gocloud.dev/blob/fileblob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
)

// newTestServer constructs a *server backed by an empty fileblob
// warehouse rooted at t.TempDir(). The directory exists but contains
// no namespaces or tables. Tests that need a populated table should
// build it via pkg/catalog test helpers; the cmd-level tests here
// only verify HTTP plumbing, so an empty warehouse is fine.
func newTestServer(t *testing.T) *server {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ns.db", "tbl", "metadata"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	url := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(context.Background(), "janitor", url, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	t.Cleanup(func() { cat.Close() })
	return &server{
		cat:     cat,
		logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		started: time.Now().Add(-5 * time.Second),
	}
}

func TestHandleHealthz(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	srv.handleHealthz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field: got %v want ok", body["status"])
	}
	// Uptime is computed from time.Since(s.started); we set started
	// 5s in the past so the field should be present and positive.
	if u, ok := body["uptime_seconds"].(float64); !ok || u <= 0 {
		t.Errorf("uptime_seconds: got %v (type %T) want positive float", body["uptime_seconds"], body["uptime_seconds"])
	}
	if _, ok := body["warehouse"].(string); !ok {
		t.Errorf("warehouse field missing or not string: %v", body["warehouse"])
	}
}

func TestHandleReadyz_OK(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/readyz", nil)
	srv.handleReadyz(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d want 200; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleListTables_Empty(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/tables", nil)
	srv.handleListTables(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Tables []any `json:"tables"`
		Count  int   `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	// The fileblob warehouse is empty (the metadata/ dir we created
	// has no metadata.json), so DiscoverTables returns nothing.
	if body.Count != 0 {
		t.Errorf("count: got %d want 0; tables=%v", body.Count, body.Tables)
	}
}

// TestIdentFromRequest covers the path-parameter parser used by the
// per-table endpoints. The .db suffix on the namespace must be
// stripped to match what the rest of the catalog uses for table
// identifiers (the on-disk path is "<ns>.db/<name>"; the identifier
// is just [ns, name]).
func TestIdentFromRequest(t *testing.T) {
	cases := []struct {
		ns, name string
		wantNS   string
		wantTbl  string
		wantOK   bool
	}{
		{"mvp.db", "events", "mvp", "events", true},
		{"mvp", "events", "mvp", "events", true},
		{"tpcds.db", "store_sales", "tpcds", "store_sales", true},
		{"", "events", "", "", false},
		{"mvp", "", "", "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.ns+"/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/tables/x/y/health", nil)
			req.SetPathValue("ns", tc.ns)
			req.SetPathValue("name", tc.name)
			rec := httptest.NewRecorder()
			ident, ok := identFromRequest(rec, req)
			if ok != tc.wantOK {
				t.Errorf("ok: got %v want %v (status=%d body=%s)", ok, tc.wantOK, rec.Code, rec.Body.String())
				return
			}
			if !tc.wantOK {
				if rec.Code != http.StatusBadRequest {
					t.Errorf("expected 400 on bad input, got %d", rec.Code)
				}
				return
			}
			if len(ident) != 2 || ident[0] != tc.wantNS || ident[1] != tc.wantTbl {
				t.Errorf("ident: got %v want [%s %s]", ident, tc.wantNS, tc.wantTbl)
			}
		})
	}
}

// TestHandleAnalyze_NotFound exercises the not-found path: an
// identifier that doesn't resolve to a table on disk should produce
// a 404 with a structured error body, not a 500 or a panic.
func TestHandleAnalyze_NotFound(t *testing.T) {
	srv := newTestServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/tables/nope/missing/health", nil)
	req.SetPathValue("ns", "nope")
	req.SetPathValue("name", "missing")
	srv.handleAnalyze(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status: got %d want 404; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if body["error"] == "" {
		t.Errorf("missing error field: %v", body)
	}
}

// TestRequestIDMiddleware verifies that withRequestID either echoes a
// caller-supplied X-Request-ID header or generates one. The
// middleware also makes the id available via context to downstream
// handlers; we don't assert that here because nothing in the test
// stack reads it, but we do verify the response header round-trip.
func TestRequestIDMiddleware(t *testing.T) {
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	wrapped := withRequestID(noop)

	t.Run("echoes caller header", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.Header.Set("X-Request-ID", "abc-123")
		wrapped.ServeHTTP(rec, req)
		if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
			t.Errorf("X-Request-ID: got %q want abc-123", got)
		}
	})
	t.Run("generates when missing", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		wrapped.ServeHTTP(rec, req)
		if got := rec.Header().Get("X-Request-ID"); got == "" {
			t.Error("X-Request-ID missing when caller did not supply one")
		}
	})
}

func TestGetenvAndGetenvInt(t *testing.T) {
	t.Setenv("JANITOR_TEST_STR", "value")
	if got := getenv("JANITOR_TEST_STR", "default"); got != "value" {
		t.Errorf("getenv: got %q want value", got)
	}
	if got := getenv("JANITOR_TEST_MISSING", "fallback"); got != "fallback" {
		t.Errorf("getenv default: got %q want fallback", got)
	}

	t.Setenv("JANITOR_TEST_INT", "42")
	if got := getenvInt("JANITOR_TEST_INT", 7); got != 42 {
		t.Errorf("getenvInt: got %d want 42", got)
	}
	if got := getenvInt("JANITOR_TEST_MISSING", 7); got != 7 {
		t.Errorf("getenvInt default: got %d want 7", got)
	}
	t.Setenv("JANITOR_TEST_BAD", "not-a-number")
	if got := getenvInt("JANITOR_TEST_BAD", 99); got != 99 {
		t.Errorf("getenvInt fallback on bad input: got %d want 99", got)
	}
}
