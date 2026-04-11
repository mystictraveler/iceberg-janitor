// Command janitor-server is the HTTP adapter for the janitor's
// pkg/janitor.Core entry points. Same code path as cmd/janitor-cli
// (in-process mode) and the planned cmd/janitor-lambda — three thin
// wrappers around one shared core.
//
// MVP scope: discover, analyze, and compact endpoints, plus
// /v1/healthz and /v1/readyz. Auth is delegated to the deployment
// platform (Knative + Kourier with auth, an ALB with Cognito, an API
// gateway, etc.) — the server itself does not enforce identity. This
// matches Knative conventions and keeps the server's surface small.
//
// Mandatory env vars:
//
//	JANITOR_WAREHOUSE_URL   gocloud.dev/blob URL of the warehouse.
//	                        Examples: file:///tmp/warehouse,
//	                        s3://my-warehouse?region=us-east-1
//
// Optional env vars:
//
//	JANITOR_LISTEN          Address to listen on. Default :8080
//	                        (Knative convention).
//	JANITOR_SHUTDOWN_TIMEOUT_SECONDS  Default 30.
//	S3_ENDPOINT, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
//	                        For MinIO / S3-compatible. Same handling as
//	                        the CLI.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	// Register gocloud.dev/blob URL openers so blob.OpenBucket can
	// dispatch on the JANITOR_WAREHOUSE_URL scheme.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	shutdownTracer := observe.Init("janitor-server")
	defer shutdownTracer()

	listen := getenv("JANITOR_LISTEN", ":8080")
	warehouseURL := os.Getenv("JANITOR_WAREHOUSE_URL")
	if warehouseURL == "" {
		logger.Error("JANITOR_WAREHOUSE_URL is required")
		os.Exit(2)
	}
	propagateAWSEnv()
	shutdownTimeout := time.Duration(getenvInt("JANITOR_SHUTDOWN_TIMEOUT_SECONDS", 30)) * time.Second

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", warehouseURL, propsFromEnv())
	if err != nil {
		logger.Error("opening directory catalog", "warehouse", warehouseURL, "err", err)
		os.Exit(1)
	}
	defer cat.Close()

	// Use the persistent jobStore so the lease + jobrecord layer
	// is active in production. Multi-replica deployments rely on
	// this for cross-replica dedup; single-replica deployments
	// still get the local fast-path cache + persistent records
	// for crash recovery and operator observability.
	srv := &server{
		cat:     cat,
		logger:  logger,
		started: time.Now(),
		jobs:    newPersistentJobStore(cat.Bucket()),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthz", srv.handleHealthz)
	mux.HandleFunc("GET /v1/readyz", srv.handleReadyz)
	mux.HandleFunc("GET /v1/tables", srv.handleListTables)
	mux.HandleFunc("GET /v1/tables/{ns}/{name}/health", srv.handleAnalyze)
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/compact", srv.handleCompactAsync)
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/expire", srv.handleExpireAsync)
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/rewrite-manifests", srv.handleRewriteManifestsAsync)
	mux.HandleFunc("POST /v1/tables/{ns}/{name}/maintain", srv.handleMaintainAsync)
	mux.HandleFunc("GET /v1/jobs/{id}", srv.handleJobStatus)

	httpServer := &http.Server{
		Addr:              listen,
		Handler:           withRequestID(withLogging(mux, logger)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("janitor-server listening", "addr", listen, "warehouse", warehouseURL)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining", "timeout", shutdownTimeout)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown error", "err", err)
	}
	logger.Info("shutdown complete")
}

type server struct {
	cat     *catalog.DirectoryCatalog
	logger  *slog.Logger
	started time.Time
	jobs    *jobStore
}

// === handlers ===

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"uptime_seconds": time.Since(s.started).Seconds(),
		"warehouse":      s.cat.WarehouseURL(),
	})
}

func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Cheap probe: list namespaces (just verifies the bucket is reachable).
	if _, err := catalog.DiscoverTables(r.Context(), s.cat.Bucket(), ""); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *server) handleListTables(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	tables, err := catalog.DiscoverTables(r.Context(), s.cat.Bucket(), prefix)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "discover failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tables": tables,
		"count":  len(tables),
	})
}

func (s *server) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	tbl, err := s.cat.LoadTable(r.Context(), ident)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "table not found", err)
		return
	}
	report, err := analyzer.Assess(r.Context(), tbl, analyzer.AnalyzerOptions{})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "analyze failed", err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *server) handleCompact(w http.ResponseWriter, r *http.Request) {
	ident, ok := identFromRequest(w, r)
	if !ok {
		return
	}
	s.logger.Info("compact request", "namespace", ident[0], "table", ident[1])
	result, err := janitor.Compact(r.Context(), s.cat, ident, janitor.CompactOptions{})
	if err != nil {
		// Distinguish CAS-exhaustion from master-check-failure from
		// internal errors so callers can react.
		status := http.StatusInternalServerError
		if errors.Is(err, catalog.ErrCASConflict) {
			status = http.StatusConflict
		} else if result != nil && result.Verification != nil && result.Verification.Overall == "fail" {
			status = http.StatusPreconditionFailed
		}
		writeJSON(w, status, map[string]any{
			"error":  err.Error(),
			"result": result,
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// === helpers ===

func identFromRequest(w http.ResponseWriter, r *http.Request) (icebergtable.Identifier, bool) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if ns == "" || name == "" {
		writeJSONError(w, http.StatusBadRequest, "missing namespace or table name", nil)
		return nil, false
	}
	// Strip the iceberg-go default ".db" suffix from the namespace, the
	// same way the CLI does. The on-disk path uses "<ns>.db/<name>",
	// but the catalog identifier is just [ns, name].
	ns = strings.TrimSuffix(ns, ".db")
	return icebergtable.Identifier{ns, name}, true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string, err error) {
	body := map[string]string{"error": msg}
	if err != nil {
		body["detail"] = err.Error()
	}
	writeJSON(w, status, body)
}

// === middleware ===

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), reqIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type reqIDKey struct{}

func withLogging(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		reqID, _ := r.Context().Value(reqIDKey{}).(string)
		logger.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(started).Milliseconds(),
			"request_id", reqID,
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// === env ===

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}

// propsFromEnv collects S3-style credentials/endpoint env vars into the
// property map iceberg-go's IO layer expects. Same behavior as the CLI.
func propsFromEnv() map[string]string {
	props := map[string]string{}
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		props["s3.endpoint"] = v
	}
	if v := os.Getenv("S3_REGION"); v != "" {
		props["s3.region"] = v
	} else if v := os.Getenv("AWS_REGION"); v != "" {
		props["s3.region"] = v
	}
	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		props["s3.access-key-id"] = v
	}
	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		props["s3.secret-access-key"] = v
	}
	return props
}

// propagateAWSEnv mirrors the CLI's env-var dance: iceberg-go's
// table-level IO is constructed from table properties (not catalog
// props), so we set the AWS_* env vars that iceberg-go reads as
// fallbacks.
func propagateAWSEnv() {
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		os.Setenv("AWS_S3_ENDPOINT", v)
		os.Setenv("AWS_ENDPOINT_URL_S3", v)
	}
	if v := os.Getenv("S3_REGION"); v != "" {
		os.Setenv("AWS_REGION", v)
		os.Setenv("AWS_DEFAULT_REGION", v)
	}
}
