package main

import (
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
)

// withTracing wraps the HTTP mux with a per-request OTel span. The
// span name is "HTTP <METHOD> <route>", where route is the matched
// pattern when the server is running under Go 1.22+'s ServeMux (which
// exposes the matched pattern via r.Pattern) and the raw URL path
// otherwise. Attributes recorded on every request:
//
//	http.method       — request method
//	http.route        — matched route pattern (or raw path)
//	http.status_code  — response status as seen by the client
//	http.duration_ms  — wall time from handler entry to handler exit
//
// When the response is 5xx, the span's status is set to Error via
// observe.RecordError so traces can be filtered on server-side
// failures. 4xx responses are NOT treated as errors — they are
// caller-induced and not a server fault.
//
// Written by hand instead of pulling in
// go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp to
// keep the dep surface minimal (the user is strict about new
// dependencies — see feedback_deps_and_libs.md in the project
// memory). The default NoOp tracer keeps the cost effectively zero
// in production.
func withTracing(next http.Handler) http.Handler {
	tr := observe.Tracer("janitor.http")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()

		// Use the matched pattern when available (Go 1.22+); fall back
		// to the raw URL path when the request didn't match a mux
		// pattern (which shouldn't happen on healthy routes but is
		// possible for 404s from a raw http.Handler chain).
		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		ctx, span := tr.Start(r.Context(), "HTTP "+r.Method+" "+route)
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.route", route),
		)
		defer span.End()

		rec := &tracingStatusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		elapsed := time.Since(started).Milliseconds()
		span.SetAttributes(
			attribute.Int("http.status_code", rec.status),
			attribute.Int64("http.duration_ms", elapsed),
		)
		if rec.status >= 500 {
			// 5xx is a server-side failure — mark the span as an
			// error so dashboards and sampling rules can find it.
			observe.RecordError(span, httpStatusError(rec.status))
		}
	})
}

// tracingStatusRecorder captures the HTTP response status so the
// tracing middleware can surface it as an attribute. We keep this
// type separate from statusRecorder in main.go because the two
// middlewares compose independently and sharing the recorder would
// create a confusing cross-file dependency.
type tracingStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *tracingStatusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// httpStatusError is a tiny error type used to fill in span.RecordError
// for 5xx responses. The error string shows up in the span's status
// message; that's the only consumer.
type httpStatusError int

func (e httpStatusError) Error() string {
	return http.StatusText(int(e))
}
