package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go-assignment/packages/logger"
	"go-assignment/packages/metrics"
)

// RequestIDHeader carries the request ID to and from clients.
const RequestIDHeader = "X-Request-ID"

var (
	validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	numericSegment = regexp.MustCompile(`/[0-9]+(/|$)`)
)

// RequestLogger assigns each request an ID (reusing a well-formed
// incoming X-Request-ID), returns it in the response, stores a logger
// tagged with it in the request context, and logs and measures the
// request once it completes.
func RequestLogger(proxies TrustedProxies) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()

			requestID := r.Header.Get(RequestIDHeader)
			if !validRequestID.MatchString(requestID) {
				requestID = newRequestID()
			}
			w.Header().Set(RequestIDHeader, requestID)

			log := slog.Default().With("request_id", requestID)
			r = r.WithContext(logger.WithContext(r.Context(), log))

			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(recorder, r)

			duration := time.Since(started)
			route := routeLabel(r.URL.Path, recorder.status)

			metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(recorder.status)).Inc()
			metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(duration.Seconds())

			level := slog.LevelInfo
			if recorder.status >= 500 {
				level = slog.LevelError
			}

			log.Log(
				r.Context(),
				level,
				"http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.status,
				"duration_ms", duration.Milliseconds(),
				"client_ip", proxies.ClientIP(r),
			)
		})
	}
}

// routeLabel keeps metric cardinality low: IDs become {id} and
// unmatched paths share one label.
func routeLabel(path string, status int) string {
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		return "unmatched"
	}

	for numericSegment.MatchString(path) {
		path = numericSegment.ReplaceAllString(path, "/{id}$1")
	}
	return strings.TrimSuffix(path, "/")
}

func newRequestID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}

// statusRecorder captures the response status for logging.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if !r.wroteHeader {
		r.status = status
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(body)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
