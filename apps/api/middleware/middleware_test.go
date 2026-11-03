package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func serve(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestAPIKeyAuth(t *testing.T) {
	handler := APIKeyAuth([]string{"first-key", "second-key"}, "/v1/health")(okHandler)

	tests := []struct {
		name   string
		path   string
		header string
		value  string
		want   int
	}{
		{"missing key", "/v1/jobs", "", "", http.StatusUnauthorized},
		{"wrong key", "/v1/jobs", "Authorization", "Bearer nope", http.StatusUnauthorized},
		{"wrong scheme", "/v1/jobs", "Authorization", "Basic first-key", http.StatusUnauthorized},
		{"bearer key", "/v1/jobs", "Authorization", "Bearer first-key", http.StatusOK},
		{"second key", "/v1/jobs", "Authorization", "bearer second-key", http.StatusOK},
		{"x-api-key header", "/v1/jobs", "X-API-Key", "second-key", http.StatusOK},
		{"public path", "/v1/health", "", "", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.header != "" {
				request.Header.Set(tt.header, tt.value)
			}

			response := serve(handler, request)
			if response.Code != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, response.Code)
			}
			if tt.want == http.StatusUnauthorized && response.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("expected a WWW-Authenticate header")
			}
		})
	}
}

func TestSecurityHeaders(t *testing.T) {
	response := serve(SecurityHeaders(okHandler), httptest.NewRequest(http.MethodGet, "/", nil))

	for header, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	} {
		if got := response.Header().Get(header); got != want {
			t.Errorf("%s: expected %q, got %q", header, want, got)
		}
	}
	if response.Header().Get("Content-Security-Policy") == "" {
		t.Error("expected a Content-Security-Policy header")
	}
}

func TestRateLimiterBlocksAfterBurstPerClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewRateLimiter(ctx, 1, 3, nil).Middleware(okHandler)

	request := func(remoteAddr string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		r.RemoteAddr = remoteAddr
		return serve(handler, r)
	}

	for i := 0; i < 3; i++ {
		if code := request("203.0.113.1:1000").Code; code != http.StatusOK {
			t.Fatalf("request %d within burst: expected 200, got %d", i+1, code)
		}
	}

	limited := request("203.0.113.1:1001")
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after the burst, got %d", limited.Code)
	}
	if limited.Header().Get("Retry-After") == "" {
		t.Fatal("expected a Retry-After header")
	}

	// Another client has its own bucket.
	if code := request("203.0.113.2:1000").Code; code != http.StatusOK {
		t.Fatalf("other client: expected 200, got %d", code)
	}
}

func TestClientIPIgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "203.0.113.9:4000"
	request.Header.Set("X-Forwarded-For", "198.51.100.1")

	if got := TrustedProxies(nil).ClientIP(request); got != "203.0.113.9" {
		t.Fatalf("expected the peer address, got %q", got)
	}
}

func TestClientIPUsesForwardedForFromTrustedProxy(t *testing.T) {
	proxies, err := ParseTrustedProxies("10.0.0.0/8, 192.168.1.10")
	if err != nil {
		t.Fatalf("ParseTrustedProxies: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "10.1.2.3:4000"
	// The client spoofed the first entry; the proxies appended the rest.
	request.Header.Set("X-Forwarded-For", "1.1.1.1, 198.51.100.7, 192.168.1.10")

	if got := proxies.ClientIP(request); got != "198.51.100.7" {
		t.Fatalf("expected the first untrusted hop, got %q", got)
	}
}

func TestParseTrustedProxiesRejectsInvalidEntries(t *testing.T) {
	if _, err := ParseTrustedProxies("10.0.0.0/8,not-an-ip"); err == nil {
		t.Fatal("expected an error for an invalid entry")
	}
}

func TestRequestLoggerSetsRequestID(t *testing.T) {
	handler := RequestLogger(nil)(okHandler)

	generated := serve(handler, httptest.NewRequest(http.MethodGet, "/v1/jobs", nil))
	if len(generated.Header().Get(RequestIDHeader)) != 32 {
		t.Fatalf("expected a generated request ID, got %q", generated.Header().Get(RequestIDHeader))
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	request.Header.Set(RequestIDHeader, "client-id-123")
	if got := serve(handler, request).Header().Get(RequestIDHeader); got != "client-id-123" {
		t.Fatalf("expected the client request ID to be kept, got %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	request.Header.Set(RequestIDHeader, "bad id\nwith newline")
	if got := serve(handler, request).Header().Get(RequestIDHeader); got == "bad id\nwith newline" {
		t.Fatal("expected an invalid request ID to be replaced")
	}
}

func TestRouteLabel(t *testing.T) {
	tests := map[string]string{
		"/v1/jobs":           "/v1/jobs",
		"/v1/jobs/42":        "/v1/jobs/{id}",
		"/v1/jobs/42/errors": "/v1/jobs/{id}/errors",
	}

	for path, want := range tests {
		if got := routeLabel(path, http.StatusOK); got != want {
			t.Errorf("routeLabel(%q) = %q, want %q", path, got, want)
		}
	}

	if got := routeLabel("/random/scan", http.StatusNotFound); got != "unmatched" {
		t.Errorf("expected unmatched for 404, got %q", got)
	}
}
