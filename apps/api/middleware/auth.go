package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// APIKeyAuth requires a valid API key on every request, sent as
// "Authorization: Bearer <key>" or "X-API-Key: <key>". Requests whose
// path is in public (for example the health check) are not checked.
//
// Keys are compared as SHA-256 digests in constant time, so response
// timing does not reveal how much of a key matched.
func APIKeyAuth(keys []string, public ...string) func(http.Handler) http.Handler {
	digests := make([][32]byte, 0, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			digests = append(digests, sha256.Sum256([]byte(key)))
		}
	}

	publicPaths := make(map[string]bool, len(public))
	for _, path := range public {
		publicPaths[path] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if publicPaths[r.URL.Path] || validKey(digests, requestKey(r)) {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "a valid API key is required",
			})
		})
	}
}

// ParseAPIKeys splits a comma-separated API_KEYS value.
func ParseAPIKeys(value string) []string {
	var keys []string
	for _, key := range strings.Split(value, ",") {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func requestKey(r *http.Request) string {
	if header := r.Header.Get("Authorization"); header != "" {
		scheme, token, ok := strings.Cut(header, " ")
		if ok && strings.EqualFold(scheme, "Bearer") {
			return strings.TrimSpace(token)
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

func validKey(digests [][32]byte, key string) bool {
	if key == "" {
		return false
	}

	digest := sha256.Sum256([]byte(key))
	match := 0
	for i := range digests {
		// Check every key so timing does not depend on which matched.
		match |= subtle.ConstantTimeCompare(digest[:], digests[i][:])
	}
	return match == 1
}
