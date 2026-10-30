package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"
)

func RateLimit(next http.Handler) http.Handler {
	var mu sync.Mutex
	requests := make(map[string]int)
	windowStart := time.Now()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		mu.Lock()

		// Reset the request counts every 60 seconds.
		if time.Since(windowStart) >= 60*time.Second {
			requests = make(map[string]int)
			windowStart = time.Now()
		}

		if requests[ip] >= 100 {
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)

			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": "Too many requests",
			})
			return
		}

		requests[ip]++
		mu.Unlock()

		// Continue to the next handler.
		next.ServeHTTP(w, r)
	})
}
