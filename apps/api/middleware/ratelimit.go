package middleware

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter applies a token bucket per client IP. Unlike a fixed
// window it refills continuously, so clients cannot burst twice across
// a window boundary, and each client has its own lock-free limiter.
//
// Limits are per API instance; running several replicas needs a shared
// store such as Redis.
type RateLimiter struct {
	limit   rate.Limit
	burst   int
	proxies TrustedProxies

	clients sync.Map // client IP -> *clientLimiter
}

type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen atomicTime
}

// idleClientTTL is how long an idle client's bucket is kept.
const idleClientTTL = 5 * time.Minute

// NewRateLimiter allows perMinute requests per client on average with
// bursts of up to burst requests. Idle clients are forgotten in the
// background until ctx is cancelled.
func NewRateLimiter(
	ctx context.Context,
	perMinute int,
	burst int,
	proxies TrustedProxies,
) *RateLimiter {
	if perMinute < 1 {
		perMinute = 1
	}
	if burst < 1 {
		burst = 1
	}

	limiter := &RateLimiter{
		limit:   rate.Limit(float64(perMinute) / 60),
		burst:   burst,
		proxies: proxies,
	}

	go limiter.cleanup(ctx)

	return limiter
}

// Middleware rejects clients over their limit with 429 and Retry-After.
func (l *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		client := l.client(l.proxies.ClientIP(r))

		reservation := client.limiter.Reserve()
		if delay := reservation.Delay(); delay > 0 {
			reservation.Cancel()

			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(delay.Seconds()))))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "too many requests",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (l *RateLimiter) client(ip string) *clientLimiter {
	now := time.Now()

	if existing, ok := l.clients.Load(ip); ok {
		client := existing.(*clientLimiter)
		client.lastSeen.Store(now)
		return client
	}

	client := &clientLimiter{limiter: rate.NewLimiter(l.limit, l.burst)}
	client.lastSeen.Store(now)

	actual, _ := l.clients.LoadOrStore(ip, client)
	return actual.(*clientLimiter)
}

func (l *RateLimiter) cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			l.clients.Range(func(key, value any) bool {
				if now.Sub(value.(*clientLimiter).lastSeen.Load()) > idleClientTTL {
					l.clients.Delete(key)
				}
				return true
			})
		}
	}
}

// atomicTime stores a time.Time safely across goroutines.
type atomicTime struct {
	mu   sync.Mutex
	time time.Time
}

func (t *atomicTime) Store(value time.Time) {
	t.mu.Lock()
	t.time = value
	t.mu.Unlock()
}

func (t *atomicTime) Load() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.time
}
