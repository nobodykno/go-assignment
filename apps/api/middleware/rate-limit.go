
package middleware

import (
    "net/http"
    "sync"
    "time"

    "github.com/gin-gonic/gin"
)

func RateLimit() gin.HandlerFunc {
    var mu sync.Mutex
    requests := make(map[string]int)
    windowStart := time.Now()

    return func(c *gin.Context) {
        ip := c.ClientIP()

        mu.Lock()

        if time.Since(windowStart) >= 60*time.Second {
            requests = make(map[string]int)
            windowStart = time.Now()
        }

        if requests[ip] >= 100 {
            mu.Unlock()

            c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
                "message": "Too many requests",
            })
            return
        }

        requests[ip]++
        mu.Unlock()

        c.Next()
    }
}