package main

import (
	"log"
	"net/http"
	"time"
)

// statusRecorder wraps ResponseWriter to capture the status code and response
// size for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	size   int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.size += n
	return n, err
}

// loggingMiddleware logs every HTTP request: method, path, client IP, status,
// response size and duration. WebSocket upgrades are logged once at start.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		if r.URL.Path == "/ws" && r.Method == http.MethodGet {
			// WebSocket: log the upgrade, then hand off (the connection outlives
			// this handler call, so we don't wrap its lifetime).
			log.Printf("http %s %s from %s (websocket upgrade)", r.Method, r.URL.Path, clientIP(r))
			next.ServeHTTP(w, r)
			return
		}

		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		log.Printf("http %s %s from %s -> %d (%d bytes, %s)",
			r.Method, r.URL.RequestURI(), clientIP(r), rec.status, rec.size, time.Since(start).Round(time.Microsecond))
	})
}

// clientIP extracts the best-effort client address from the request.
func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if i := lastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	return host
}

func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}
