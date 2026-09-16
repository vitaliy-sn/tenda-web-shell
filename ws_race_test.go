package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestConcurrentWrites reproduces the "concurrent write to websocket
// connection" panic: while the per-client writer goroutine streams terminal
// output, the hub broadcast writes status messages to the same connection.
// All writes are serialized by client.mu, so this must not panic.
func TestConcurrentWrites(t *testing.T) {
	term := NewTerminal()
	sender := NewSender("127.0.0.1", "127.0.0.1:9")
	h := newHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS(term, sender, h))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Drain a few initial messages (target + status).
	drain := func(n int) {
		for i := 0; i < n; i++ {
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			var m wsMessage
			if err := conn.ReadJSON(&m); err != nil {
				t.Fatalf("drain read: %v", err)
			}
		}
	}
	drain(2)

	// Stream terminal output (writer goroutine) and trigger broadcasts
	// (reader goroutine via cmd messages) concurrently.
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			term.Append("out-" + strconv.Itoa(i))
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := conn.WriteJSON(wsMessage{Type: "cmd", Data: "id"}); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// The connection should still be usable (no panic, no forced close).
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var m wsMessage
	if err := conn.ReadJSON(&m); err != nil {
		t.Fatalf("post-race read: %v", err)
	}
}
