package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSBridge(t *testing.T) {
	term := NewTerminal()
	sender := NewSender("127.0.0.1", 1, "127.0.0.1:1") // UDP target has no listener; send will fail -> status disconnected

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS(term, sender, newHub()))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	readMsg := func(t *testing.T, typ string) wsMessage {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		var m wsMessage
		if err := conn.ReadJSON(&m); err != nil {
			t.Fatalf("read: %v", err)
		}
		if m.Type != typ {
			t.Fatalf("want type %q got %q (data=%q)", typ, m.Type, m.Data)
		}
		return m
	}

	// Initial target, status, and prompt (empty history) messages.
	readMsg(t, "target")
	readMsg(t, "status")
	readMsg(t, "prompt")

	// Send a command; the server emits a newline then a fresh prompt before
	// the status update so output starts on a new line.
	if err := conn.WriteJSON(wsMessage{Type: "cmd", Data: "ls -l /etc"}); err != nil {
		t.Fatalf("write cmd: %v", err)
	}
	nl := readMsg(t, "out")
	if nl.Data != "\r\n" {
		t.Fatalf("want newline got %q", nl.Data)
	}
	prompt := readMsg(t, "out")
	if prompt.Data != "$ " {
		t.Fatalf("want prompt got %q", prompt.Data)
	}
	readMsg(t, "status")

	// Simulate device output arriving (direct Append) and confirm broadcast.
	term.Append("output-line-123")
	got := readMsg(t, "out")
	if got.Data != "output-line-123" {
		t.Fatalf("broadcast mismatch: %q", got.Data)
	}
}
