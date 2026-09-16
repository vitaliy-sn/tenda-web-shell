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
	sender := NewSender("127.0.0.1", "127.0.0.1:1") // UDP target has no listener; send will fail -> status disconnected

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

	// Initial target and status messages.
	readMsg(t, "target")
	readMsg(t, "status")

	// Send a command; the server stores it in history and broadcasts it back as
	// an "out" line before the status update.
	if err := conn.WriteJSON(wsMessage{Type: "cmd", Data: "ls -l /etc"}); err != nil {
		t.Fatalf("write cmd: %v", err)
	}
	cmdEcho := readMsg(t, "out")
	if cmdEcho.Data != "$ ls -l /etc" {
		t.Fatalf("want command echo %q got %q", "$ ls -l /etc", cmdEcho.Data)
	}
	readMsg(t, "status")

	// Simulate device output arriving (direct Append) and confirm broadcast.
	term.Append("output-line-123")
	got := readMsg(t, "out")
	if got.Data != "output-line-123" {
		t.Fatalf("broadcast mismatch: %q", got.Data)
	}
}
