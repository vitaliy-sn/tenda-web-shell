package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newFakeDevice starts a UDP listener that replies "format failed" to any
// datagram, emulating the target device for connect probes.
func newFakeDevice(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	a := pc.LocalAddr().(*net.UDPAddr)
	go func() {
		buf := make([]byte, 65536)
		for {
			n, ra, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			pc.WriteTo([]byte(`{"result":"format failed"}`), ra)
			_ = n
		}
	}()
	return a.String(), func() { pc.Close() }
}

func TestSetTargetAndConnect(t *testing.T) {
	fakeAddr, cleanup := newFakeDevice(t)
	defer cleanup()

	fakeHost, fakePortStr, err := net.SplitHostPort(fakeAddr)
	if err != nil {
		t.Fatalf("split fake addr: %v", err)
	}
	fakePort, err := strconv.Atoi(fakePortStr)
	if err != nil {
		t.Fatalf("atoi fake port: %v", err)
	}

	term := NewTerminal()
	sender := NewSender(fakeHost, "127.0.0.1:9")
	// The fake device listens on a single port; point both the wake and command
	// phases at it so the two-phase connect sequence succeeds.
	sender.SetWakePort(fakePort)
	sender.SetCommandPort(fakePort)
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

	read := func(typ string) wsMessage {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(4 * time.Second))
		var m wsMessage
		if err := conn.ReadJSON(&m); err != nil {
			t.Fatalf("read: %v", err)
		}
		if m.Type != typ {
			t.Fatalf("want type %q got %q (data=%q)", typ, m.Type, m.Data)
		}
		return m
	}

	// Initial target + status.
	read("target")
	read("status")

	// Repoint at the fake device -> status drops to disconnected. The target is
	// IP-only now (ports are fixed by protocol constants), so send just the host.
	if err := conn.WriteJSON(wsMessage{Type: "settarget", Data: fakeHost}); err != nil {
		t.Fatalf("write settarget: %v", err)
	}
	st := read("status")
	if st.Data != "disconnected" {
		t.Fatalf("after settarget want disconnected got %q", st.Data)
	}

	// Connect -> probe succeeds against the fake device.
	if err := conn.WriteJSON(wsMessage{Type: "connect"}); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	st = read("status")
	if st.Data != "connected" {
		t.Fatalf("after connect want connected got %q", st.Data)
	}

	// Disconnect.
	if err := conn.WriteJSON(wsMessage{Type: "disconnect"}); err != nil {
		t.Fatalf("write disconnect: %v", err)
	}
	st = read("status")
	if st.Data != "disconnected" {
		t.Fatalf("after disconnect want disconnected got %q", st.Data)
	}
}
