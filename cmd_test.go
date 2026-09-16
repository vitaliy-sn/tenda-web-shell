package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var advertiseRe = regexp.MustCompile(`http://([^/]+)/`)

// newFakeShellDevice starts a UDP listener that emulates the target device for
// a full connect -> command -> response cycle:
//
//   - probe datagrams (anything without "PTEfuseSet", e.g. "123") get a
//     {"result":"format failed"} reply so Sender.Connect succeeds;
//   - command payloads (containing "PTEfuseSet") are NOT answered over UDP
//     (answering would be eaten by the sender's background reader and hang
//     Connect). Instead the device "executes" the command and POSTs its output
//     back to the advertise address parsed from the payload, mimicking the
//     wget --post-data callback.
func newFakeShellDevice(t *testing.T, output string) (addr string, cleanup func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	a := pc.LocalAddr().(*net.UDPAddr)
	go func() {
		buf := make([]byte, 65536)
		for {
			n, ra, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			payload := string(buf[:n])
			if !strings.Contains(payload, "PTEfuseSet") {
				// Probe: reply to the sender so Connect() can complete.
				pc.WriteTo([]byte(`{"result":"format failed"}`), ra)
				continue
			}
			m := advertiseRe.FindStringSubmatch(payload)
			if m == nil {
				t.Logf("fake device: no advertise in payload %q", payload)
				return
			}
			resp, err := http.Post("http://"+m[1]+"/", "application/x-www-form-urlencoded", strings.NewReader(output))
			if err != nil {
				t.Logf("fake device: callback post: %v", err)
				return
			}
			resp.Body.Close()
		}
	}()
	return a.String(), func() { pc.Close() }
}

// TestConnectCommandAndResponse drives the full loop: connect to the (fake)
// device, send "ls /etc/passwd", and verify the command output "/etc/passwd"
// arrives at the WebSocket client via the wget callback POST.
func TestConnectCommandAndResponse(t *testing.T) {
	fakeAddr, cleanup := newFakeShellDevice(t, "/etc/passwd\n")
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
	hub := newHub()
	sender := NewSender(fakeHost, "placeholder") // advertise set below
	// The fake device listens on a single port; point both the wake and command
	// phases at it so the two-phase connect sequence succeeds.
	sender.SetWakePort(fakePort)
	sender.SetCommandPort(fakePort)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			out := strings.TrimRight(string(body), "\r\n")
			if out != "" {
				for _, line := range strings.Split(out, "\n") {
					term.Append(line)
				}
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if r.URL.Path == "/ws" {
				handleWS(term, sender, hub)(w, r)
				return
			}
			http.NotFound(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// advertise = the httptest server, where the device's wget callback lands.
	sender.advertise = strings.TrimPrefix(ts.URL, "http://")

	wsURL := "ws://" + strings.TrimPrefix(ts.URL, "http://") + "/ws"
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(wsURL, nil)
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
		t.Logf("WS <- type=%q data=%q", m.Type, m.Data)
		if m.Type != typ {
			t.Fatalf("want type %q got %q (data=%q)", typ, m.Type, m.Data)
		}
		return m
	}

	// Initial target + status.
	read("target")
	read("status")

	// Connect: probe succeeds against the fake device.
	if err := conn.WriteJSON(wsMessage{Type: "connect"}); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	st := read("status")
	if st.Data != "connected" {
		t.Fatalf("after connect want connected got %q", st.Data)
	}
	t.Logf("connected to device (probe OK)")

	// Send the command. The server stores it in history and broadcasts it back
	// as an "out" line ("$ ls /etc/passwd"), then reports status, then the
	// device's callback POST delivers the actual output.
	if err := conn.WriteJSON(wsMessage{Type: "cmd", Data: "ls /etc/passwd"}); err != nil {
		t.Fatalf("write cmd: %v", err)
	}
	t.Logf("WS -> cmd \"ls /etc/passwd\"")

	// The stored command is echoed back first.
	cmdEcho := read("out")
	if cmdEcho.Data != "$ ls /etc/passwd" {
		t.Fatalf("want command echo %q got %q", "$ ls /etc/passwd", cmdEcho.Data)
	}

	read("status")

	// The fake device POSTs the command output back; the server appends it to
	// the terminal and broadcasts it. Expect exactly "/etc/passwd".
	got := read("out")
	if got.Data != "/etc/passwd" {
		t.Fatalf("want command output %q got %q", "/etc/passwd", got.Data)
	}
	t.Logf("PASS: command output received = %q (expected %q)", got.Data, "/etc/passwd")
}
