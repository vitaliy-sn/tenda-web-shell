package main

import (
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type wsMessage struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

// client wraps a websocket connection with a mutex that serializes all writes.
// gorilla/websocket does not allow concurrent writes to the same connection,
// and we have multiple writers (the per-client output goroutine plus the hub
// broadcast), so every WriteJSON must hold c.mu.
type client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *client) send(m wsMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	return c.conn.WriteJSON(m)
}

type hub struct {
	mu    sync.Mutex
	conns map[*client]struct{}
}

func newHub() *hub { return &hub{conns: make(map[*client]struct{})} }

func (h *hub) add(c *client) {
	h.mu.Lock()
	h.conns[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
}

// broadcast sends a message to every connected client.
func (h *hub) broadcast(m wsMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.conns {
		if err := c.send(m); err != nil {
			delete(h.conns, c)
			c.conn.Close()
		}
	}
}

// handleWS upgrades the connection and bridges the terminal to the client.
func handleWS(term *Terminal, sender *Sender, h *hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("ws upgrade: %v", err)
			return
		}
		c := &client{conn: conn}
		h.add(c)
		defer func() {
			h.remove(c)
			conn.Close()
		}()

		ch, hist := term.Subscribe()
		defer term.Unsubscribe(ch)

		// Replay history and current status.
		if len(hist) > 0 {
			c.send(wsMessage{Type: "out", Data: joinLines(hist)})
		}
		c.send(wsMessage{Type: "target", Data: sender.Target()})
		c.send(wsMessage{Type: "status", Data: sender.Status()})
		if len(hist) == 0 {
			c.send(wsMessage{Type: "prompt"})
		}

		// Reader goroutine: commands from the browser.
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var msg wsMessage
				if err := jsonUnmarshal(data, &msg); err != nil {
					continue
				}
				switch msg.Type {
				case "cmd":
					cmd := strings.TrimRight(msg.Data, "\r\n")
					if cmd == "" {
						continue
					}
					// Move to a fresh line so command output starts below the
					// typed command, then re-show the prompt for the next input.
					term.Append("\r\n")
					term.Append("$ ")
					if err := sender.Send(cmd); err != nil {
						term.Append("[send error: " + err.Error() + "]")
					}
					h.broadcast(wsMessage{Type: "status", Data: sender.Status()})
				case "connect":
					ok := sender.Connect()
					st := "disconnected"
					if ok {
						st = "connected"
					}
					h.broadcast(wsMessage{Type: "status", Data: st})
				case "disconnect":
					sender.Disconnect()
					h.broadcast(wsMessage{Type: "status", Data: sender.Status()})
				case "clear":
					term.Clear()
					h.broadcast(wsMessage{Type: "clear"})
				case "settarget":
					// data is "host:port"
					host, portStr, err := net.SplitHostPort(msg.Data)
					if err != nil {
						term.Append("[bad target: "+msg.Data+"]")
						continue
					}
					port, err := strconv.Atoi(portStr)
					if err != nil || port <= 0 {
						term.Append("[bad port in target: "+msg.Data+"]")
						continue
					}
					sender.SetTarget(host, port)
					h.broadcast(wsMessage{Type: "status", Data: sender.Status()})
				}
			}
		}()

		// Writer goroutine: terminal output to the browser.
		go func() {
			for line := range ch {
				if err := c.send(wsMessage{Type: "out", Data: line}); err != nil {
					return
				}
			}
		}()

		<-done
	}
}
