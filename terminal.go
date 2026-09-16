package main

import (
	"strings"
	"sync"
)

// Terminal is a thread-safe line buffer that fans out every appended line to
// all currently subscribed WebSocket clients and replays history to new ones.
type Terminal struct {
	mu      sync.Mutex
	lines   []string
	sub     map[chan string]struct{}
}

func NewTerminal() *Terminal {
	return &Terminal{sub: make(map[chan string]struct{})}
}

// Subscribe registers a new client and returns its channel plus the existing
// history so the client can catch up.
func (t *Terminal) Subscribe() (<-chan string, []string) {
	t.mu.Lock()
	hist := make([]string, len(t.lines))
	copy(hist, t.lines)
	ch := make(chan string, 256)
	t.sub[ch] = struct{}{}
	t.mu.Unlock()
	return ch, hist
}

// Unsubscribe removes a client and closes its channel.
func (t *Terminal) Unsubscribe(ch <-chan string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for c := range t.sub {
		if c == ch {
			delete(t.sub, c)
			close(c)
			return
		}
	}
}

// Append stores a line and broadcasts it to every subscriber.
func (t *Terminal) Append(line string) {
	t.mu.Lock()
	t.lines = append(t.lines, line)
	if len(t.lines) > 99999 {
		t.lines = t.lines[len(t.lines)-99999:]
	}
	for ch := range t.sub {
		select {
		case ch <- line:
		default:
			// Slow consumer: drop rather than block the writer.
		}
	}
	t.mu.Unlock()
}

// Clear wipes the stored history so new clients join with an empty terminal.
func (t *Terminal) Clear() {
	t.mu.Lock()
	t.lines = nil
	t.mu.Unlock()
}

// History returns a copy of all stored lines.
func (t *Terminal) History() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.lines))
	copy(out, t.lines)
	return out
}

// Render joins history into a single string for replaying to a new client.
func (t *Terminal) Render() string {
	return strings.Join(t.History(), "\r\n")
}
