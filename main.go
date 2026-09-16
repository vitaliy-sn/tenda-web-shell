package main

import (
	"embed"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

//go:embed web/*.html
var webFS embed.FS

func main() {
	listen := flag.String("listen", ":7777", "HTTP listen address")
	advertise := flag.String("advertise", "", "host:port the device can reach for the wget callback (default: auto-detect LAN IP + listen port)")
	flag.Parse()

	_, listenPort, err := splitHostPort(*listen)
	if err != nil {
		log.Fatalf("bad -listen: %v", err)
	}

	adv := *advertise
	if adv == "" {
		ip := detectLANIP()
		if ip == "" {
			ip = "127.0.0.1"
		}
		adv = fmt.Sprintf("%s:%s", ip, listenPort)
	}

	sender := NewSender("", adv) // target IP is set via the web UI (settarget)
	term := NewTerminal()
	hub := newHub()

	mux := http.NewServeMux()

	// Any POST (including /) is treated as command output from the device.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			out := strings.TrimRight(string(body), "\r\n")
			if out != "" {
				log.Printf("wget data from %s (%d bytes):\n%s", r.RemoteAddr, len(body), out)
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
			serveStatic(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("tenda-web-shell listening on %s (wake port %d, command port %d, advertise %s; set target IP in the web UI)", *listen, wakePort, commandPort, adv)
	if err := http.ListenAndServe(*listen, loggingMiddleware(mux)); err != nil {
		log.Fatal(err)
	}
}

func serveStatic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		path = "index.html"
	}
	data, err := webFS.ReadFile("web/" + path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(path, ".html"):
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Write(data)
}

// splitHostPort splits a listen address into host and port, defaulting the
// host to "" (all interfaces) when only a port is given.
func splitHostPort(addr string) (host, port string, err error) {
	if !strings.Contains(addr, ":") {
		return "", addr, nil
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", err
	}
	return h, p, nil
}

// detectLANIP picks the first non-loopback IPv4 address of this host.
func detectLANIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipnet.IP.To4()
		if ip == nil || ip.IsLoopback() {
			continue
		}
		return ip.String()
	}
	return ""
}
