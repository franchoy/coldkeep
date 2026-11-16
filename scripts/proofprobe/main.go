// Command proofprobe is a private, connect-only Linux qualification helper.
// It is never linked into or exposed by the product daemon.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"syscall"
	"time"
)

const deadlineMS = 1000

type result struct {
	Target     string `json:"target"`
	Stage      string `json:"stage"`
	Outcome    string `json:"outcome"`
	DeadlineMS int    `json:"deadline_ms"`
	ElapsedMS  int64  `json:"elapsed_ms"`
	Errno      int    `json:"errno"`
	Error      string `json:"error"`
}

func probe(target string) int {
	r := result{Target: target, Stage: "connect", Outcome: "invalid", DeadlineMS: deadlineMS}
	address, err := netip.ParseAddrPort(target)
	if err != nil || address.Port() == 0 || address.Addr().IsUnspecified() || address.Addr().Zone() != "" {
		r.Error = "numeric endpoint required"
	} else {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", target, deadlineMS*time.Millisecond)
		r.ElapsedMS = time.Since(start).Milliseconds()
		if err == nil {
			// Establishment is decisive. No read, HTTP, redirect or retry follows.
			r.Outcome = "connected"
			_ = json.NewEncoder(os.Stdout).Encode(r)
			_ = conn.Close()
			return 0
		}
		r.Error = err.Error()
		var errno syscall.Errno
		if errors.As(err, &errno) {
			r.Errno = int(errno)
		}
		var netErr net.Error
		switch {
		case errors.Is(err, syscall.ECONNREFUSED):
			r.Outcome = "refused"
		case errors.As(err, &netErr) && netErr.Timeout():
			r.Outcome = "timeout"
		case errors.Is(err, syscall.ENETUNREACH), errors.Is(err, syscall.EHOSTUNREACH):
			r.Outcome = "no_route"
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
	if r.Outcome == "invalid" {
		return 2
	}
	return 0
}

// The one container hosting these owned controls is also the probe namespace
// for every real endpoint. Nothing is published to the host. Its process logs
// record accepts independently of the probing subprocess's result stream.
func fixture() error {
	var mu sync.Mutex
	emit := func(v any) { mu.Lock(); defer mu.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }
	for i, name := range []string{"http_200", "http_403", "http_404", "stalled"} {
		ln, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", 18770+i))
		if err != nil {
			return err
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				emit(map[string]any{"event": "accepted", "control": name, "peer": conn.RemoteAddr().String()})
				// The fixed control campaign makes at most two connections per port.
				go func() {
					defer func() { _ = conn.Close() }()
					if name == "stalled" {
						time.Sleep(3 * time.Second)
						return
					}
					_ = conn.SetDeadline(time.Now().Add(time.Second))
					var request [1024]byte
					n, _ := conn.Read(request[:])
					if n > 0 {
						emit(map[string]any{"event": "request", "control": name, "request": string(request[:n])})
					}
					status := map[string]int{"http_200": 200, "http_403": 403, "http_404": 404}[name]
					_, _ = fmt.Fprintf(conn, "HTTP/1.1 %d Control\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status)
				}()
			}
		}()
	}
	closed, err := net.Listen("tcp4", "127.0.0.1:18774")
	if err != nil {
		return err
	}
	if err := closed.Close(); err != nil {
		return err
	}
	emit(map[string]any{"event": "ready", "closed_target": "127.0.0.1:18774"})
	select {}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "fixture" {
		if err := fixture(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	} else if len(os.Args) == 3 && os.Args[1] == "probe" {
		os.Exit(probe(os.Args[2]))
	} else {
		fmt.Fprintln(os.Stderr, "expected fixture or probe NUMERIC_ENDPOINT")
		os.Exit(2)
	}
}
