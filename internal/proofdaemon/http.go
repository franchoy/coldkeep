package proofdaemon

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/version"
	"github.com/franchoy/coldkeep/v2/internal/webui"
)

const MaxBody = 4096
const PageSize = 32
const MaxConnections = 16
const MaxRequests = 8
const BootstrapTTL = 2 * time.Minute
const SessionTTL = 30 * time.Minute

type Problem struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}
type BootstrapRequest struct {
	Secret string `json:"secret"`
}
type SessionResult struct {
	Token          string `json:"token"`
	ExpiresSeconds int    `json:"expires_seconds"`
}
type HistoryEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size string `json:"size"`
}
type HistoryResult struct {
	Entries    []HistoryEntry `json:"entries"`
	NextCursor string         `json:"next_cursor"`
}
type RestoreRequest struct {
	EntryID string `json:"entry_id"`
}
type RestoreResult struct {
	Outcome string `json:"outcome"`
	Code    string `json:"code"`
	Detail  string `json:"detail"`
}
type Capabilities struct {
	History           bool `json:"history"`
	Restore           bool `json:"restore"`
	Search            bool `json:"search"`
	Jobs              bool `json:"jobs"`
	LiveGC            bool `json:"live_gc"`
	DurableAcceptance bool `json:"durable_acceptance"`
}
type Status struct {
	Application    string `json:"application"`
	API            string `json:"api"`
	Profile        string `json:"profile"`
	Backend        string `json:"backend"`
	Destination    string `json:"destination"`
	SourceRevision string `json:"source_revision"`
	SourceTree     string `json:"source_tree"`
	SourceDirty    bool   `json:"source_dirty"`
	AssetDigest    string `json:"asset_digest"`
}

type Service struct {
	runtime                          *Runtime
	assets                           *webui.Bundle
	host, origin, bootstrap, session string
	bootstrapUntil, sessionUntil     time.Time
	bootstrapFile                    string
	authMu                           sync.Mutex
	requests                         chan struct{}
	cursors                          map[string]int
	now                              func() time.Time
}

func NewService(r *Runtime, b *webui.Bundle) (*Service, error) {
	u, e := url.Parse(r.cfg.PublicURL)
	if e != nil {
		return nil, ErrFixture
	}
	s := &Service{runtime: r, assets: b, host: u.Host, origin: r.cfg.PublicURL, bootstrap: randomToken(), bootstrapUntil: time.Now().Add(BootstrapTTL), requests: make(chan struct{}, MaxRequests), cursors: map[string]int{}, now: time.Now}
	for i := PageSize; i < len(r.entries); i += PageSize {
		s.cursors[randomToken()] = i
	}
	s.bootstrapFile = filepath.Join(r.cfg.RuntimeDir, "bootstrap")
	f, e := os.OpenFile(s.bootstrapFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, errors.New("bootstrap_custody_failed")
	}
	_, e = f.WriteString(s.bootstrap)
	e = errors.Join(e, f.Sync(), f.Close())
	if e != nil {
		_ = os.Remove(s.bootstrapFile)
		return nil, errors.New("bootstrap_custody_failed")
	}
	return s, nil
}
func (s *Service) Close() error {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	s.bootstrap = ""
	s.session = ""
	e := os.Remove(s.bootstrapFile)
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, Problem{code, detail})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		problem(w, 415, "json_required", "Use application/json.")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBody)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		problem(w, 400, "invalid_body", "A bounded JSON object with the declared fields is required.")
		return false
	}
	return true
}
func secureEqual(a, b string) bool {
	return len(a) == 64 && len(b) == 64 && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	if r.Host != s.host {
		problem(w, 403, "host_refused", "Unexpected host.")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != s.origin {
		problem(w, 403, "origin_refused", "Cross-origin requests are refused.")
		return
	}
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		problem(w, 403, "origin_refused", "Cross-origin requests are refused.")
		return
	}
	if len(r.RequestURI) > 2048 {
		problem(w, 414, "request_limit", "Request target exceeds the proof limit.")
		return
	}
	select {
	case s.requests <- struct{}{}:
		defer func() { <-s.requests }()
	default:
		problem(w, 503, "request_busy", "Request capacity is full.")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api") {
		if r.Method != "GET" && r.Header.Get("Origin") != s.origin {
			problem(w, 403, "origin_required", "The configured origin is required for mutations.")
			return
		}
		s.api(w, r)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		problem(w, 405, "method_refused", "Use GET or HEAD for assets.")
		return
	}
	s.assets.ServeHTTP(w, r)
}
func (s *Service) authenticated(r *http.Request) bool {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	return s.now().Before(s.sessionUntil) && secureEqual(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), s.session) && strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")
}
func (s *Service) api(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/v1/session" {
		if r.Method != "POST" {
			problem(w, 405, "method_refused", "Use POST.")
			return
		}
		s.exchange(w, r)
		return
	}
	if !s.authenticated(r) {
		problem(w, 401, "authentication_required", "Authenticate this internal fixture session.")
		return
	}
	if r.Method != "GET" && (r.Method != "POST" || r.URL.Path != "/api/v1/restore") {
		problem(w, 405, "method_refused", "Method is unsupported.")
		return
	}
	switch r.URL.Path {
	case "/api/v1/status":
		m := s.assets.Manifest
		writeJSON(w, 200, Status{version.String(), "v1", "internal-proof-only", s.runtime.cfg.Backend, "Fresh operator-configured fixture output; original relative names; no overwrite.", m.SourceRevision, m.SourceTree, m.SourceDirty, m.AssetDigest})
	case "/api/v1/capabilities":
		writeJSON(w, 200, Capabilities{History: true, Restore: true})
	case "/api/v1/history":
		s.history(w, r)
	case "/api/v1/restore":
		if r.Method != "POST" {
			problem(w, 405, "method_refused", "Use POST.")
			return
		}
		var req RestoreRequest
		if !decode(w, r, &req) {
			return
		}
		if len(req.EntryID) != 64 {
			problem(w, 400, "invalid_entry", "Select a history entry.")
			return
		}
		result := s.runtime.restore(req.EntryID)
		status := 200
		if result.Outcome == "refused" {
			status = 409
		}
		if result.Outcome == "failed_or_partial" {
			status = 422
		}
		writeJSON(w, status, result)
	case "/api/v1/search", "/api/v1/jobs":
		problem(w, 501, "unsupported_capability", "This operation is not implemented by the internal proof.")
	default:
		problem(w, 404, "route_not_found", "Unknown API route.")
	}
}
func (s *Service) exchange(w http.ResponseWriter, r *http.Request) {
	var req BootstrapRequest
	if !decode(w, r, &req) {
		return
	}
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if !s.now().Before(s.bootstrapUntil) || !secureEqual(req.Secret, s.bootstrap) {
		problem(w, 401, "bootstrap_refused", "Bootstrap is invalid, expired, or already used.")
		return
	}
	if e := os.Remove(s.bootstrapFile); e != nil {
		problem(w, 500, "bootstrap_custody_failed", "Cannot retire bootstrap material.")
		return
	}
	s.bootstrap = ""
	s.session = randomToken()
	s.sessionUntil = s.now().Add(SessionTTL)
	writeJSON(w, 200, SessionResult{s.session, int(SessionTTL.Seconds())})
}
func (s *Service) history(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for k, v := range q {
		if (k != "cursor" && k != "limit") || len(v) != 1 {
			problem(w, 400, "invalid_page", "Use one bounded cursor and limit.")
			return
		}
	}
	limit := PageSize
	if q.Has("limit") {
		n, e := strconv.Atoi(q.Get("limit"))
		if e != nil || n != PageSize {
			problem(w, 400, "invalid_page", "The proof page size is fixed at 32.")
			return
		}
		limit = n
	}
	start := 0
	if q.Has("cursor") {
		var ok bool
		start, ok = s.cursors[q.Get("cursor")]
		if !ok {
			problem(w, 400, "invalid_cursor", "Unknown session cursor.")
			return
		}
	}
	end := min(start+limit, len(s.runtime.entries))
	out := HistoryResult{Entries: []HistoryEntry{}}
	for _, e := range s.runtime.entries[start:end] {
		out.Entries = append(out.Entries, HistoryEntry(e))
	}
	if end < len(s.runtime.entries) {
		for c, i := range s.cursors {
			if i == end {
				out.NextCursor = c
				break
			}
		}
	}
	writeJSON(w, 200, out)
}

// The connection cap is applied before net/http creates handler goroutines.
// Kernel backlog is finite; no application queue or detached job goroutines.
type limitedListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}
type limitedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitedConn) Close() error { e := c.Conn.Close(); c.once.Do(c.release); return e }
func (l *limitedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, e := l.Listener.Accept()
	if e != nil {
		<-l.slots
		return nil, e
	}
	return &limitedConn{Conn: c, release: func() { <-l.slots }}, nil
}
func (l *limitedListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}

// Run stops admission on cancellation, drains every synchronous operation and
// handler, closes credentials/catalog, then releases the repository lease.
// Client disconnect does not cancel restore; lost responses have unknown outcome.
func Run(ctx context.Context, c Config, b *webui.Bundle) (outErr error) {
	r, e := Open(c)
	if e != nil {
		return e
	}
	defer func() { outErr = errors.Join(outErr, r.Close()) }()
	service, e := NewService(r, b)
	if e != nil {
		return e
	}
	defer func() { outErr = errors.Join(outErr, service.Close()) }()
	listener, e := net.Listen("tcp", c.Listen)
	if e != nil {
		return errors.New("listen_failed")
	}
	bounded := &limitedListener{Listener: listener, slots: make(chan struct{}, MaxConnections), done: make(chan struct{})}
	server := &http.Server{Handler: service, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second, WriteTimeout: 2 * time.Minute, MaxHeaderBytes: 8192}
	done := make(chan error, 1)
	go func() { done <- server.Serve(bounded) }()
	select {
	case <-ctx.Done():
		// No deadline is mistaken for completed cleanup. Bounded fixture work is
		// drained and actual joins retain ownership even if the OS stalls.
		e = server.Shutdown(context.Background())
		serveErr := <-done
		if !errors.Is(serveErr, http.ErrServerClosed) {
			e = errors.Join(e, serveErr)
		}
		return e
	case e = <-done:
		joined := server.Shutdown(context.Background())
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		return errors.Join(e, joined)
	}
}
