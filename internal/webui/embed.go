// Package webui holds generated static assets separately from the CLI graph.
package webui

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"path"
	"sort"
	"strings"
)

//go:embed assets
var embedded embed.FS

type Manifest struct {
	SourceRevision string            `json:"source_revision"`
	SourceTree     string            `json:"source_tree"`
	SourceDirty    bool              `json:"source_dirty"`
	InputDigest    string            `json:"input_digest"`
	LockDigest     string            `json:"lock_digest"`
	AssetDigest    string            `json:"asset_digest"`
	Node           string            `json:"node"`
	NPM            string            `json:"npm"`
	Go             string            `json:"go"`
	Files          map[string]string `json:"files"`
}
type Bundle struct {
	Manifest Manifest
	fs       fs.FS
}

func Load() (*Bundle, error) {
	f, e := fs.Sub(embedded, "assets")
	if e != nil {
		return nil, e
	}
	return validate(f)
}
func validate(f fs.FS) (*Bundle, error) {
	b, e := fs.ReadFile(f, "manifest.json")
	if e != nil || len(b) > 32768 {
		return nil, errors.New("missing embedded manifest")
	}
	var m Manifest
	if e = json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	if len(m.Files) < 2 || len(m.Files) > 32 || len(m.SourceRevision) != 40 || len(m.SourceTree) != 40 || len(m.InputDigest) != 64 || m.Files["index.html"] == "" {
		return nil, errors.New("invalid embedded manifest")
	}
	names := make([]string, 0, len(m.Files))
	total := 0
	for n, h := range m.Files {
		if !fs.ValidPath(n) || n == "manifest.json" || len(h) != 64 {
			return nil, errors.New("invalid asset path")
		}
		b, e := fs.ReadFile(f, n)
		if e != nil || len(b) > 4<<20 {
			return nil, errors.New("missing or oversized asset")
		}
		total += len(b)
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != h {
			return nil, errors.New("asset digest mismatch")
		}
		names = append(names, n)
	}
	if total > 8<<20 {
		return nil, errors.New("asset budget exceeded")
	}
	sort.Strings(names)
	aggregate := sha256.New()
	for _, n := range names {
		_, _ = aggregate.Write([]byte(n + "\x00" + m.Files[n] + "\n"))
	}
	if hex.EncodeToString(aggregate.Sum(nil)) != m.AssetDigest {
		return nil, errors.New("asset manifest mismatch")
	}
	return &Bundle{Manifest: m, fs: f}, nil
}
func (b *Bundle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		name = "index.html"
	}
	if path.Clean(name) != name || b.Manifest.Files[name] == "" {
		http.NotFound(w, r)
		return
	}
	raw, e := fs.ReadFile(b.fs, name)
	if e != nil {
		http.Error(w, "embedded asset unavailable", 500)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	etag := `"` + b.Manifest.Files[name] + `"`
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	switch path.Ext(name) {
	case ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case ".js":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	case ".css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	}
	if r.Method != "HEAD" {
		_, _ = w.Write(raw)
	}
}
