package webui

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"testing/fstest"
)

func TestProofEmbeddedManifest(t *testing.T) {
	h := func(b string) string { s := sha256.Sum256([]byte(b)); return hex.EncodeToString(s[:]) }
	f := fstest.MapFS{"index.html": {Data: []byte("html")}, "assets/main-123.js": {Data: []byte("script")}}
	m := Manifest{SourceRevision: strings.Repeat("1", 40), SourceTree: strings.Repeat("2", 40), InputDigest: strings.Repeat("3", 64), Files: map[string]string{"index.html": h("html"), "assets/main-123.js": h("script")}}
	m.AssetDigest = h("assets/main-123.js\x00" + h("script") + "\nindex.html\x00" + h("html") + "\n")
	raw, _ := json.Marshal(m)
	f["manifest.json"] = &fstest.MapFile{Data: raw}
	if _, e := validate(f); e != nil {
		t.Fatal(e)
	}
	f["assets/main-123.js"].Data = []byte("corrupt")
	if _, e := validate(f); e == nil {
		t.Fatal("corrupt accepted")
	}
	delete(f, "manifest.json")
	if _, e := validate(f); e == nil {
		t.Fatal("missing accepted")
	}
	t.Log("PROOF_EVENT embedded_manifest_missing_corrupt_refusal")
}
