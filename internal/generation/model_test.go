package generation

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func selector(parts ...[]byte) Selector {
	s := Selector{Kind: "posix-bytes-base64"}
	for _, p := range parts {
		s.Components = append(s.Components, base64.StdEncoding.EncodeToString(p))
	}
	return s
}
func TestExactNames(t *testing.T) {
	names := [][]byte{[]byte("A"), []byte("a"), []byte(" x"), []byte("x "), []byte("é"), []byte("é"), []byte("\\"), []byte("\n"), []byte("�"), {0xff, 0xfe}, bytes.Repeat([]byte{0xfd}, 255)}
	seen := map[string]bool{}
	for _, name := range names {
		s := selector(name)
		raw, _ := json.Marshal(s)
		got, e := DecodeSelector(raw)
		if e != nil {
			t.Fatal(e)
		}
		parts, e := got.Bytes()
		if e != nil || !bytes.Equal(parts[0], name) || seen[string(parts[0])] {
			t.Fatal("lossy alias", name, e)
		}
		seen[string(parts[0])] = true
	}
}
func TestSelectorRefusals(t *testing.T) {
	cases := map[string][]byte{
		"empty":          []byte(`{"kind":"posix-bytes-base64","components":[]}`),
		"duplicate":      []byte(`{"kind":"posix-bytes-base64","kind":"posix-bytes-base64","components":["YQ=="]}`),
		"unknown":        []byte(`{"kind":"posix-bytes-base64","display":"a","components":["YQ=="]}`),
		"case-alias":     []byte(`{"Kind":"posix-bytes-base64","components":["YQ=="]}`),
		"surrogate":      []byte(`{"kind":"posix-bytes-base64","components":["\ud800"]}`),
		"low-surrogate":  []byte(`{"kind":"posix-bytes-base64","components":["\udfff"]}`),
		"native-json":    append([]byte(`{"kind":"posix-bytes-base64","components":["`), append([]byte{0xff}, []byte(`"]}`)...)...),
		"base64-tail":    []byte(`{"kind":"posix-bytes-base64","components":["YR=="]}`),
		"base64-newline": []byte(`{"kind":"posix-bytes-base64","components":["YQ==\n"]}`),
		"trailing":       []byte(`{"kind":"posix-bytes-base64","components":["YQ=="]} {}`),
		"padding":        []byte(strings.Repeat(" ", SelectorBytes+1)),
	}
	for name, part := range map[string][]byte{"dot": []byte("."), "dotdot": []byte(".."), "slash": []byte("a/b"), "absolute": []byte("/a"), "nul": {0}, "component-over": bytes.Repeat([]byte("a"), 256)} {
		cases[name], _ = json.Marshal(selector(part))
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := DecodeSelector(raw); e == nil {
				t.Fatal("admitted invalid selector")
			}
		})
	}
}
func TestSelectorLimits(t *testing.T) {
	depth := selector()
	for range 128 {
		depth.Components = append(depth.Components, "YQ==")
	}
	if _, e := depth.Bytes(); e != nil {
		t.Fatal(e)
	}
	depth.Components = append(depth.Components, "YQ==")
	if _, e := depth.Bytes(); e == nil {
		t.Fatal("depth overrun")
	}
	path := selector()
	for range 12 {
		path.Components = append(path.Components, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 255)))
	}
	path.Components = append(path.Components, "YQ==")
	if _, e := path.Bytes(); e == nil {
		t.Fatal("3073 bytes admitted")
	}
	path.Components[11] = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 254))
	if _, e := path.Bytes(); e != nil {
		t.Fatal("3072 bytes", e)
	}
}
func TestPolicyGrammarAndImmutability(t *testing.T) {
	p := Policy{Version: 1, Intent: "capture", Scope: "selected", SelectionVersion: 1, Rules: []Rule{{Action: "include", Selection: selector([]byte(" A "))}}}
	b, e := p.Canonical()
	if e != nil {
		t.Fatal(e)
	}
	original := append([]byte(nil), b...)
	digest := Digest(b)
	p.Intent = "archival-import"
	next, e := p.Canonical()
	if e != nil || bytes.Equal(b, next) || digest == Digest(next) || !bytes.Equal(original, b) {
		t.Fatal("policy revision")
	}
	for _, bad := range [][]byte{append(b, ' '), bytes.Replace(b, []byte(`"version":1`), []byte(`"version":2`), 1), bytes.Replace(b, []byte(`"include"`), []byte(`"execute"`), 1), bytes.Replace(b, []byte(`"capture"`), []byte(`"unknown"`), 1), bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"secret":"x"`), 1)} {
		if _, e = DecodePolicy(bad); e == nil {
			t.Fatal("invalid policy accepted")
		}
	}
	if _, e = DecodePolicy(original); e != nil {
		t.Fatal(e)
	}
}
func TestPolicyLimits(t *testing.T) {
	p := Policy{Version: 1, Intent: "capture", Scope: "source-root", SelectionVersion: 1, Rules: []Rule{}}
	for range 32 {
		p.Rules = append(p.Rules, Rule{Action: "include", Selection: selector([]byte("a"))})
	}
	b, e := p.Canonical()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = DecodePolicy(b); e != nil {
		t.Fatal(e)
	}
	p.Rules = append(p.Rules, p.Rules[0])
	raw, _ := json.Marshal(p)
	if _, e = DecodePolicy(raw); e == nil {
		t.Fatal("33 rules")
	}
	p.Rules = p.Rules[:32]
	for n := range p.Rules {
		p.Rules[n].Selection = selector(bytes.Repeat([]byte("x"), 255))
	}
	if _, e = p.Canonical(); e == nil {
		t.Fatal("descriptor bytes")
	}
	if _, e = DecodePolicy(bytes.Repeat([]byte(" "), PolicyBytes+1)); e == nil {
		t.Fatal("input bytes")
	}
}

func TestPolicyMaximumDescriptor(t *testing.T) {
	p := Policy{Version: 1, Intent: "capture", Scope: "source-root", SelectionVersion: 1, Rules: []Rule{}}
	for n := 0; n < 10; n++ {
		size := 255
		if n == 9 {
			size = 105
		}
		p.Rules = append(p.Rules, Rule{Action: "include", Selection: selector(bytes.Repeat([]byte("a"), size))})
	}
	b, e := p.Canonical()
	if e != nil || len(b) != 4096 {
		t.Fatal(len(b), e)
	}
	if _, e = DecodePolicy(b); e != nil {
		t.Fatal(e)
	}
	p.Rules[9].Selection = selector(bytes.Repeat([]byte("a"), 108))
	if _, e = p.Canonical(); e == nil {
		t.Fatal("canonical descriptor overflow")
	}
}
