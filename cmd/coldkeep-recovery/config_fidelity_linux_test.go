package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

func recoveryConfigInput(t *testing.T, raw string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runRecoveryConfig(t *testing.T, path string) error {
	t.Helper()
	previous := os.Args
	os.Args = []string{"coldkeep-recovery", "binding-preview", "-config", path}
	defer func() { os.Args = previous }()
	return run()
}

func TestRecoveryConfigUnicodeRefusals(t *testing.T) {
	for _, test := range []struct{ name, raw string }{
		{"invalid-utf8", "{\"payload_root\":\"/tmp/root-\xff\"}"},
		{"overlong-utf8", "{\"payload_root\":\"/tmp/root-\xc0\xaf\"}"},
		{"encoded-surrogate", "{\"payload_root\":\"/tmp/root-\xed\xa0\x80\"}"},
		{"truncated-utf8", "{\"payload_root\":\"/tmp/root-\xe2\x82\"}"},
		{"lone-low-surrogate", `{"payload_root":"/tmp/root-\udcff"}`},
		{"lone-high-surrogate", `{"payload_root":"/tmp/root-\ud800"}`},
		{"high-high", `{"payload_root":"/tmp/root-\ud800\udbff"}`},
		{"low-high", `{"payload_root":"/tmp/root-\udc00\ud800"}`},
		{"high-scalar", `{"payload_root":"/tmp/root-\ud800\ufffd"}`},
		{"high-gap-low", `{"payload_root":"/tmp/root-\ud800x\udc00"}`},
		{"high-escaped-backslash-low", `{"payload_root":"/tmp/root-\ud800\\udc00"}`},
		{"paired-then-low", `{"payload_root":"/tmp/root-\ud83d\ude00\udcff"}`},
		{"escaped-quote-then-high", `{"payload_root":"/tmp/root-\"\ud800"}`},
		{"key-surrogate", `{"payload_root\ud800":"/tmp/root"}`},
		{"password-surrogate", `{"payload_root":"/tmp/root","password_file":"/tmp/password-\ud800"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := recoveryConfigInput(t, test.raw)
			want := config{PayloadRoot: "must-not-decode"}
			got := want
			if err := readJSON(path, &got); err == nil || err.Error() != "invalid configuration Unicode" {
				t.Fatalf("expected Unicode admission refusal: %v; decoded root bytes=%x", err, []byte(got.PayloadRoot))
			}
			if got != want {
				t.Fatal("malformed Unicode reached typed configuration decoding")
			}
			// With no password file, reaching credential admission would give a
			// different refusal. Exercise the real command entry point as well.
			if err := runRecoveryConfig(t, path); err == nil || err.Error() != "invalid configuration Unicode" {
				t.Fatalf("configuration did not refuse before credentials: %v", err)
			}
		})
	}
}

func TestRecoveryConfigExactPathControls(t *testing.T) {
	for _, test := range []struct{ name, raw, want string }{
		{"literal-replacement-rune", `{"payload_root":"/tmp/root-�"}`, "/tmp/root-�"},
		{"escaped-replacement-rune", `{"payload_root":"/tmp/root-\ufffd"}`, "/tmp/root-�"},
		{"paired-surrogates", `{"payload_root":"/tmp/root-\ud83d\ude00"}`, "/tmp/root-😀"},
		{"pair-minimum", `{"payload_root":"/tmp/root-\uD800\uDC00"}`, "/tmp/root-\U00010000"},
		{"pair-maximum", `{"payload_root":"/tmp/root-\uDBFF\uDFFF"}`, "/tmp/root-\U0010ffff"},
		{"scalar-boundaries", `{"payload_root":"/tmp/root-\ud7ff\ue000"}`, "/tmp/root-\ud7ff\ue000"},
		{"literal-backslash-u", `{"payload_root":"/tmp/root-\\udcff"}`, `/tmp/root-\udcff`},
		{"doubled-backslash-u", `{"payload_root":"/tmp/root-\\\\ud800"}`, `/tmp/root-\\ud800`},
		{"escaped-quote", `{"payload_root":"/tmp/root-\"-\ufffd"}`, "/tmp/root-\"-�"},
		{"adjacent-pairs", `{"payload_root":"/tmp/root-\ud83d\ude00\ud83d\ude01"}`, "/tmp/root-😀😁"},
		{"whitespace-case-nonascii", `{"payload_root":"/tmp/ Root-Ä "}`, "/tmp/ Root-Ä "},
		{"standard-escapes", `{"payload_root":"/tmp/root-\b\f\n\r\t\/"}`, "/tmp/root-\b\f\n\r\t/"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got config
			if err := readJSON(recoveryConfigInput(t, test.raw), &got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal([]byte(got.PayloadRoot), []byte(test.want)) {
				t.Fatalf("path bytes changed: got=%x want=%x", []byte(got.PayloadRoot), []byte(test.want))
			}
		})
	}
	t.Run("base64-native-invalid-utf8", func(t *testing.T) {
		want := []byte("/tmp/root-\xff")
		encoded := base64.StdEncoding.EncodeToString(want)
		var c config
		if err := readJSON(recoveryConfigInput(t, `{"payload_root_base64":"`+encoded+`"}`), &c); err != nil {
			t.Fatal(err)
		}
		if c.PayloadRoot != "" || c.PayloadRootBase64 != encoded {
			t.Fatal("base64 representation changed")
		}
		got, err := recoveryexport.DecodeBindingPath(c.PayloadRootBase64)
		if err != nil || !bytes.Equal([]byte(got), want) {
			t.Fatalf("native path bytes changed: got=%x want=%x error=%v", []byte(got), want, err)
		}
	})
}

func TestRecoveryConfigByteBoundary(t *testing.T) {
	const ceiling = 1 << 20
	small := `{"payload_root":"/tmp/root"}`
	padded := small + strings.Repeat(" ", ceiling-len(small))
	largeRoot := "/" + strings.Repeat("x", ceiling-len(`{"payload_root":""}`)-1)
	large := `{"payload_root":"` + largeRoot + `"}`
	for _, test := range []struct {
		name, raw, want string
		refuse          bool
	}{
		{"exact-value", large, largeRoot, false},
		{"exact-padding", padded, "/tmp/root", false},
		{"excess-value", `{"payload_root":"` + largeRoot + `x"}`, "", true},
		{"excess-padding", padded + " ", "", true},
		{"excess-trailing-value", padded + "{}", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := config{PayloadRoot: "must-not-decode"}
			err := readJSON(recoveryConfigInput(t, test.raw), &got)
			if test.refuse {
				if err == nil || err.Error() != "operator configuration limit" || got.PayloadRoot != "must-not-decode" {
					t.Fatalf("excess bytes reached typed decode: bytes=%d error=%v", len(test.raw), err)
				}
			} else if err != nil || got.PayloadRoot != test.want || len(test.raw) != ceiling {
				t.Fatalf("exact byte boundary: bytes=%d error=%v", len(test.raw), err)
			}
		})
	}
}

func TestRecoveryConfigSyntaxRefusals(t *testing.T) {
	for _, test := range []struct{ name, raw string }{
		{"unknown-field", `{"payload_root":"/tmp/root","unexpected":true}`},
		{"trailing-value", `{"payload_root":"/tmp/root"} {}`},
		{"trailing-garbage", `{"payload_root":"/tmp/root"} x`},
		{"truncated", `{"payload_root":"/tmp/root"`},
		{"malformed-escape", `{"payload_root":"/tmp/root-\q"}`},
		{"incomplete-unicode", `{"payload_root":"/tmp/root-\u12"}`},
		{"nonhex-unicode", `{"payload_root":"/tmp/root-\uxxxx"}`},
		{"unescaped-control", "{\"payload_root\":\"/tmp/root-\n\"}"},
		{"typed-value", `{"payload_root":42}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got config
			if err := readJSON(recoveryConfigInput(t, test.raw), &got); err == nil {
				t.Fatal("accepted malformed or unsupported configuration")
			}
		})
	}
}

func TestRecoveryConfigRepresentationPolicy(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("/tmp/root-\xff"))
	for _, test := range []struct {
		name, raw string
		refuse    bool
	}{
		{"both-nonempty", `{"payload_root":"/tmp/root","payload_root_base64":"` + encoded + `"}`, true},
		{"both-nonempty-invalid-base64", `{"payload_root":"/tmp/root","payload_root_base64":"!"}`, true},
		{"invalid-base64", `{"payload_root_base64":"!"}`, true},
		{"plain-only", `{"payload_root":"/tmp/root"}`, false},
		{"base64-only", `{"payload_root_base64":"` + encoded + `"}`, false},
		{"empty-plain", `{"payload_root":"","payload_root_base64":"` + encoded + `"}`, false},
		{"empty-base64", `{"payload_root":"/tmp/root","payload_root_base64":""}`, false},
		{"null-plain", `{"payload_root":null,"payload_root_base64":"` + encoded + `"}`, false},
		{"null-base64", `{"payload_root":"/tmp/root","payload_root_base64":null}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runRecoveryConfig(t, recoveryConfigInput(t, test.raw))
			if test.refuse {
				if !errors.Is(err, recoveryexport.ErrConfiguration) {
					t.Fatalf("expected representation refusal before credentials: %v", err)
				}
			} else if err == nil || err.Error() != "private regular credential file required" {
				t.Fatalf("supported representation did not reach credential admission: %v", err)
			}
		})
	}
}
