package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

// Exercise process-level output and admission without replacing production
// operations or introducing credential/session hooks.
func runRecoveryOutput(t *testing.T, args []string, closed bool) (string, error) {
	t.Helper()
	reader, writer, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	var captured struct {
		bytes []byte
		err   error
	}
	done := make(chan struct{})
	go func() {
		captured.bytes, captured.err = io.ReadAll(reader)
		captured.err = errors.Join(captured.err, reader.Close())
		close(done)
	}()
	previousArgs, previousOutput := os.Args, os.Stdout
	defer func() {
		os.Args, os.Stdout = previousArgs, previousOutput
		_ = writer.Close()
		<-done
	}()
	os.Args, os.Stdout = append([]string{"coldkeep-recovery"}, args...), writer
	if closed {
		if e = writer.Close(); e != nil {
			t.Fatal(e)
		}
	}
	err := run()
	if !closed {
		if e = writer.Close(); e != nil {
			t.Fatal(e)
		}
	}
	<-done
	if captured.err != nil {
		t.Fatal(captured.err)
	}
	return string(captured.bytes), err
}

func TestRecoveryPublicOperationsBeforeConfig(t *testing.T) {
	for _, test := range []struct {
		name, command string
		closed        bool
		want          error
	}{
		{"check", "check", false, recoveryexport.ErrPackage},
		{"check-closed-output", "check", true, recoveryexport.ErrPackage},
		{"binding-status", "binding-status", false, recoveryexport.ErrBindingUnavailable},
		{"binding-status-closed-output", "binding-status", true, recoveryexport.ErrBindingUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			missing := filepath.Join(t.TempDir(), "absent")
			args := []string{test.command, "-config", recoveryConfigInput(t, "{"), "-package", missing, "-installation-directory", missing}
			output, e := runRecoveryOutput(t, args, test.closed)
			if !errors.Is(e, test.want) {
				t.Fatalf("public operation read config or changed refusal: %v", e)
			}
			wantClosed := test.command == "binding-status" && test.closed
			if errors.Is(e, os.ErrClosed) != wantClosed {
				t.Fatalf("operation/encoding error join: %v", e)
			}
			checkPublicOperationOutput(t, output, test.command == "binding-status" && !test.closed)
		})
	}
}

func checkPublicOperationOutput(t *testing.T, output string, wantJSON bool) {
	t.Helper()
	if !wantJSON {
		if output != "" {
			t.Fatalf("unexpected result on refusal: %q", output)
		}
		return
	}
	var result struct {
		Status string `json:"status"`
	}
	if e := json.Unmarshal([]byte(output), &result); e != nil {
		t.Fatal(e)
	}
	if result.Status != "binding-mismatch" {
		t.Fatalf("binding refusal result: %q", output)
	}
}

func q2Credential(t *testing.T, name string, b []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	return path
}

func TestRecoveryUnknownOperationAdmission(t *testing.T) {
	password := q2Credential(t, "password", []byte(" password\n\xff"))
	key := q2Credential(t, "key", []byte(strings.Repeat("a", 64)))
	badKey := q2Credential(t, "key", []byte(strings.Repeat("z", 64)))
	for _, test := range []struct {
		name string
		c    config
		want string
	}{
		{"path", config{PayloadRoot: "/plain", PayloadRootBase64: "invalid"}, recoveryexport.ErrConfiguration.Error()},
		{"password", config{}, "private regular credential file required"},
		{"key-file", config{PasswordFile: password, KeyFile: filepath.Join(t.TempDir(), "missing-key")}, "private regular credential file required"},
		{"key-decode", config{PasswordFile: password, KeyFile: badKey}, recoveryexport.ErrKeyUnavailable.Error()},
		{"dispatch", config{PasswordFile: password, KeyFile: key}, "unknown recovery operation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, e := json.Marshal(test.c)
			if e != nil {
				t.Fatal(e)
			}
			output, e := runRecoveryOutput(t, []string{"unknown", "-config", recoveryConfigInput(t, string(b))}, false)
			if e == nil || e.Error() != test.want || output != "" {
				t.Fatalf("configured admission order/output: %v output=%q", e, output)
			}
		})
	}
	t.Run("config", func(t *testing.T) {
		output, e := runRecoveryOutput(t, []string{"unknown", "-config", recoveryConfigInput(t, "{")}, false)
		if !errors.Is(e, io.ErrUnexpectedEOF) || output != "" {
			t.Fatalf("config must precede dispatch: %v output=%q", e, output)
		}
	})
}

func TestRecoveryPrivateCredentialAdmission(t *testing.T) {
	for _, kind := range []string{"missing", "directory", "symlink", "permissions", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			path := privateCredentialRefusalFixture(t, kind)
			b, e := privateBytes(path, 4)
			if e == nil || e.Error() != "private regular credential file required" || b != nil {
				t.Fatalf("private admission: bytes=%x error=%v", b, e)
			}
		})
	}
}

func privateCredentialRefusalFixture(t *testing.T, kind string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credential")
	var e error
	switch kind {
	case "directory":
		return t.TempDir()
	case "symlink":
		e = os.Symlink(q2Credential(t, "source", []byte("abcd")), path)
	case "permissions":
		path = q2Credential(t, "credential", []byte("abcd"))
		// Intentionally grant only group read in this refusal fixture. Chmod
		// makes the forbidden bit independent of the process umask.
		e = os.Chmod(path, 0040)
	case "oversize":
		e = os.WriteFile(path, []byte("abcde"), 0600)
	}
	if e != nil {
		t.Fatal(e)
	}
	return path
}

func TestRecoveryPrivateCredentialExactBytes(t *testing.T) {
	want := []byte{' ', '\n', 0xff, 0}
	source := q2Credential(t, "source", want)
	alias := filepath.Join(t.TempDir(), "hardlink")
	if e := os.Link(source, alias); e != nil {
		t.Fatal(e)
	}
	// Credential admission intentionally accepts hardlinks; receipt admission
	// has a separate policy. Keep exact max-length native bytes on both paths.
	for _, path := range []string{source, alias} {
		got, e := privateBytes(path, 4)
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("credential bytes changed: %x error=%v", got, e)
		}
	}
}
