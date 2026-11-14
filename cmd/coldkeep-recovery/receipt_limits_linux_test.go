package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

func readReceiptInput(t *testing.T, ctx context.Context, b []byte) (recoveryexport.ImportReceipt, error) {
	t.Helper()
	root := t.TempDir()
	if e := os.Mkdir(filepath.Join(root, ".coldkeep-control"), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, ".coldkeep-control", "IMPORT_READY"), b, 0600); e != nil {
		t.Fatal(e)
	}
	a, e := os.OpenRoot(root)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	return recoveryexport.ReadImportReceipt(ctx, a)
}

func TestRecoveryReceiptByteBoundary(t *testing.T) {
	_, _, b := recoveryReceiptScaleFixture(t)
	b = append(b, bytes.Repeat([]byte(" "), int(recoveryexport.ImportReceiptMaxBytes)-len(b))...)
	got, e := readReceiptInput(t, context.Background(), b)
	if e != nil || len(got.Objects) != 6000 {
		t.Fatalf("exact byte boundary: %v count=%d", e, len(got.Objects))
	}
	b = append(b, ' ')
	got, e = readReceiptInput(t, context.Background(), b)
	if !errors.Is(e, recoveryexport.ErrLimit) || got.Objects != nil {
		t.Fatalf("byte overflow allocated objects: %v", e)
	}
}

func TestRecoveryReceiptObjectBoundary(t *testing.T) {
	objects := strings.Repeat(`{},`, recoveryexport.ImportReceiptMaxObjects-1) + `{}`
	got, e := readReceiptInput(t, context.Background(), []byte(`{"objects":[`+objects+`],"kind":"boundary"}`))
	if e != nil || len(got.Objects) != recoveryexport.ImportReceiptMaxObjects {
		t.Fatalf("exact count boundary: %v count=%d", e, len(got.Objects))
	}
	for _, field := range []string{"objects", "OBJECTS", "objectſ"} {
		t.Run(field, func(t *testing.T) {
			got, e := readReceiptInput(t, context.Background(), []byte(`{"kind":"must-not-decode","`+field+`":[`+objects+`,{}]}`))
			if !errors.Is(e, recoveryexport.ErrLimit) || got.Objects != nil || got.Kind != "" {
				t.Fatalf("count overflow reached typed decode: %v count=%d kind=%q", e, len(got.Objects), got.Kind)
			}
		})
	}
}

func TestRecoveryReceiptStrictness(t *testing.T) {
	for name, b := range map[string]string{
		"duplicate":         `{"objects":[],"objects":[]}`,
		"alias-duplicate":   `{"objects":[],"OBJECTS":[]}`,
		"unicode-duplicate": `{"objects":[],"objectſ":[]}`,
		"nested-duplicate":  `{"objects":[{"id":1,"ID":2}]}`,
		"unknown":           `{"unknown":1}`,
		"truncated":         `{"objects":[{}`,
		"trailing-value":    `{"objects":[]} {}`,
		"trailing-garbage":  `{"objects":[]} x`,
		"depth":             `{"objects":` + strings.Repeat("[", 14) + `0` + strings.Repeat("]", 14) + `}`,
		"string":            `{"kind":"` + strings.Repeat("x", 4097) + `"}`,
		"key":               `{"` + strings.Repeat("x", 129) + `":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := readReceiptInput(t, context.Background(), []byte(b)); e == nil {
				t.Fatal("accepted malformed receipt")
			}
		})
	}
}

type receiptCancelContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *receiptCancelContext) Err() error {
	c.checks++
	if c.checks == 12 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestRecoveryReceiptCancellation(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "during"}[during], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			work := ctx
			if during {
				work = &receiptCancelContext{Context: ctx, cancel: cancel}
			} else {
				cancel()
			}
			got, e := readReceiptInput(t, work, []byte(`{"kind":"must-not-decode","objects":[{},{},{},{},{},{},{},{},{},{}]}`))
			if !errors.Is(e, context.Canceled) || got.Kind != "" || got.Objects != nil {
				t.Fatalf("cancellation reached typed decode: %v", e)
			}
		})
	}
}

func TestRecoveryReceiptRegularInput(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			control := filepath.Join(root, ".coldkeep-control")
			if e := os.Mkdir(control, 0700); e != nil {
				t.Fatal(e)
			}
			source := filepath.Join(root, "source")
			path := filepath.Join(control, "IMPORT_READY")
			if e := os.WriteFile(source, []byte(`{"objects":[]}`), 0600); e != nil {
				t.Fatal(e)
			}
			var e error
			switch kind {
			case "symlink":
				e = os.Symlink(source, path)
			case "hardlink":
				e = os.Link(source, path)
			case "directory":
				e = os.Mkdir(path, 0700)
			}
			if e != nil {
				t.Fatal(e)
			}
			a, e := os.OpenRoot(root)
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			if _, e = recoveryexport.ReadImportReceipt(context.Background(), a); e == nil {
				t.Fatal("accepted nonregular/aliased receipt")
			}
		})
	}
}

func TestRecoveryConfigLimitRemainsSmall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if e := os.WriteFile(path, []byte(`{"payload_root":"`+strings.Repeat("x", 1<<20)+`"}`), 0600); e != nil {
		t.Fatal(e)
	}
	var c config
	if e := readJSON(path, &c); e == nil {
		t.Fatal("operator config ceiling raised")
	}
}
