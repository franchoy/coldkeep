package engine

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestStoreRequestCodecDocumentationMatchesRuntimePrecedence(t *testing.T) {
	type codecCase struct {
		name      string
		request   string
		env       *string
		want      string
		wantError bool
	}
	plain := "plain"
	aesGCM := "aes-gcm"
	invalid := "invalid"
	cases := []codecCase{
		{name: "explicit plain wins over env", request: "plain", env: &aesGCM, want: "none"},
		{name: "explicit aes-gcm wins over env", request: "aes-gcm", env: &plain, want: string(blocks.CodecAESGCM)},
		{name: "empty request uses plain env", env: &plain, want: "none"},
		{name: "empty request uses aes-gcm env", env: &aesGCM, want: string(blocks.CodecAESGCM)},
		{name: "empty request and unset env uses aes-gcm", want: string(blocks.CodecAESGCM)},
		{name: "empty request preserves invalid env error", env: &invalid, wantError: true},
		{name: "explicit request bypasses invalid env", request: "plain", env: &invalid, want: "none"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setCodecEnvironment(t, tc.env)
			t.Setenv("COLDKEEP_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			dbconn := newEngineTestDB(t)
			sgctx := storage.StorageContext{
				DB:           dbconn,
				Writer:       container.NewSimulatedWriter(1 << 20),
				ContainerDir: t.TempDir(),
			}
			eng, err := New(Config{DB: dbconn, ContainerDir: sgctx.ContainerDir, StoreContext: &sgctx})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			input := filepath.Join(t.TempDir(), "codec-precedence.txt")
			if err := os.WriteFile(input, []byte("phase12 codec precedence"), 0o600); err != nil {
				t.Fatalf("write input: %v", err)
			}

			_, err = eng.Store(context.Background(), StoreRequest{SourcePath: input, Codec: tc.request})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "COLDKEEP_CODEC") {
					t.Fatalf("Store error = %v, want existing COLDKEEP_CODEC error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Store: %v", err)
			}

			var got string
			if err := dbconn.QueryRow(`SELECT codec FROM storage_blocks ORDER BY id LIMIT 1`).Scan(&got); err != nil {
				t.Fatalf("query stored codec: %v", err)
			}
			if got != tc.want {
				t.Fatalf("stored codec = %q, want %q", got, tc.want)
			}
		})
	}

	comment := storeRequestCodecComment(t)
	if strings.Contains(strings.ToLower(comment), "repository default") {
		t.Fatalf("StoreRequest.Codec documentation still claims a repository default: %q", comment)
	}
	for _, required := range []string{"COLDKEEP_CODEC", "aes-gcm", "non-empty", "empty"} {
		if !strings.Contains(comment, required) {
			t.Fatalf("StoreRequest.Codec documentation %q does not contain %q", comment, required)
		}
	}
}

func TestEffectiveContainerDirPreservesExplicitValueAndDefaultsOnlyAbsentConfiguration(t *testing.T) {
	originalDefault := container.ContainersDir
	container.ContainersDir = filepath.Join(t.TempDir(), "default-containers")
	t.Cleanup(func() { container.ContainersDir = originalDefault })

	if got := (&DefaultEngine{config: Config{ContainerDir: " \t "}}).effectiveContainerDir(); got != container.ContainersDir {
		t.Fatalf("whitespace-only configuration resolved to %q, want default %q", got, container.ContainersDir)
	}
	explicit := " /custom/value "
	if got := (&DefaultEngine{config: Config{ContainerDir: explicit}}).effectiveContainerDir(); got != explicit {
		t.Fatalf("explicit configuration resolved to %q, want exact supplied value %q", got, explicit)
	}
}

func setCodecEnvironment(t *testing.T, value *string) {
	t.Helper()
	const key = "COLDKEEP_CODEC"
	old, hadOld := os.LookupEnv(key)
	t.Cleanup(func() {
		if hadOld {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	if value == nil {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		return
	}
	if err := os.Setenv(key, *value); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}

func storeRequestCodecComment(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	path := filepath.Join(filepath.Dir(currentFile), "candidates.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "StoreRequest" {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatal("StoreRequest is not a struct")
			}
			for _, field := range structType.Fields.List {
				if len(field.Names) == 1 && field.Names[0].Name == "Codec" && field.Doc != nil {
					return strings.TrimSpace(field.Doc.Text())
				}
			}
		}
	}
	t.Fatal("StoreRequest.Codec documentation not found")
	return ""
}
