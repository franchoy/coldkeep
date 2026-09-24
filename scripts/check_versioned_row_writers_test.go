package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVersionedRowWriterGuardUsesCompleteTableIdentifiers(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("versioned row writer guard is a bash release gate")
	}

	for _, mode := range versionedWriterSearchModes(t) {
		t.Run(mode.name, func(t *testing.T) {
			t.Run("allows distinct identifiers and existing exceptions", func(t *testing.T) {
				repo := newVersionedWriterFixture(t, map[string]string{
					"internal/example/writer.go": `package example
const chunkRefs = "INSERT INTO chunk_block_refs (chunk_id) VALUES (1)"
const chunkSuffix = "INSERT INTO chunk_something (id) VALUES (1)"
const logicalSuffix = "INSERT INTO logical_file_something (id) VALUES (1)"
const qualifiedSuffix = "INSERT INTO chunk.schema_table (id) VALUES (1)"
`,
					"internal/storage/store.go": `package storage
const chunkInsert = "INSERT INTO chunk (chunk_hash) VALUES ('hash')"
const logicalInsert = "INSERT INTO logical_file(original_name) VALUES ('name')"
`,
					"internal/example/writer_test.go": `package example
const testOnlyInsert = "INSERT INTO chunk (chunk_hash) VALUES ('test')"
`,
				})

				output, err := runVersionedWriterGuard(t, repo, mode.path)
				if err != nil {
					t.Fatalf("allowed identifiers or exceptions failed: %v\n%s", err, output)
				}
				if !strings.Contains(output, "[versioned-row-writers] ok:") {
					t.Fatalf("success marker missing:\n%s", output)
				}
			})

			prohibited := []struct {
				name  string
				query string
			}{
				{name: "chunk whitespace", query: "INSERT INTO chunk (chunk_hash) VALUES ('hash')"},
				{name: "chunk parenthesis", query: "INSERT INTO chunk(chunk_hash) VALUES ('hash')"},
				{name: "logical file whitespace", query: "INSERT INTO logical_file (original_name) VALUES ('name')"},
				{name: "table at line end", query: "INSERT INTO chunk\n(chunk_hash) VALUES ('hash')"},
			}
			for _, test := range prohibited {
				t.Run(test.name, func(t *testing.T) {
					repo := newVersionedWriterFixture(t, map[string]string{
						"internal/example/writer.go": "package example\nconst query = `" + test.query + "`\n",
					})

					output, err := runVersionedWriterGuard(t, repo, mode.path)
					if err == nil {
						t.Fatalf("protected direct writer passed:\n%s", output)
					}
					if !strings.Contains(output, "internal/example/writer.go:") {
						t.Fatalf("violation path missing:\n%s", output)
					}
				})
			}
		})
	}
}

func TestVersionedRowWriterGuardAcceptsCurrentRepository(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("versioned row writer guard is a bash release gate")
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatalf("find bash: %v", err)
	}
	cmd := exec.Command(bash, filepath.Join(repoRoot(t), "scripts", "check_versioned_row_writers.sh"))
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("current repository failed guard: %v\n%s", err, output)
	}
}

type versionedWriterSearchMode struct {
	name string
	path string
}

func versionedWriterSearchModes(t *testing.T) []versionedWriterSearchMode {
	t.Helper()
	if _, err := exec.LookPath("rg"); err != nil {
		t.Fatal("rg is required to certify the primary guard path")
	}

	fallbackPath := filepath.Join(t.TempDir(), "fallback-bin")
	if err := os.MkdirAll(fallbackPath, 0o700); err != nil {
		t.Fatalf("create fallback tool directory: %v", err)
	}
	for _, name := range []string{"dirname", "find", "grep", "xargs"} {
		target, err := exec.LookPath(name)
		if err != nil {
			t.Fatalf("find fallback tool %s: %v", name, err)
		}
		if err := os.Symlink(target, filepath.Join(fallbackPath, name)); err != nil {
			t.Fatalf("link fallback tool %s: %v", name, err)
		}
	}

	return []versionedWriterSearchMode{
		{name: "ripgrep", path: os.Getenv("PATH")},
		{name: "grep fallback", path: fallbackPath},
	}
}

func newVersionedWriterFixture(t *testing.T, sources map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"scripts", "internal", "cmd"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatalf("create fixture directory %s: %v", dir, err)
		}
	}
	script := readRepoFile(t, filepath.Join("scripts", "check_versioned_row_writers.sh"))
	if err := os.WriteFile(filepath.Join(root, "scripts", "check_versioned_row_writers.sh"), []byte(script), 0o700); err != nil {
		t.Fatalf("write guard fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "cmd", "fixture.go"), []byte("package cmd\n"), 0o600); err != nil {
		t.Fatalf("write benign source fixture: %v", err)
	}
	for relative, content := range sources {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create source directory %s: %v", relative, err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write source fixture %s: %v", relative, err)
		}
	}
	return root
}

func runVersionedWriterGuard(t *testing.T, repo, path string) (string, error) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatalf("find bash: %v", err)
	}
	cmd := exec.Command(bash, filepath.Join(repo, "scripts", "check_versioned_row_writers.sh"))
	cmd.Dir = repo
	cmd.Env = replaceVersionedWriterEnv(os.Environ(), "PATH", path)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func replaceVersionedWriterEnv(env []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(env)+1)
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
}
