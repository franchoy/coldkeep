package recoveryexport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

func TestBindingDockerProfile(t *testing.T) {
	if os.Getuid() == 0 {
		t.Fatal("non-root fixture operator required")
	}
	image, reader := os.Getenv("COLDKEEP_RECOVERY_IMAGE"), os.Getenv("COLDKEEP_RECOVERY_READER")
	if image == "" || reader == "" {
		t.Fatal("staged current reader and pinned image required")
	}
	f, a := adoptionFixture(t)
	_, e := ApplyAdoption(context.Background(), f.config, a)
	must(t, e)
	private, installation, checkpoints, source, tools := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	must(t, os.Chmod(installation, 0700))
	bytes, e := os.ReadFile(reader)
	must(t, e)
	must(t, os.WriteFile(filepath.Join(tools, "reader"), bytes, 0700))
	password, e := f.config.Password(context.Background())
	must(t, e)
	must(t, os.WriteFile(filepath.Join(private, "password"), []byte(password), 0600))
	must(t, os.WriteFile(filepath.Join(private, "key"), []byte(hex.EncodeToString(f.key)), 0600))
	cfg := map[string]any{"host": f.config.Host, "port": f.config.Port, "database": f.config.Database, "user": f.config.User, "payload_root": "/payload", "password_file": "/private/password", "key_file": "/private/key", "key_reference": f.config.KeyReference, "pg_dump": "/usr/lib/postgresql/16/bin/pg_dump", "pg_restore": "/usr/lib/postgresql/16/bin/pg_restore"}
	must(t, os.WriteFile(filepath.Join(private, "config.json"), []byte(bindingJSON(cfg)), 0600))
	name := fmt.Sprintf("ck-phase3b-binding-%d-%d", os.Getpid(), fixtureSeq.Add(1))
	cid := ""
	docker := func(args ...string) ([]byte, error) { return exec.Command("docker", args...).CombinedOutput() }
	stop := func() {
		if cid != "" {
			out, e := docker("rm", "-f", cid)
			if e != nil {
				t.Fatalf("owned cleanup %v %s", e, out)
			}
			cid = ""
		}
	}
	defer stop()
	start := func(t *testing.T, root string) {
		args := []string{"run", "-d", "--name", name, "--label", "coldkeep.disposable=phase3b-binding", "--network", "host", "--read-only", "--memory", "256m", "--cpus", "2", "--tmpfs", "/tmp:rw,size=64m", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())}
		for _, m := range []string{"type=bind,src=" + root + ",dst=/payload", "type=bind,src=" + installation + ",dst=/installation", "type=bind,src=" + checkpoints + ",dst=/checkpoints", "type=bind,src=" + source + ",dst=/source,readonly", "type=bind,src=" + private + ",dst=/private,readonly", "type=bind,src=" + tools + ",dst=/tools,readonly"} {
			args = append(args, "--mount", m)
		}
		args = append(args, "--entrypoint", "/bin/sleep", image, "300")
		out, e := docker(args...)
		if e != nil {
			t.Fatalf("container start %v %s", e, out)
		}
		cid = strings.TrimSpace(string(out))
	}
	call := func(t *testing.T, args ...string) (BindingResult, error) {
		command := append([]string{"exec", cid, "/usr/bin/env", "-i", "PATH=/usr/lib/postgresql/16/bin:/usr/bin:/bin", "LC_ALL=C", "/tools/reader"}, args...)
		out, e := docker(command...)
		var r BindingResult
		line := ""
		for _, candidate := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(candidate, "{") {
				line = candidate
				break
			}
		}
		if len(line) > 0 && line[0] == '{' {
			if x := json.Unmarshal([]byte(line), &r); x != nil {
				t.Fatalf("operator JSON: %v %s", x, out)
			}
		}
		if e != nil {
			t.Logf("expected/operator diagnostic: %s", out)
		}
		return r, e
	}
	var last BindingResult
	operation := func(t *testing.T, action BindingAction, revision int64, sourceID string) {
		id, e := bindingID("bind")
		must(t, e)
		schema := 19
		if action == BindingInitial {
			schema = 18
		}
		args := []string{"-config", "/private/config.json", "-installation-directory", "/installation", "-binding-action", string(action), "-expected-schema", strconv.Itoa(schema), "-expected-revision", strconv.FormatInt(revision, 10), "-operation-id", id}
		if sourceID != "" {
			p, e := EncodeBindingPath("/source")
			must(t, e)
			args = append(args, "-source-id", sourceID, "-source-path-base64", p)
		}
		preview, e := call(t, append([]string{"binding-preview"}, args...)...)
		must(t, e)
		apply := append([]string{"binding-apply"}, args...)
		apply = append(apply, "-plan-sha256", preview.PlanSHA256, "-checkpoint", fmt.Sprintf("/checkpoints/generation-%d", revision+1))
		last, e = call(t, apply...)
		must(t, e)
		if last.Status != "complete; bounded maintenance only" {
			t.Fatal(last)
		}
	}
	t.Run("non-root-readonly-source", func(t *testing.T) {
		start(t, f.root)
		out, e := docker("exec", cid, "/bin/sh", "-c", `test "$(id -u)" != 0 && ! touch /source/must-not-exist`)
		if e != nil {
			t.Fatalf("non-root/read-only proof %v %s", e, out)
		}
		operation(t, BindingInitial, 0, "")
		id, e := db.NewIdentity("source")
		must(t, e)
		operation(t, BindingSourceRegister, 1, id)
		r, e := call(t, "binding-source-status", "-config", "/private/config.json", "-installation-directory", "/installation", "-source-id", id)
		must(t, e)
		if r.Status != "source-available; capture disabled" {
			t.Fatal(r)
		}
	})
	t.Run("recreation-requires-reconnect", func(t *testing.T) {
		old := last.Descriptor
		stop()
		start(t, f.root)
		r, e := call(t, "binding-status", "-installation-directory", "/installation")
		if e == nil || r.Status != "binding-mismatch" {
			t.Fatal(r, e)
		}
		operation(t, BindingReconnect, 2, "")
		if last.Descriptor.Marker.Incarnation == old.Marker.Incarnation || last.Descriptor.Marker.RootID != old.Marker.RootID {
			t.Fatal("reconnect lineage")
		}
	})
	t.Run("empty-substitute", func(t *testing.T) {
		stop()
		replacement := t.TempDir()
		start(t, replacement)
		r, e := call(t, "binding-status", "-installation-directory", "/installation")
		if e == nil {
			t.Fatal(r)
		}
		entries, e := os.ReadDir(replacement)
		must(t, e)
		if len(entries) != 0 {
			t.Fatal("empty substitute initialized")
		}
	})
	t.Run("missing-bind-source", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		out, e := docker("run", "--rm", "--mount", "type=bind,src="+missing+",dst=/source,readonly", "--entrypoint", "/bin/true", image)
		if e == nil || !strings.Contains(string(out), "bind source path does not exist") {
			t.Fatalf("missing bind control %v %s", e, out)
		}
		if _, e = os.Stat(missing); !os.IsNotExist(e) {
			t.Fatal("host path auto-created")
		}
	})
	if report := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); report != "" {
		must(t, os.WriteFile(filepath.Join(report, "phase3b-docker.json"), []byte(bindingJSON(map[string]any{"name": name, "last_descriptor": last.Descriptor, "uid": os.Getuid(), "profile": "fixed separate bind mounts; read-only sources; no auto creation; namespace-scoped reconnect", "memory_bytes": 256 << 20, "tmpfs_bytes": 64 << 20, "hardware_independence": "unknown", "privileged_unmount_reboot": "unqualified"})), 0600))
	}
}
