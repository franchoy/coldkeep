package recoveryexport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const bindingFixtureImage = "postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825"

type bindingFixtureMount struct {
	Source, Destination, Type, Name string
	RW                              bool
}

type bindingFixtureInspection struct {
	ID, Name string
	Config   struct {
		Image, User string
		Labels      map[string]string
	}
	Mounts     []bindingFixtureMount
	HostConfig struct {
		Tmpfs            map[string]string
		ReadonlyRootfs   bool
		Memory, NanoCpus int64
		NetworkMode      string
	}
}

// The ID and random task label, never a name pattern or volume age, own cleanup.
type bindingDockerFixture struct {
	id, name, owner string
	image           string
	mounts          []bindingFixtureMount
	observed        bindingFixtureInspection
	baselineVolumes map[string]bool
	ownedVolumes    []string
	removed         bool
	remove          func(...string) ([]byte, error)
	tmpfsEvidence   []string
}

func bindingDocker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

func bindingDockerVolumes() (map[string]bool, error) {
	out, err := bindingDocker("volume", "ls", "--format", "{{.Name}}")
	if err != nil {
		return nil, fmt.Errorf("volume inventory: %w", err)
	}
	volumes := map[string]bool{}
	for _, name := range strings.Fields(string(out)) {
		volumes[name] = true
	}
	return volumes, nil
}

func newBindingDockerFixture(t *testing.T, mounts []bindingFixtureMount) *bindingDockerFixture {
	t.Helper()
	if os.Getuid() == 0 || os.Getenv("COLDKEEP_RECOVERY_IMAGE") != bindingFixtureImage {
		t.Fatal("non-root operator and exact pinned image required")
	}
	owner, err := bindingID("fixture")
	must(t, err)
	f := &bindingDockerFixture{name: "ck-binding-" + owner, owner: owner, image: bindingFixtureImage, mounts: mounts, remove: bindingDocker}
	f.baselineVolumes, err = bindingDockerVolumes()
	must(t, err)
	t.Cleanup(func() {
		if err := f.stop(); err != nil {
			t.Errorf("owned fixture cleanup: %v", err)
		}
		if report := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR"); report != "" {
			data, err := json.Marshal(map[string]any{"container_id": f.id, "task_identity": f.owner, "effective_mounts": f.observed.Mounts, "tmpfs_configuration": f.observed.HostConfig.Tmpfs, "effective_tmpfs": f.tmpfsEvidence, "owned_volumes": f.ownedVolumes, "removed": f.removed})
			if err == nil {
				err = os.WriteFile(filepath.Join(report, f.name+".json"), data, 0600)
			}
			if err != nil {
				t.Errorf("required fixture evidence: %v", err)
			}
		}
	})
	// The pinned image declares one otherwise unused persistent data destination.
	out, err := bindingDocker("image", "inspect", f.image, "--format", "{{json .Config.Volumes}}")
	must(t, err)
	var declared map[string]json.RawMessage
	must(t, json.Unmarshal(out, &declared))
	if len(declared) != 1 || declared["/var/lib/postgresql/data"] == nil {
		t.Fatal("pinned fixture image declared-volume contract changed")
	}
	args := []string{"create", "--name", f.name, "--label", "coldkeep.disposable=phase3c2-binding", "--label", "coldkeep.fixture-owner=" + f.owner, "--network", "host", "--read-only", "--memory", "256m", "--cpus", "2", "--tmpfs", "/tmp:rw,size=64m", "--tmpfs", "/var/lib/postgresql/data:rw,size=1m", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())}
	for _, m := range mounts {
		value := "type=bind,src=" + m.Source + ",dst=" + m.Destination
		if !m.RW {
			value += ",readonly"
		}
		args = append(args, "--mount", value)
	}
	// A private cidfile retains identity even if the client's response is lost.
	cidfile := filepath.Join(t.TempDir(), "container-id")
	args = append(args, "--cidfile", cidfile, "--entrypoint", "/bin/sleep", f.image, "300")
	out, createErr := bindingDocker(args...)
	if id, readErr := os.ReadFile(cidfile); readErr == nil {
		f.id = strings.TrimSpace(string(id))
	} else if createErr == nil {
		f.id = strings.TrimSpace(string(out))
	}
	if createErr != nil {
		t.Fatalf("fixture create: %v %s", createErr, out)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(f.id) {
		t.Fatal("missing exact created container ID")
	}
	f.observed, err = f.inspect()
	must(t, err)
	for _, m := range f.observed.Mounts {
		if m.Type == "volume" && !f.baselineVolumes[m.Name] {
			f.ownedVolumes = append(f.ownedVolumes, m.Name)
		}
	}
	must(t, f.validateMounts(f.observed.Mounts))
	out, err = bindingDocker("start", f.id)
	if err != nil {
		t.Fatalf("fixture start: %v %s", err, out)
	}
	out, err = bindingDocker("exec", f.id, "/bin/cat", "/proc/self/mountinfo")
	must(t, err)
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, " - ", 2)
		left := strings.Fields(parts[0])
		if len(parts) != 2 || len(left) < 6 || (left[4] != "/tmp" && left[4] != "/var/lib/postgresql/data") {
			continue
		}
		right := strings.Fields(parts[1])
		if len(right) < 3 || right[0] != "tmpfs" {
			t.Fatal("effective fixture data/tmp mount is not tmpfs")
		}
		f.tmpfsEvidence = append(f.tmpfsEvidence, line)
	}
	if len(f.tmpfsEvidence) != 2 {
		t.Fatal("missing effective tmpfs evidence")
	}
	return f
}

func (f *bindingDockerFixture) inspect() (bindingFixtureInspection, error) {
	var objects []bindingFixtureInspection
	out, err := bindingDocker("inspect", f.id)
	if err != nil {
		return bindingFixtureInspection{}, fmt.Errorf("inspect exact fixture: %w", err)
	}
	if err = json.Unmarshal(out, &objects); err != nil || len(objects) != 1 {
		return bindingFixtureInspection{}, errors.New("invalid fixture inspection")
	}
	v := objects[0]
	if v.ID != f.id || v.Name != "/"+f.name || v.Config.Labels["coldkeep.fixture-owner"] != f.owner || v.Config.Image != f.image {
		return v, errors.New("fixture ownership mismatch; no cleanup authorized")
	}
	if len(v.HostConfig.Tmpfs) != 2 || v.HostConfig.Tmpfs["/tmp"] != "rw,size=64m" || v.HostConfig.Tmpfs["/var/lib/postgresql/data"] != "rw,size=1m" ||
		!v.HostConfig.ReadonlyRootfs || v.HostConfig.Memory != 256<<20 || v.HostConfig.NanoCpus != 2_000_000_000 || v.HostConfig.NetworkMode != "host" ||
		v.Config.User != fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()) {
		return v, errors.New("fixture runtime isolation changed")
	}
	return v, nil
}

func (f *bindingDockerFixture) validateMounts(mounts []bindingFixtureMount) error {
	expected := map[string]bindingFixtureMount{}
	for _, m := range f.mounts {
		m.Type = "bind"
		expected[m.Destination] = m
	}
	if len(mounts) != len(expected) {
		return errors.New("unexpected fixture mount count")
	}
	for _, m := range mounts {
		want, ok := expected[m.Destination]
		if !ok || m.Type != want.Type || m.Source != want.Source || m.RW != want.RW || m.Name != "" {
			return errors.New("unexpected persistent or substituted fixture mount")
		}
		delete(expected, m.Destination)
	}
	return nil
}

func (f *bindingDockerFixture) stop() error {
	if f.id == "" || f.removed {
		return nil
	}
	if _, err := f.inspect(); err != nil {
		return err
	}
	// Docker -v removes only anonymous volumes attached to this verified ID;
	// it never removes a named volume. Normal fixtures have no volume mounts.
	if out, err := f.remove("rm", "-f", "-v", f.id); err != nil {
		return fmt.Errorf("remove owned fixture: %w %s", err, out)
	}
	out, err := bindingDocker("container", "ls", "-a", "--no-trunc", "--format", "{{.ID}}")
	if err != nil {
		return fmt.Errorf("container absence inventory: %w", err)
	}
	for _, id := range strings.Fields(string(out)) {
		if id == f.id {
			return errors.New("owned fixture container remains")
		}
	}
	volumes, err := bindingDockerVolumes()
	if err != nil {
		return err
	}
	for _, name := range f.ownedVolumes {
		if volumes[name] {
			return errors.New("owned anonymous fixture volume remains")
		}
	}
	for name := range f.baselineVolumes {
		if !volumes[name] {
			return errors.New("baseline volume disappeared; preservation unqualified")
		}
	}
	f.removed = true
	return nil
}

func TestBindingDockerFixtureHygiene(t *testing.T) {
	for _, scenario := range []string{"success", "early-exit", "unexpected-persistent-mount", "foreign-owner", "cleanup-failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := newBindingDockerFixture(t, nil)
			switch scenario {
			case "early-exit":
				if _, err := bindingDocker("exec", f.id, "/bin/false"); err == nil {
					t.Fatal("injected child failure did not fail")
				}
				return // registered cleanup must still publish a successful absence receipt
			case "unexpected-persistent-mount":
				mounts := append([]bindingFixtureMount(nil), f.observed.Mounts...)
				mounts = append(mounts, bindingFixtureMount{Type: "volume", Name: "unowned-volume", Destination: "/unexpected"})
				if f.validateMounts(mounts) == nil {
					t.Fatal("unexpected persistent mount accepted")
				}
			case "foreign-owner":
				owner := f.owner
				f.owner = "not-this-operation"
				err := f.stop()
				f.owner = owner
				if err == nil {
					t.Fatal("foreign-owned cleanup allowed")
				}
				_, err = f.inspect()
				must(t, err)
			case "cleanup-failure":
				f.remove = func(...string) ([]byte, error) { return nil, errors.New("injected removal failure") }
				err := f.stop()
				f.remove = bindingDocker
				if err == nil || f.removed {
					t.Fatal("failed cleanup claimed completion")
				}
			}
			must(t, f.stop())
		})
	}
}
