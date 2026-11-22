package recoveryexport

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestBindingUnsafeControlOwner(t *testing.T) {
	f, o := bindingFixture(t)
	_, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	image := os.Getenv("COLDKEEP_RECOVERY_IMAGE")
	if image == "" {
		t.Fatal("explicit disposable image required")
	}
	for _, name := range []string{"marker", "descriptor"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(o.Request.InstallationDirectory, bindingDescriptorName)
			if name == "marker" {
				path = filepath.Join(f.root, ".coldkeep-control", bindingMarkerName)
			}
			original, e := os.ReadFile(path)
			must(t, e)
			defer func() { must(t, os.Remove(path)); must(t, os.WriteFile(path, original, 0600)) }()
			cmd := exec.Command("docker", "run", "--rm", "--label", "coldkeep.disposable=phase3b-owner-probe", "--network", "none", "--memory", "64m", "--user", "0", "--mount", "type=bind,src="+path+",dst=/record", "--entrypoint", "/bin/chown", image, strconv.Itoa(os.Getuid()+1), "/record")
			out, e := cmd.CombinedOutput()
			if e != nil {
				t.Fatalf("owned fixture owner change %v %s", e, out)
			}
			r, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, &f.config)
			if e == nil || r.OrdinaryWriterReady {
				t.Fatal("foreign-owned control admitted", r, e)
			}
		})
	}
}
