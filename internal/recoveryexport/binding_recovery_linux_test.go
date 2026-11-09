package recoveryexport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/db"
)

func bindingLineageQuery() string {
	parts := []string{}
	for _, t := range append(append([]tableSpec(nil), identityTables...), bindingTables...) {
		parts = append(parts, "'"+t.name+"',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM public."+pqIdent(t.name)+" t)")
	}
	return "SELECT jsonb_build_object(" + strings.Join(parts, ",") + ")::text"
}
func bindHistory(t *testing.T, f *fixture) BindingOptions {
	t.Helper()
	a := grantAdopter(t, f)
	_, e := ApplyAdoption(context.Background(), f.config, a)
	must(t, e)
	directory, e := os.MkdirTemp("/dev/shm", "coldkeep-binding-history-")
	must(t, e)
	t.Cleanup(func() { must(t, os.RemoveAll(directory)) })
	id, e := bindingID("bind")
	must(t, e)
	q := BindingRequest{Action: BindingInitial, InstallationDirectory: directory, ExpectedSchema: 18, OperationID: id}
	p, e := PreviewBinding(context.Background(), f.config, q, DefaultLimits())
	must(t, e)
	o := BindingOptions{Request: q, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)}
	_, e = ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	return o
}
func TestBindingIndependentRecovery(t *testing.T) {
	for _, plain := range []bool{true, false} {
		t.Run(fmt.Sprintf("plaintext=%v", plain), func(t *testing.T) {
			f, files := recoveryHistory(t, plain)
			key := stageRecoveryKey(t, f)
			o := bindHistory(t, f)
			source := t.TempDir()
			id, e := db.NewIdentity("source")
			must(t, e)
			q := nextBinding(t, f, o, BindingSourceRegister, id, source, 0)
			_, e = ApplyBinding(context.Background(), f.config, q)
			must(t, e)
			// Recover real historical files after the drained offline relocation,
			// as well as after the subsequent explicit incarnation advance.
			moved := f.root + " relocated history"
			must(t, os.Rename(f.root, moved))
			f.root, f.config.PayloadRoot = moved, moved
			q = nextBinding(t, f, o, BindingRelocate, "", "", 0)
			_, e = ApplyBinding(context.Background(), f.config, q)
			must(t, e)
			q = nextBinding(t, f, o, BindingReconnect, "", "", 0)
			_, e = ApplyBinding(context.Background(), f.config, q)
			must(t, e)
			post := exportOptions(t)
			_, e = Export(context.Background(), f.config, post)
			must(t, e)
			clear(f.key)
			isolatedRecovery(t, f, post, files, key, fmt.Sprintf("phase3b-schema19-plain-%v", plain), o.Request.InstallationDirectory, source)
		})
	}
}
func TestBindingOldWritersAndReaders(t *testing.T) {
	readers := os.Getenv("COLDKEEP_PHASE3A_READERS")
	old := filepath.Join(readers, "pre3b-reader")
	current := os.Getenv("COLDKEEP_RECOVERY_READER")
	if readers == "" || current == "" {
		t.Fatal("ordinary runner must stage untouched pre3B and current binaries")
	}
	f, files := recoveryHistory(t, false)
	for _, schema := range []int{17, 18, 19} {
		t.Run(fmt.Sprintf("schema%d", schema), func(t *testing.T) {
			if schema == 18 {
				a := grantAdopter(t, f)
				_, e := ApplyAdoption(context.Background(), f.config, a)
				must(t, e)
			}
			if schema == 19 {
				dir, e := os.MkdirTemp("/dev/shm", "coldkeep-binding-reader-")
				must(t, e)
				defer func() { must(t, os.RemoveAll(dir)) }()
				id, e := bindingID("bind")
				must(t, e)
				q := BindingRequest{Action: BindingInitial, InstallationDirectory: dir, ExpectedSchema: 18, OperationID: id}
				p, e := PreviewBinding(context.Background(), f.config, q, DefaultLimits())
				must(t, e)
				_, e = ApplyBinding(context.Background(), f.config, BindingOptions{Request: q, PlanSHA256: p.PlanSHA256, Export: exportOptions(t)})
				must(t, e)
			}
			post := exportOptions(t)
			_, e := Export(context.Background(), f.config, post)
			must(t, e)
			if schema < 19 {
				for _, label := range []string{"d19", "accepted", "pre3b", "current"} {
					command := exec.Command(filepath.Join(readers, label+"-writer"))
					command.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true")
					out, e := command.CombinedOutput()
					if schema == 17 && e != nil {
						t.Fatalf("writer positive %s: %v %s", label, e, out)
					}
					if schema == 18 && e == nil {
						t.Fatal("schema18 writer admitted", label)
					}
				}
			} else {
				for _, label := range []string{"d19", "accepted", "pre3b", "current"} {
					before, tree := catalogFingerprint(t, f.db), treeFingerprint(t, f.root)
					command := exec.Command(filepath.Join(readers, label+"-writer"))
					command.Env = append(os.Environ(), "DB_NAME="+f.config.Database, "COLDKEEP_STORAGE_DIR="+f.root, "COLDKEEP_DB_AUTO_BOOTSTRAP=true")
					out, e := command.CombinedOutput()
					if e == nil || !strings.Contains(string(out), "future version") {
						t.Fatalf("writer19 %s %v %s", label, e, out)
					}
					if before != catalogFingerprint(t, f.db) || tree != treeFingerprint(t, f.root) {
						t.Fatal("old writer mutated schema19")
					}
				}
			}
			d, open := importTarget(t, f)
			opts := importOptionsFor(t, post)
			cfg := readerConfig(t, d)
			if schema == 19 {
				out, e := runReader(t, old, "check", "-package", post.Destination)
				if e == nil {
					t.Fatal("old checker admitted v3", string(out))
				}
				out, e = runReader(t, old, "import", "-config", cfg, "-package", post.Destination, "-generation", opts.Generation, "-manifest-sha256", opts.ManifestSHA256, "-trusted-operator-backup", "-exclusive-target", "-database-capacity-bytes", "805306368")
				if e == nil {
					t.Fatal("old import admitted v3", string(out))
				}
			}
			imported, e := Import(context.Background(), d, opts)
			must(t, e)
			receipt := filepath.Join(d.PayloadRoot, ".coldkeep-control/IMPORT_READY")
			before, e := os.ReadFile(receipt)
			must(t, e)
			for _, binary := range []string{old, current} {
				output := filepath.Join(t.TempDir(), "files")
				out, e := runReader(t, binary, "restore", "-config", cfg, "-snapshot", "independent-history", "-output", output)
				if schema == 19 && binary == old {
					if e == nil {
						t.Fatal("old reader restored19")
					}
					if _, x := os.Stat(output); !os.IsNotExist(x) {
						t.Fatal("old reader wrote output")
					}
				} else {
					if e != nil {
						t.Fatalf("reader positive %v %s", e, out)
					}
					checkRestoredHistory(t, output, files)
				}
				after, e := os.ReadFile(receipt)
				must(t, e)
				if string(after) != string(before) {
					t.Fatal("ready receipt altered")
				}
			}
			var pins int
			must(t, open().QueryRow("SELECT coalesce(sum(pin_count),0) FROM chunk").Scan(&pins))
			if pins != 0 {
				t.Fatal("pins changed")
			}
			// Every profile's real retained receipt blocks every maintenance action.
			dir, e := os.MkdirTemp("/dev/shm", "coldkeep-ready-refusal-")
			must(t, e)
			defer func() { must(t, os.RemoveAll(dir)) }()
			for _, action := range []BindingAction{BindingInitial, BindingRelocate, BindingReconnect, BindingSourceRegister, BindingSourceRebind, BindingSourceDisable} {
				id, e := bindingID("bind")
				must(t, e)
				q := BindingRequest{Action: action, InstallationDirectory: dir, ExpectedSchema: 19, ExpectedRevision: 1, OperationID: id}
				if action == BindingInitial {
					q.ExpectedSchema = 18
					q.ExpectedRevision = 0
				}
				if strings.HasPrefix(string(action), "source-") {
					q.SourceID, _ = db.NewIdentity("source")
					if action != BindingSourceDisable {
						q.SourcePathBase64, _ = EncodeBindingPath(t.TempDir())
					}
					if action != BindingSourceRegister {
						q.ExpectedSourceRevision = 1
					}
				}
				_, e = ApplyBinding(context.Background(), Config(d), BindingOptions{Request: q, PlanSHA256: strings.Repeat("0", 64), Export: exportOptions(t)})
				if !errors.Is(e, ErrRetainedAuthority) {
					t.Fatal("receipt maintenance refusal", action, e)
				}
			}
			if !imported.Ready {
				t.Fatal(imported)
			}
		})
	}
}
