//go:build linux

package recoveryexport

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

func TestRecoveryCredentialDirectoryPlacement(t *testing.T) {
	for _, name := range []string{
		"source-equal", "source-descendant", "source-alias",
		"destination-equal", "destination-descendant", "destination-alias",
		"sibling-parent", "outside-alias",
	} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			source := filepath.Join(base, "source")
			destination := filepath.Join(base, "generation")
			must(t, os.Mkdir(source, 0700))
			must(t, os.Mkdir(destination, 0700))
			parent := base
			refuse := true
			switch name {
			case "source-equal":
				parent = source
			case "source-descendant":
				parent = filepath.Join(source, "temporary")
				must(t, os.Mkdir(parent, 0700))
			case "source-alias":
				parent = filepath.Join(base, "temporary-alias")
				must(t, os.Symlink(source, parent))
			case "destination-equal":
				parent = destination
			case "destination-descendant":
				parent = filepath.Join(destination, "temporary")
				must(t, os.Mkdir(parent, 0700))
			case "destination-alias":
				parent = filepath.Join(base, "temporary-alias")
				must(t, os.Symlink(destination, parent))
			case "sibling-parent":
				refuse = false
			case "outside-alias":
				target := filepath.Join(base, "temporary")
				must(t, os.Mkdir(target, 0700))
				parent = filepath.Join(base, "temporary-alias")
				must(t, os.Symlink(target, parent))
				refuse = false
			}
			identity, e := coordination.ResolveIdentity(source)
			must(t, e)
			canonicalDestination, e := filepath.EvalSymlinks(destination)
			must(t, e)
			canonicalParent, e := filepath.EvalSymlinks(parent)
			must(t, e)
			r := resolved{identity: identity}
			r.pg.Host, r.pg.Port = "127.0.0.1", 5432
			r.pg.Database, r.pg.User, r.pg.Password = "fixture", "reader", "fake:secret\\value"
			sourceChanged := watchMutations(t, source)
			destinationChanged := watchMutations(t, destination)
			env, remove, e := childCredentialsAt(r, canonicalDestination, parent)
			// Clean up even on an unexpected outcome, then inspect creation/removal
			// events. A final tree fingerprint alone misses create-and-undo.
			if remove != nil {
				t.Cleanup(func() {
					if remove != nil {
						must(t, remove())
					}
				})
			}
			if refuse {
				allocated := remove != nil
				if remove != nil {
					must(t, remove())
					remove = nil
				}
				mutatedSource, mutatedDestination := sourceChanged(), destinationChanged()
				if !errors.Is(e, ErrConfiguration) || len(env) != 0 || allocated || mutatedSource || mutatedDestination {
					t.Fatalf("pre-allocation refusal: error=%v env=%d allocated=%v source-events=%v destination-events=%v", e, len(env), allocated, mutatedSource, mutatedDestination)
				}
				return
			}
			must(t, e)
			if remove == nil {
				t.Fatal("successful allocation has no cleanup")
			}
			values := map[string]string{}
			for _, entry := range env {
				key, value, ok := strings.Cut(entry, "=")
				if !ok {
					t.Fatal("invalid child environment entry")
				}
				values[key] = value
			}
			dir, pass := values["HOME"], values["PGPASSFILE"]
			if filepath.Dir(dir) != canonicalParent || pass != filepath.Join(dir, "passfile") || overlap(dir, identity.CanonicalPath) || overlap(dir, canonicalDestination) {
				t.Fatalf("credential directory escaped the eligible canonical parent: %q", dir)
			}
			info, e := os.Stat(dir)
			must(t, e)
			if !info.IsDir() || info.Mode().Perm() != 0700 {
				t.Fatal("credential directory is not private")
			}
			info, e = os.Stat(pass)
			must(t, e)
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
				t.Fatal("passfile is not private")
			}
			content, e := os.ReadFile(pass)
			must(t, e)
			if string(content) != "127.0.0.1:5432:fixture:reader:fake\\:secret\\\\value\n" {
				t.Fatal("passfile contents or escaping changed")
			}
			must(t, remove())
			remove = nil
			if _, e = os.Lstat(dir); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("credential directory remains after cleanup: %v", e)
			}
			if sourceChanged() || destinationChanged() {
				t.Fatal("credential allocation or cleanup mutated a protected tree")
			}
		})
	}
}
