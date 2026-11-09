package recoveryexport

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBindingIntentConflicts(t *testing.T) {
	f, o := bindingFixture(t)
	fault := errors.New("prepared intent cut")
	_, e := applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
		if s == "after-intent" {
			return fault
		}
		return nil
	}})
	if !errors.Is(e, fault) {
		t.Fatal(e)
	}
	path := filepath.Join(o.Request.InstallationDirectory, bindingIntentName)
	raw, e := os.ReadFile(path)
	must(t, e)
	for _, kind := range []string{"future", "plan", "request", "descriptor", "prior", "duplicate", "trailing", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			defer func() { must(t, os.WriteFile(path, raw, 0600)) }()
			var intent bindingIntent
			must(t, json.Unmarshal(raw, &intent))
			switch kind {
			case "future":
				intent.Kind = "coldkeep-binding-intent/v99"
			case "plan":
				intent.PlanSHA256 = "bad"
			case "request":
				intent.Request.OperationID, _ = bindingID("bind")
			case "descriptor":
				intent.Result.Descriptor.Locator.User = "different"
			case "prior":
				intent.PriorPlan = "bad"
			}
			b := []byte(bindingJSON(intent))
			if kind == "duplicate" {
				b = append([]byte(`{"kind":"bad",`), b[1:]...)
			}
			if kind == "trailing" {
				b = append(b, []byte(" {}")...)
			}
			if kind == "oversized" {
				b = make([]byte, BindingIntentMaxBytes+1)
			}
			must(t, os.WriteFile(path, b, 0600))
			before := catalogFingerprint(t, f.db)
			r, e := ReconcileBinding(context.Background(), f.config, o)
			if e == nil || r.Status == "complete; bounded maintenance only" {
				t.Fatal(r, e)
			}
			if before != catalogFingerprint(t, f.db) {
				t.Fatal("conflicting intent changed catalog")
			}
		})
	}
	_, e = ReconcileBinding(context.Background(), f.config, o)
	must(t, e)
	t.Run("pending-staging-status", func(t *testing.T) {
		pending := path + ".pending"
		must(t, os.WriteFile(pending, []byte("{"), 0600))
		defer func() { must(t, os.Remove(pending)) }()
		before := catalogFingerprint(t, f.db)
		for _, config := range []*Config{nil, &f.config} {
			r, e := BindingStatus(context.Background(), o.Request.InstallationDirectory, config)
			if !errors.Is(e, ErrBindingReconciliation) || r.Status != "reconciliation-required" {
				t.Fatal(r, e)
			}
		}
		if before != catalogFingerprint(t, f.db) {
			t.Fatal("passive status changed catalog")
		}
	})
	t.Run("missing-descriptor-no-reconstruction", func(t *testing.T) {
		must(t, os.Remove(filepath.Join(o.Request.InstallationDirectory, bindingDescriptorName)))
		_, e := ReconcileBinding(context.Background(), f.config, o)
		if e == nil {
			t.Fatal("reconstructed descriptor without intent")
		}
	})
}
func TestBindingNativeByteRootMapping(t *testing.T) {
	f, o := bindingFixture(t)
	path := filepath.Join(filepath.Dir(f.root), string([]byte{' ', 'R', 'o', 'o', 't', '-', 0xff, ' '}))
	must(t, os.Rename(f.root, path))
	f.root = path
	f.config.PayloadRoot = path
	p, e := PreviewBinding(context.Background(), f.config, o.Request, DefaultLimits())
	must(t, e)
	o.PlanSHA256 = p.PlanSHA256
	r, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	decoded, e := DecodeBindingPath(r.Descriptor.RootPathBase64)
	must(t, e)
	if decoded != path {
		t.Fatal("native root bytes altered")
	}
	var actual string
	must(t, f.db.QueryRow("SELECT path_base64 FROM root_binding_revision WHERE revision=1").Scan(&actual))
	if actual != r.Descriptor.RootPathBase64 {
		t.Fatal("SQL path changed")
	}
	config := readerConfig(t, Destination(f.config))
	b, e := os.ReadFile(config)
	must(t, e)
	var values map[string]any
	must(t, json.Unmarshal(b, &values))
	delete(values, "payload_root")
	values["payload_root_base64"] = actual
	must(t, os.WriteFile(config, []byte(bindingJSON(values)), 0600))
	binary := os.Getenv("COLDKEEP_RECOVERY_READER")
	if binary == "" {
		t.Fatal("staged reader required")
	}
	out, e := runReader(t, binary, "binding-check", "-config", config, "-installation-directory", o.Request.InstallationDirectory)
	if e != nil {
		t.Fatalf("exact operator path %v %s", e, out)
	}
}

func TestBindingCommittedIntentConflicts(t *testing.T) {
	f, o := bindingFixture(t)
	_, e := ApplyBinding(context.Background(), f.config, o)
	must(t, e)
	o.Request.Action = BindingReconnect
	o.Request.ExpectedSchema = 19
	o.Request.ExpectedRevision = 1
	o.Request.OperationID, e = bindingID("bind")
	must(t, e)
	o.Export.Destination = filepath.Join(filepath.Dir(o.Export.Destination), "committed-intent-checkpoint")
	p, e := PreviewBinding(context.Background(), f.config, o.Request, DefaultLimits())
	must(t, e)
	o.PlanSHA256 = p.PlanSHA256
	fault := errors.New("committed intent cut")
	_, e = applyBinding(context.Background(), f.config, o, &bindingHooks{event: func(s string, _ *sql.Tx) error {
		if s == "after-commit" {
			return fault
		}
		return nil
	}})
	if !errors.Is(e, fault) {
		t.Fatal(e)
	}
	path := filepath.Join(o.Request.InstallationDirectory, bindingIntentName)
	raw, e := os.ReadFile(path)
	must(t, e)
	for _, kind := range []string{"prior-descriptor", "generation", "manifest", "source"} {
		t.Run(kind, func(t *testing.T) {
			defer func() { must(t, os.WriteFile(path, raw, 0600)) }()
			var intent bindingIntent
			must(t, json.Unmarshal(raw, &intent))
			switch kind {
			case "prior-descriptor":
				intent.Result.Prior.Locator.User = "different"
			case "generation":
				intent.Result.Generation = "different"
			case "manifest":
				intent.Result.ManifestSHA256 = "different"
			case "source":
				intent.Result.SourceID = "different"
			}
			must(t, os.WriteFile(path, []byte(bindingJSON(intent)), 0600))
			before := catalogFingerprint(t, f.db)
			r, e := ReconcileBinding(context.Background(), f.config, o)
			if e == nil || r.Status == "complete; bounded maintenance only" {
				t.Fatal(r, e)
			}
			if before != catalogFingerprint(t, f.db) {
				t.Fatal("conflicting committed intent changed catalog")
			}
		})
	}
	_, e = ReconcileBinding(context.Background(), f.config, o)
	must(t, e)
}
