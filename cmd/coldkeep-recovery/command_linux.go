package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"

	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

type commandOptions struct {
	command        string
	pkg            string
	cfgPath        string
	gen            string
	digest         string
	trusted        bool
	exclusive      bool
	capacity       int64
	expectedSchema int
	operation      string
	plan           string
	checkpoint     string
	installation   string
	action         string
	revision       int64
	sourceID       string
	sourcePath     string
	sourceRevision int64
	repositoryID   string
	rootID         string
	snap           string
	out            string
}

func parseCommandOptions(args []string) (commandOptions, error) {
	var o commandOptions
	if len(args) < 2 {
		return o, errors.New("usage: coldkeep-recovery check|import|restore|adopt-preview|adopt-apply [explicit options]; see recovery-package.md")
	}
	o.command = args[1]
	fs := flag.NewFlagSet(args[1], flag.ContinueOnError)
	fs.StringVar(&o.pkg, "package", "", "completed generation path")
	fs.StringVar(&o.cfgPath, "config", "", "private explicit destination JSON")
	fs.StringVar(&o.gen, "generation", "", "trusted generation identity")
	fs.StringVar(&o.digest, "manifest-sha256", "", "trusted exact manifest SHA-256")
	fs.BoolVar(&o.trusted, "trusted-operator-backup", false, "consent to execute this trusted operator backup")
	fs.BoolVar(&o.exclusive, "exclusive-target", false, "operator controls exclusive access to dedicated target")
	fs.Int64Var(&o.capacity, "database-capacity-bytes", 0, "dedicated PostgreSQL heap/index/temp/WAL budget")
	fs.IntVar(&o.expectedSchema, "expected-schema", 0, "explicit adoption source schema")
	fs.StringVar(&o.operation, "operation-id", "", "typed adoption request identity")
	fs.StringVar(&o.plan, "plan-sha256", "", "reviewed preview digest")
	fs.StringVar(&o.checkpoint, "checkpoint", "", "fresh independent pre-adoption generation path")
	fs.StringVar(&o.installation, "installation-directory", "", "existing private directory on a separate mount")
	fs.StringVar(&o.action, "binding-action", "", "bind, relocate, rebind, source-register, source-rebind or source-disable")
	fs.Int64Var(&o.revision, "expected-revision", 0, "reviewed current root binding revision")
	fs.StringVar(&o.sourceID, "source-id", "", "distinct typed configured source ID")
	fs.StringVar(&o.sourcePath, "source-path-base64", "", "exact absolute source path bytes in base64")
	fs.Int64Var(&o.sourceRevision, "expected-source-revision", 0, "reviewed source revision")
	fs.StringVar(&o.repositoryID, "repository-id", "", "exact repository identity for schema upgrade")
	fs.StringVar(&o.rootID, "root-id", "", "exact root identity for schema upgrade")
	fs.StringVar(&o.snap, "snapshot", "", "exact selected snapshot ID")
	fs.StringVar(&o.out, "output", "", "fresh absolute restore output directory")
	if e := fs.Parse(args[2:]); e != nil {
		return o, e
	}
	if fs.NArg() != 0 {
		return o, errors.New("unexpected arguments")
	}
	return o, nil
}

func runUnconfigured(ctx context.Context, o commandOptions) (bool, error) {
	switch o.command {
	case "check":
		r, e := recoveryexport.Check(ctx, o.pkg, recoveryexport.DefaultExportLimits())
		if e != nil {
			return true, e
		}
		return true, json.NewEncoder(os.Stdout).Encode(r)
	case "binding-status":
		r, e := recoveryexport.BindingStatus(ctx, o.installation, nil)
		return true, errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	default:
		return false, nil
	}
}

// This frame owns the credentials until synchronous dispatch and encoding end.
func runConfigured(ctx context.Context, o commandOptions) error {
	c, e := readRecoveryConfig(o.cfgPath)
	if e != nil {
		return e
	}
	password, e := privateBytes(c.PasswordFile, 4096)
	if e != nil {
		return e
	}
	defer clear(password)
	var key []byte
	if c.KeyFile != "" {
		raw, e := privateBytes(c.KeyFile, 64)
		if e != nil {
			return e
		}
		key, e = hex.DecodeString(string(raw))
		clear(raw)
		if e != nil || len(key) != 32 {
			return recoveryexport.ErrKeyUnavailable
		}
		defer clear(key)
	}
	d := recoveryexport.Destination{Backend: "postgres", Profile: "manual-postgresql-docker", Transport: "local-plaintext", Host: c.Host, Port: c.Port, Database: c.Database, User: c.User, PayloadRoot: c.PayloadRoot, Password: func(context.Context) (string, error) { return string(password), nil }, KeyReference: c.KeyReference}
	if key != nil {
		d.Key = func(context.Context) ([]byte, error) { return key, nil }
	}
	return dispatchConfigured(ctx, o, c, d, password, key)
}

func dispatchConfigured(ctx context.Context, o commandOptions, c config, d recoveryexport.Destination, password, key []byte) error {
	switch o.command {
	case "upgrade-preview", "upgrade-apply", "upgrade-reconcile":
		return runUpgrade(ctx, o, c, d)
	case "binding-source-status", "binding-check", "binding-preview", "binding-apply", "binding-reconcile":
		return runBinding(ctx, o, c, d)
	default:
		return runImportRestoreAdoption(ctx, o, c, d, password, key)
	}
}

func runUpgrade(ctx context.Context, o commandOptions, c config, d recoveryexport.Destination) error {
	switch o.command {
	case "upgrade-preview":
		q := recoveryexport.UpgradeRequest{InstallationDirectory: o.installation, ExpectedSchema: o.expectedSchema, RepositoryID: o.repositoryID, RootID: o.rootID, ExpectedRevision: o.revision, OperationID: o.operation}
		r, e := recoveryexport.PreviewUpgrade(ctx, recoveryexport.Config(d), q, recoveryexport.DefaultLimits())
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "upgrade-apply", "upgrade-reconcile":
		q := recoveryexport.UpgradeRequest{InstallationDirectory: o.installation, ExpectedSchema: o.expectedSchema, RepositoryID: o.repositoryID, RootID: o.rootID, ExpectedRevision: o.revision, OperationID: o.operation}
		o := recoveryexport.UpgradeOptions{Request: q, PlanSHA256: o.plan, Export: recoveryexport.ExportOptions{Destination: o.checkpoint, Tools: recoveryexport.Tools{PGDump: c.PGDump, PGRestore: c.PGRestore}, Limits: recoveryexport.DefaultExportLimits(), InventoryLimits: recoveryexport.DefaultLimits()}}
		r, e := recoveryexport.ApplyUpgrade(ctx, recoveryexport.Config(d), o)
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	default:
		return errors.New("unknown recovery operation")
	}
}

func runBinding(ctx context.Context, o commandOptions, c config, d recoveryexport.Destination) error {
	request := recoveryexport.BindingRequest{Action: recoveryexport.BindingAction(o.action), InstallationDirectory: o.installation, ExpectedSchema: o.expectedSchema, ExpectedRevision: o.revision, OperationID: o.operation, SourceID: o.sourceID, SourcePathBase64: o.sourcePath, ExpectedSourceRevision: o.sourceRevision}
	switch o.command {
	case "binding-source-status":
		r, e := recoveryexport.SourceBindingStatus(ctx, o.installation, recoveryexport.Config(d), o.sourceID)
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "binding-check":
		selected := recoveryexport.Config(d)
		r, e := recoveryexport.BindingStatus(ctx, o.installation, &selected)
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "binding-preview":
		r, e := recoveryexport.PreviewBinding(ctx, recoveryexport.Config(d), request, recoveryexport.DefaultLimits())
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "binding-apply", "binding-reconcile":
		o := recoveryexport.BindingOptions{Request: request, PlanSHA256: o.plan, Export: recoveryexport.ExportOptions{Destination: o.checkpoint, Tools: recoveryexport.Tools{PGDump: c.PGDump, PGRestore: c.PGRestore}, Limits: recoveryexport.DefaultExportLimits(), InventoryLimits: recoveryexport.DefaultLimits()}}
		r, e := recoveryexport.ApplyBinding(ctx, recoveryexport.Config(d), o)
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	default:
		return errors.New("unknown recovery operation")
	}
}

func runImportRestoreAdoption(ctx context.Context, o commandOptions, c config, d recoveryexport.Destination, password, key []byte) error {
	switch o.command {
	case "adopt-preview":
		r, e := recoveryexport.PreviewAdoption(ctx, recoveryexport.Config(d), recoveryexport.DefaultLimits())
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "adopt-apply":
		r, e := recoveryexport.ApplyAdoption(ctx, recoveryexport.Config(d), recoveryexport.AdoptionOptions{ExpectedSchema: o.expectedSchema, OperationID: o.operation, PlanSHA256: o.plan, Export: recoveryexport.ExportOptions{Destination: o.checkpoint, Tools: recoveryexport.Tools{PGDump: c.PGDump, PGRestore: c.PGRestore}, Limits: recoveryexport.DefaultExportLimits(), InventoryLimits: recoveryexport.DefaultLimits()}})
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "import":
		r, e := recoveryexport.Import(ctx, d, recoveryexport.ImportOptions{Package: o.pkg, Generation: o.gen, ManifestSHA256: o.digest, TrustedOperatorBackup: o.trusted, ExclusiveTarget: o.exclusive, DatabaseCapacityBytes: o.capacity, Tools: recoveryexport.Tools{PGDump: c.PGDump, PGRestore: c.PGRestore}, Limits: recoveryexport.DefaultExportLimits(), InventoryLimits: recoveryexport.DefaultLimits()})
		return errors.Join(e, json.NewEncoder(os.Stdout).Encode(r))
	case "restore":
		return restore(ctx, c, string(password), key, o.snap, o.out)
	default:
		return errors.New("unknown recovery operation")
	}
}
