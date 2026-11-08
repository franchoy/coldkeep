package main

// Green tests use the dedicated production reader; exact submitted generic-helper
// probes and failures are retained in the external evidence.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/recoveryexport"
)

func recoveryReceiptScaleFixture(t *testing.T) (config, recoveryexport.ImportReceipt, []byte) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "payload")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	id, err := coordination.ResolveIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	c := config{Host: "127.0.0.1", Port: 5432, Database: "receipt_probe", User: "receipt_owner", PayloadRoot: root}
	r := recoveryexport.ImportReceipt{
		Kind: "coldkeep-import-ready/v1", Generation: strings.Repeat("a", 32),
		ManifestSHA256: strings.Repeat("b", 64), LegacyRootObservation: strings.Repeat("c", 64),
		TargetRoot: root, TargetRootHash: id.Hash, Host: c.Host, Port: c.Port,
		Database: c.Database, DatabaseOID: 16384, Role: c.User,
		Profile:               "manual-postgresql-docker",
		Reader:                "Coldkeep schema-17, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM",
		DatabaseCapacityBytes: 768 << 20, PhysicalIndependence: "unknown",
	}
	for i := 1; i <= 6000; i++ {
		r.Objects = append(r.Objects, recoveryexport.Object{
			ID: int64(i), Name: fmt.Sprintf("container_%06d_%s.bin", i, strings.Repeat("x", 160)),
			Size: 64, RequiredEnd: 64, SHA256: strings.Repeat("d", 64), FormatMajor: 1,
		})
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) <= 1<<20 || int64(len(b)) > recoveryexport.DefaultLimits().DiagnosticBytes ||
		len(r.Objects)+5 > recoveryexport.DefaultLimits().Rows ||
		len(r.Objects)+1 > recoveryexport.DefaultExportLimits().Members {
		t.Fatalf("bad bounded scale fixture: bytes=%d objects=%d", len(b), len(r.Objects))
	}
	return c, r, b
}

func TestRecoveryReaderReceiptWithinAdvertisedScale(t *testing.T) {
	_, want, b := recoveryReceiptScaleFixture(t)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".coldkeep-control"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, ".coldkeep-control", "IMPORT_READY")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	anchor, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.Close()
	got, err := recoveryexport.ReadImportReceipt(context.Background(), anchor)
	if err != nil {
		t.Fatalf("published supported-size receipt must be readable: bytes=%d objects=%d: %v", len(b), len(want.Objects), err)
	}
	if len(got.Objects) != len(want.Objects) || got.Objects[len(got.Objects)-1] != want.Objects[len(want.Objects)-1] {
		t.Fatal("receipt was truncated or changed")
	}
}

func TestRecoveryRestoreConsumesLargeReceiptBeforeBackend(t *testing.T) {
	c, _, b := recoveryReceiptScaleFixture(t)
	control := filepath.Join(c.PayloadRoot, ".coldkeep-control")
	if err := os.Mkdir(control, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "IMPORT_READY"), b, 0600); err != nil {
		t.Fatal(err)
	}
	// The submitted adapter checks this after receipt decoding and before any
	// SQL connector/lease use. Reaching the guard proves the actual consumer
	// decoded its receipt; the test never needs a database or a network request.
	t.Setenv("PG_COLDKEEP_RECEIPT_PROBE", "1")
	err := restore(context.Background(), c, "unused", nil, "selected-history", filepath.Join(t.TempDir(), "output"))
	if err == nil || err.Error() != "ambient database/key settings refused" {
		t.Fatalf("restore failed before the post-receipt guard: %v", err)
	}
}
