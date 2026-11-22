package recoveryexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
)

func TestRecoveryImportReceiptPublicationBudget(t *testing.T) {
	f, _, o := importFixture(t)
	for _, kind := range []string{"bytes", "objects", "compatible"} {
		t.Run(kind, func(t *testing.T) {
			d, _ := importTarget(t, f)
			h := &importHooks{receipt: func(r *ImportReceipt) {
				switch kind {
				case "bytes":
					r.Objects[0].Name = strings.Repeat("x", int(ImportReceiptMaxBytes)+1)
				case "objects":
					r.Objects = make([]Object, ImportReceiptMaxObjects+1)
				}
			}}
			r, e := importPreserved(t, f, d, o, h)
			if kind == "compatible" {
				must(t, e)
				if !r.Ready || !r.Published {
					t.Fatal("compatible receipt not published")
				}
				root, e := os.OpenRoot(d.PayloadRoot)
				must(t, e)
				defer root.Close()
				got, e := ReadImportReceipt(context.Background(), root)
				must(t, e)
				if len(got.Objects) != len(r.Receipt.Objects) || got.Objects[0] != r.Receipt.Objects[0] {
					t.Fatal("published receipt differs")
				}
			} else {
				if !errors.Is(e, ErrLimit) || r.Ready || r.Published {
					t.Fatalf("over-budget publication: %+v %v", r, e)
				}
				if _, e = os.Stat(filepath.Join(d.PayloadRoot, ".coldkeep-control", "import-ready.pending")); !os.IsNotExist(e) {
					t.Fatal("over-budget pending receipt", e)
				}
			}
		})
	}
}

func TestRecoveryReceiptBoundDerivation(t *testing.T) {
	l := DefaultLimits()
	if ImportReceiptMaxBytes != l.DiagnosticBytes+(64<<10) || ImportReceiptMaxObjects != l.Rows || ImportReceiptMaxObjects != l.DirectoryEntries || ImportReceiptMaxObjects+1 != DefaultExportLimits().Members {
		t.Fatal("admitted profile/receipt bounds diverged")
	}
	// All variable non-object fields at their maxima, including worst-case JSON
	// escaping; the encoded overhead must remain inside the explicit reserve.
	r := ImportReceipt{Kind: "coldkeep-import-ready/v1", Generation: strings.Repeat("a", 32), ManifestSHA256: strings.Repeat("b", 64), LegacyRootObservation: strings.Repeat("c", 64), TargetRoot: strings.Repeat("\x01", 4096), TargetRootHash: strings.Repeat("d", 64), Host: strings.Repeat("\x01", 4096), Port: 65535, Database: strings.Repeat("a", 63), DatabaseOID: 1<<63 - 1, Role: strings.Repeat("a", 63), Profile: "manual-postgresql-docker", Reader: importReader, KeyRequired: true, KeyReference: strings.Repeat("\x01", 128), DatabaseCapacityBytes: 1<<63 - 1, PhysicalIndependence: "unknown"}
	b, e := json.Marshal(r)
	must(t, e)
	if len(b) > 64<<10 {
		t.Fatal("receipt overhead exceeds reserve", len(b))
	}
	_, e = decodeImportReceipt(context.Background(), b)
	must(t, e)
	t.Logf("maximum non-object encoding=%d bytes; object encoding already inside %d-byte diagnostic", len(b), l.DiagnosticBytes)
}

type deadlineProbeConnector struct {
	driver.Connector
	observe func(context.Context)
}

func (c deadlineProbeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.observe(ctx)
	return nil, errors.New("deadline observation complete")
}

func TestRecoveryOwnedInventoryDeadline(t *testing.T) {
	f := exportFixture(t)
	r, e := resolve(context.Background(), f.config)
	must(t, e)
	defer clear(r.key)
	lease, e := coordination.AcquireExisting(context.Background(), r.identity)
	must(t, e)
	defer lease.Release()
	for _, kind := range []string{"earlier-parent", "lower-inventory", "expired-parent"} {
		t.Run(kind, func(t *testing.T) {
			deadline := time.Now().Add(time.Minute)
			if kind == "expired-parent" {
				deadline = time.Unix(1, 0)
			}
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			l := DefaultLimits()
			if kind == "lower-inventory" {
				l.Duration = time.Second
			}
			called := false
			h := &hooks{wrapConnector: func(c driver.Connector) driver.Connector {
				return deadlineProbeConnector{c, func(work context.Context) {
					called = true
					got, ok := work.Deadline()
					if !ok {
						t.Error("missing deadline")
					}
					if kind == "earlier-parent" && !got.Equal(deadline) {
						t.Error("parent deadline extended")
					}
					if kind == "lower-inventory" && !got.Before(deadline) {
						t.Error("inventory deadline ignored")
					}
				}}
			}}
			got, e := inspectOwnedCut(ctx, f.config, r, l, func(context.Context, resolved, *sql.Tx, *Inventory) error { return nil }, h, ownedCut{r.identity, lease})
			if e == nil || got.Complete {
				t.Fatal("probe unexpectedly completed")
			}
			if kind == "expired-parent" {
				if called || !errors.Is(e, context.DeadlineExceeded) {
					t.Fatal("expired work opened active connection", e)
				}
			} else if !called {
				t.Fatal("missing deadline observation")
			}
			must(t, lease.Check())
		})
	}
}

func TestRecoveryImportReceiptScaleRestore(t *testing.T) {
	f, files := recoveryHistory(t, true)
	// A valid existing header includes its real CRC/version/max size. Independent
	// 64-byte copies have no tasks; full inventory still admits every container.
	data, e := os.ReadFile(payloadPath(t, f))
	must(t, e)
	var maximum int64
	must(t, f.db.QueryRow("SELECT max_size FROM container WHERE id=1").Scan(&maximum))
	const count = 3600
	for i := 1000; i < 1000+count; i++ {
		name := fmt.Sprintf("receipt_%06d_%s.bin", i, strings.Repeat("x", 160))
		must(t, os.WriteFile(filepath.Join(f.root, name), data[:64], 0600))
	}
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) SELECT id,'receipt_'||lpad(id::text,6,'0')||'_'||repeat('x',160)||'.bin',64,$1 FROM generate_series(1000,4599) id`, maximum)
	o := exportOptions(t)
	_, e = exportPreserved(t, f, o, nil)
	must(t, e)
	check, e := Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	if len(check.Manifest.Members) != count+3 {
		t.Fatal("fixture object count", len(check.Manifest.Members))
	}
	dir := os.Getenv("COLDKEEP_PHASE1D_REPORT_DIR")
	if dir == "" {
		dir = t.TempDir()
		t.Setenv("COLDKEEP_PHASE1D_REPORT_DIR", dir)
	}
	beforeTree, beforeCatalog := treeFingerprint(t, f.root), catalogFingerprint(t, f.db)
	isolatedRecovery(t, f, o, files, "", "receipt-scale")
	if beforeTree != treeFingerprint(t, f.root) || beforeCatalog != catalogFingerprint(t, f.db) {
		t.Fatal("large recovery changed source")
	}
	b, e := os.ReadFile(filepath.Join(dir, "receipt-scale.json"))
	must(t, e)
	var report struct {
		Import ImportResult `json:"import"`
	}
	must(t, json.Unmarshal(b, &report))
	receipt, e := json.Marshal(report.Import.Receipt)
	must(t, e)
	if len(receipt) <= 1<<20 || int64(len(receipt)) > ImportReceiptMaxBytes || len(report.Import.Receipt.Objects) != count+2 {
		t.Fatalf("scale receipt bytes=%d", len(receipt))
	}
	last := report.Import.Receipt.Objects[len(report.Import.Receipt.Objects)-1]
	if last.ID != 4599 || !strings.HasPrefix(last.Name, "receipt_004599_") {
		t.Fatal("last object lost", last)
	}
	t.Logf("F1 real valid package: receipt=%d bytes objects=%d catalog_rows=%d; actual staged import/restore; nonempty history/Case.txt expected=8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c independently reread", len(receipt), len(report.Import.Receipt.Objects), check.Manifest.CatalogRows)
}
