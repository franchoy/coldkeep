package recoveryexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"golang.org/x/sys/unix"
)

// Destination is an explicit WRITE-intent boundary. The database must be
// operator-provisioned from template0, exclusively controlled and pristine.
// PayloadRoot must be absent. No source connection/configuration is accepted.
type Destination Config

type ImportOptions struct {
	Package, Generation, ManifestSHA256    string
	TrustedOperatorBackup, ExclusiveTarget bool
	Tools                                  Tools
	Limits                                 ExportLimits
	InventoryLimits                        Limits
	// Operator-attested dedicated PostgreSQL heap/index/temp/WAL capacity.
	// This is a budget requirement, not a generic SQL resource sandbox.
	DatabaseCapacityBytes int64
}

type ImportResult struct {
	Status    string         `json:"status"`
	Published bool           `json:"published"`
	Ready     bool           `json:"ready"`
	Receipt   *ImportReceipt `json:"receipt,omitempty"`
}

type ImportReceipt struct {
	Kind                  string   `json:"kind"`
	Generation            string   `json:"generation"`
	ManifestSHA256        string   `json:"manifest_sha256"`
	LegacyRootObservation string   `json:"legacy_root_observation"`
	TargetRoot            string   `json:"target_root"`
	TargetRootHash        string   `json:"target_root_hash"`
	Host                  string   `json:"host"`
	Port                  uint16   `json:"port"`
	Database              string   `json:"database"`
	DatabaseOID           int64    `json:"database_oid"`
	Role                  string   `json:"role"`
	Profile               string   `json:"profile"`
	Reader                string   `json:"reader"`
	KeyRequired           bool     `json:"key_required"`
	KeyReference          string   `json:"key_reference,omitempty"`
	Objects               []Object `json:"objects"`
	DatabaseCapacityBytes int64    `json:"database_capacity_bytes"`
	PhysicalIndependence  string   `json:"physical_independence"`
}

const importReader = "Coldkeep schema-17, container-major-0/1, legacy-v1, CKBL-v1, none/zstd, plain/AES-GCM"
const importReserve int64 = 16 << 20
const importLock int64 = 0x434b494d504f5254

// Import never drops a database or reuses a failed destination. A published
// error requires reconciliation; preserve the target even if Ready is false.
func Import(ctx context.Context, d Destination, o ImportOptions) (ImportResult, error) {
	return importGeneration(ctx, d, o, nil)
}

type importHooks struct {
	files       *exportHooks
	cut         *hooks
	event       func(string) error
	credentials func(func() error) error
	release     func(func() error) error
	receipt     func(*ImportReceipt)
}

func (h *importHooks) at(s string) error {
	if h != nil && h.event != nil {
		return h.event(s)
	}
	return nil
}
func (h *importHooks) fileHooks() *exportHooks {
	if h != nil {
		return h.files
	}
	return nil
}

func importGeneration(ctx context.Context, d Destination, o ImportOptions, h *importHooks) (result ImportResult, err error) {
	result.Status = "incomplete; no usable handoff"
	defer func() {
		if err != nil {
			result.Ready = false
			if result.Published {
				result.Status = "published-or-uncertain; preserve target and reconcile"
			}
		}
	}()
	if ctx == nil || !o.TrustedOperatorBackup || !o.ExclusiveTarget || !o.Limits.valid() || !o.InventoryLimits.valid() ||
		!safeGeneration.MatchString(o.Generation) || !safeDigest.MatchString(o.ManifestSHA256) || o.DatabaseCapacityBytes < 64<<20 ||
		!scalarName.MatchString(d.Database) || !scalarName.MatchString(d.User) {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, o.Limits.Duration)
	defer cancel()
	// Anchor package identity and check exactly these bytes before any SQL.
	packagePath, e := filepath.EvalSymlinks(o.Package)
	if e != nil || !filepath.IsAbs(o.Package) || packagePath != o.Package {
		return result, ErrPackage
	}
	info, e := os.Lstat(packagePath)
	if e != nil || !info.IsDir() {
		return result, ErrPackage
	}
	source, e := os.OpenRoot(packagePath)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, source.Close())
		if err != nil {
			result.Ready = false
		}
	}()
	m, raw, e := checkGeneration(ctx, source, o.Limits, true)
	if e != nil {
		return result, e
	}
	digest := sha256.Sum256(raw)
	if m.Generation != o.Generation || hex.EncodeToString(digest[:]) != o.ManifestSHA256 || m.Reader != importReader {
		return result, ErrPackage
	}
	if e = validateImportNames(m); e != nil {
		return result, e
	}
	if _, _, e = validateTools(ctx, o.Tools); e != nil {
		return result, e
	}
	// Resolution invokes secrets once and rejects resolver-introduced PG state.
	// Resolve uses the package solely as an existing identity anchor. The target
	// root is checked before creation below; no source DB information is needed.
	c := Config(d)
	c.PayloadRoot = packagePath
	r, e := resolve(ctx, c)
	if e != nil {
		return result, e
	}
	defer clear(r.key)
	if (m.KeyRequired && (len(r.key) != 32 || r.keyRef != m.KeyReference)) || (!m.KeyRequired && (len(r.key) != 0 || r.keyRef != "")) {
		return result, ErrKeyUnavailable
	}
	if e = validateImportPath(packagePath, d.PayloadRoot); e != nil {
		return result, e
	}
	reservation, e := reserveImport(ctx, r)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, reservation.close())
		if err != nil {
			result.Ready = false
			if result.Published {
				result.Status = "published-or-uncertain; preserve target and reconcile"
			}
		}
	}()
	oid, e := reservation.fresh(ctx, d, m)
	if e != nil {
		return result, e
	}
	if e = h.at("database-reserved"); e != nil {
		return result, e
	}
	dest, e := newGeneration(packagePath, d.PayloadRoot)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, dest.close())
		if err != nil {
			result.Ready = false
			if result.Published {
				result.Status = "published-or-uncertain; preserve target and reconcile"
			}
		}
	}()
	if e = coordination.PrepareExisting(ctx, dest.path); e != nil {
		return result, e
	}
	id, e := coordination.ResolveIdentity(dest.path)
	if e != nil {
		return result, e
	}
	lease, e := coordination.AcquireExisting(ctx, id)
	if e != nil {
		return result, e
	}
	defer func() {
		var releaseErr error
		if h != nil && h.release != nil {
			releaseErr = h.release(lease.Release)
		} else {
			releaseErr = lease.Release()
		}
		err = errors.Join(err, releaseErr)
		if err != nil {
			result.Ready = false
			if result.Published {
				result.Status = "published-or-uncertain; preserve target and reconcile"
			}
		}
	}()
	if _, e = reservation.fresh(ctx, d, m); e != nil {
		return result, e
	}
	if e = errors.Join(dest.check(), lease.Check(), h.at("owned")); e != nil {
		return result, e
	}
	need := importReserve
	for _, member := range m.Members {
		need += member.Size
	}
	if e = dest.capacity(need); e != nil {
		return result, e
	}
	control, e := dest.root.OpenRoot(coordination.ControlDirectoryName)
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, control.Close()) }()
	fh := h.fileHooks()
	// Every consumed member is copied through a fresh regular handle and hashed
	// against the bound manifest. No pathname-only earlier CheckResult is trusted.
	for i, member := range m.Members {
		root, name := dest.root, member.SourceName
		if i == 0 {
			root, name = control, "catalog.dump"
		}
		obj := Object{Name: member.Name, Size: member.Size, SHA256: member.SHA256}
		if _, e = copyObject(ctx, source, root, obj, name, fh); e != nil {
			return result, e
		}
	}
	if e = h.at("copied"); e != nil {
		return result, e
	}
	if e = checkPackageIdentity(ctx, source, packagePath, info, raw, o.Limits); e != nil {
		return result, e
	}
	if e = restoreImport(ctx, r, packagePath, dest, control, o, m.Members[0], h); e != nil {
		return result, e
	}
	if e = h.at("restored"); e != nil {
		return result, e
	}
	r.identity = id
	c.PayloadRoot = dest.path
	var cutHooks *hooks
	if h != nil {
		cutHooks = h.cut
	}
	var admitted []Object
	checked, e := inspectOwnedCut(ctx, c, r, o.InventoryLimits, func(ctx context.Context, _ resolved, tx *sql.Tx, inv *Inventory) error {
		if _, x := databaseRequirements(ctx, tx); x != nil {
			return x
		}
		if x := admitImport(m, inv); x != nil {
			return x
		}
		admitted = append([]Object(nil), inv.Objects...)
		return nil
	}, cutHooks, ownedCut{id, lease})
	if e != nil || !checked.Complete {
		return result, errors.Join(e, ErrCorrupt)
	}
	if e = h.at("active-cleaned"); e != nil {
		return result, e
	}
	// Remove only private working archive, after every child/session/credential
	// join. The package remains untouched and independently usable.
	if e = control.Remove("catalog.dump"); e != nil {
		return result, e
	}
	receipt := &ImportReceipt{Kind: "coldkeep-import-ready/v1", Generation: m.Generation, ManifestSHA256: o.ManifestSHA256,
		LegacyRootObservation: m.LegacyRootHash, TargetRoot: dest.path, TargetRootHash: id.Hash,
		Host: d.Host, Port: d.Port, Database: d.Database, DatabaseOID: oid, Role: d.User, Profile: d.Profile,
		Reader: m.Reader, KeyRequired: m.KeyRequired, KeyReference: m.KeyReference, Objects: admitted,
		DatabaseCapacityBytes: o.DatabaseCapacityBytes, PhysicalIndependence: "unknown"}
	if h != nil && h.receipt != nil {
		h.receipt(receipt)
	}
	if e = writeImportReceipt(ctx, control, receipt, fh); e != nil {
		return result, e
	}
	dir, e := control.Open(".")
	if e != nil {
		return result, e
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	parentDir, e := dest.parent.Open(".")
	if e != nil {
		return result, e
	}
	e = errors.Join(fh.syncFile(parentDir), parentDir.Close())
	if e != nil {
		return result, e
	}
	if e = errors.Join(fh.syncFile(dir), dest.sync(fh), dest.check(), lease.Check(), reservation.check(ctx), ctx.Err(), h.at("before-ready")); e != nil {
		return result, e
	}
	if e = errors.Join(ctx.Err(), dest.check(), lease.Check(), reservation.check(ctx)); e != nil {
		return result, e
	}
	if e = unix.Renameat2(int(dir.Fd()), "import-ready.pending", int(dir.Fd()), "IMPORT_READY", unix.RENAME_NOREPLACE); e != nil {
		return result, e
	}
	result.Published, result.Receipt = true, receipt
	result.Status = "published-or-uncertain; preserve target and reconcile"
	if e = errors.Join(fh.syncFile(dir), dest.sync(fh), lease.Check(), reservation.check(ctx), h.at("ready-published")); e != nil {
		return result, e
	}
	result.Ready, result.Status = true, "import-ready; reader/key required; physical independence unknown"
	return result, nil
}

func validateImportNames(m Manifest) error {
	seen := map[string]bool{}
	for _, member := range m.Members[1:] {
		if _, e := container.SafeContainerPath("/", member.SourceName); e != nil || strings.HasPrefix(strings.ToLower(member.SourceName), ".coldkeep") || seen[member.SourceName] {
			return ErrPackage
		}
		seen[member.SourceName] = true
	}
	return nil
}

func validateImportPath(pkg, target string) error {
	if !filepath.IsAbs(target) || filepath.Clean(target) != target || len(target) > 4096 {
		return ErrConfiguration
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(target))
	if e != nil || parent != filepath.Dir(target) || overlap(pkg, filepath.Join(parent, filepath.Base(target))) {
		return ErrConfiguration
	}
	if _, e = os.Lstat(target); !os.IsNotExist(e) {
		return ErrConfiguration
	}
	return nil
}

func checkPackageIdentity(ctx context.Context, root *os.Root, path string, info os.FileInfo, raw []byte, limits ExportLimits) error {
	after, e := os.Lstat(path)
	if e != nil || !os.SameFile(info, after) {
		return ErrPackage
	}
	_, got, e := checkGeneration(ctx, root, limits, true)
	if e != nil {
		return e
	}
	if string(got) != string(raw) {
		return ErrPackage
	}
	return nil
}

func restoreImport(ctx context.Context, r resolved, pkg string, dest *generationDirectory, control *os.Root, o ImportOptions, member Member, h *importHooks) (err error) {
	archive, e := openRegular(control, "catalog.dump")
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, archive.Close()) }()
	fh := h.fileHooks()
	n, digest, e := hashFile(ctx, archive, member.Size)
	if e != nil || n != member.Size || digest != member.SHA256 {
		return errors.Join(e, ErrPackage)
	}
	if _, e = archive.Seek(0, io.SeekStart); e != nil {
		return e
	}
	if e = runChild(ctx, o.Tools.PGRestore, []string{"--no-owner", "--no-acl", "--file=-", "/proc/self/fd/3"}, []string{"LC_ALL=C"}, archive, io.Discard, 512<<20, fh); e != nil {
		return e
	}
	if _, e = archive.Seek(0, io.SeekStart); e != nil {
		return e
	}
	// Credentials are outside package AND target; original passive environment
	// remains read-only. Only this dedicated destination child can write SQL.
	r.identity.CanonicalPath = pkg
	env, remove, e := childCredentials(r, dest.path)
	if remove != nil {
		defer func() {
			if h != nil && h.credentials != nil {
				err = errors.Join(err, h.credentials(remove))
			} else {
				err = errors.Join(err, remove())
			}
		}()
	}
	if e != nil {
		return e
	}
	for i, v := range env {
		if strings.HasPrefix(v, "PGOPTIONS=") {
			env[i] = "PGOPTIONS=-c default_transaction_read_only=off -c row_security=off -c statement_timeout=60000 -c lock_timeout=1000"
		}
		if strings.HasPrefix(v, "PGAPPNAME=") {
			env[i] = "PGAPPNAME=coldkeep-recovery-import"
		}
	}
	args := []string{"--no-owner", "--no-acl", "--exit-on-error", "--single-transaction", "--no-password", "--host=" + r.pg.Host, "--port=" + strconv.Itoa(int(r.pg.Port)), "--username=" + r.pg.User, "--dbname=" + r.pg.Database, "/proc/self/fd/3"}
	return runChild(ctx, o.Tools.PGRestore, args, env, archive, io.Discard, 64<<10, fh)
}

func admitImport(m Manifest, inv *Inventory) error {
	if inv.CatalogVersion != m.CatalogVersion || inv.Rows != m.CatalogRows || len(catalogTables) != m.Tables || inv.Reader != m.Reader || inv.KeyRequired != m.KeyRequired || inv.KeyReference != m.KeyReference || len(inv.Objects)+1 != len(m.Members) {
		return fmt.Errorf("%w: manifest/catalog admission", ErrCorrupt)
	}
	objects := make(map[int64]Object, len(inv.Objects))
	for _, obj := range inv.Objects {
		objects[obj.ID] = obj
	}
	for _, member := range m.Members[1:] {
		obj, ok := objects[member.ContainerID]
		if !ok || obj.Name != member.SourceName || obj.Size != member.Size || obj.SHA256 != member.SHA256 || obj.FormatMajor != member.FormatMajor {
			return fmt.Errorf("%w: manifest/object admission", ErrCorrupt)
		}
	}
	return nil
}
