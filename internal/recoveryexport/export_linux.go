package recoveryexport

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/franchoy/coldkeep/v2/internal/coordination"
	"github.com/franchoy/coldkeep/v2/internal/version"
	"golang.org/x/sys/unix"
)

type ExportOptions struct {
	Destination     string
	Tools           Tools
	InventoryLimits Limits
	Limits          ExportLimits
}
type ExportResult struct {
	Generation  string `json:"generation"`
	Destination string `json:"destination"`
	Complete    bool   `json:"byte_complete"`
	Published   bool   `json:"published"`
	Status      string `json:"status"`
}

// Export creates a fresh generation. An error with Published=true must be
// reconciled with Check; it never authorizes deleting a completed generation.
func Export(ctx context.Context, c Config, o ExportOptions) (ExportResult, error) {
	return export(ctx, c, o, nil)
}
func export(ctx context.Context, c Config, o ExportOptions, h *exportHooks) (result ExportResult, err error) {
	return exportOwned(ctx, c, o, h, nil, nil)
}
func exportOwned(ctx context.Context, c Config, o ExportOptions, h *exportHooks, r *resolved, owned *ownedCut) (result ExportResult, err error) {
	result.Status = "incomplete"
	if ctx == nil || !o.Limits.valid() || !o.InventoryLimits.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, o.Limits.Duration)
	defer cancel()
	dumpVersion, restoreVersion, e := validateTools(ctx, o.Tools)
	if e != nil {
		return result, e
	}
	// Reject dangerous child scalar interpretation before creating a destination.
	if !scalarName.MatchString(c.Database) || !scalarName.MatchString(c.User) {
		return result, ErrConfiguration
	}
	id, e := coordination.ResolveIdentity(c.PayloadRoot)
	if e != nil {
		return result, e
	}
	dest, e := newGeneration(id.CanonicalPath, o.Destination)
	if e != nil {
		return result, e
	}
	defer func() {
		err = errors.Join(err, dest.close())
		if err != nil {
			result.Complete = false
			if result.Published {
				result.Status = "published; acknowledgement or durability uncertain; run Check"
			}
		}
	}()
	var generation [16]byte
	if _, e = rand.Read(generation[:]); e != nil {
		return result, e
	}
	result.Generation = hex.EncodeToString(generation[:])
	result.Destination = dest.path
	var manifest Manifest
	var cutHooks *hooks
	if h != nil {
		cutHooks = h.cut
	}
	inspectFn := func(ctx context.Context, c Config, l Limits, consume func(context.Context, resolved, *sql.Tx, *Inventory) error, h *hooks) (Result, error) {
		if owned != nil && r != nil {
			return inspectOwnedCut(ctx, c, *r, l, consume, h, *owned)
		}
		return inspectCut(ctx, c, l, consume, h)
	}
	inspected, e := inspectFn(ctx, c, o.InventoryLimits, func(cutCtx context.Context, r resolved, tx *sql.Tx, inv *Inventory) (cutErr error) {
		requirements, e := databaseRequirements(cutCtx, tx)
		if e != nil {
			return e
		}
		requirements.DumpVersion = dumpVersion
		requirements.RestoreVersion = restoreVersion
		var snapshot string
		if e = tx.QueryRowContext(cutCtx, "SELECT pg_export_snapshot()").Scan(&snapshot); e != nil {
			return errors.New("catalog snapshot export unavailable")
		}
		if !snapshotName.MatchString(snapshot) {
			return ErrUnsupported
		}
		manifest = Manifest{Version: profilePackage(inv.CatalogVersion), Generation: result.Generation, ApplicationVersion: version.String(), CatalogVersion: inv.CatalogVersion, Selection: "full-catalog", CutTime: time.Now().UTC().Format(time.RFC3339Nano), Snapshot: snapshot, LegacyRootHash: inv.RootHash, Reader: inv.Reader, KeyRequired: inv.KeyRequired, KeyReference: inv.KeyReference, KeyCustody: "unknown", ReaderCustody: "unknown", Independence: "unknown", Database: requirements, CatalogRows: inv.Rows, Tables: len(profileTables(inv.CatalogVersion)), Excluded: inv.Uncataloged}
		if inv.CatalogVersion >= 18 {
			manifest.RepositoryID = inv.Catalog["repository_identity"][0].s("repository_id")
		}
		if len(inv.Objects)+1 > o.Limits.Members {
			return ErrLimit
		}
		var payloadBytes int64
		for _, obj := range inv.Objects {
			if obj.Size > o.Limits.TotalBytes-payloadBytes {
				return ErrLimit
			}
			payloadBytes += obj.Size
		}
		if o.Limits.ArchiveBytes+o.Limits.ManifestBytes+4096 > o.Limits.TotalBytes-payloadBytes {
			return ErrLimit
		}
		if e = dest.capacity(payloadBytes + o.Limits.ArchiveBytes + o.Limits.ManifestBytes + 4096); e != nil {
			return e
		}
		if e = h.at("snapshot-exported"); e != nil {
			return e
		}
		env, remove, e := childCredentials(r, dest.path)
		if remove != nil {
			defer func() { cutErr = errors.Join(cutErr, h.cleanCredentials(remove)) }()
		}
		if e != nil {
			return e
		}
		archive, e := dest.root.OpenFile("catalog.dump", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		args := []string{"--format=custom", "--compress=0", "--no-password", "--lock-wait-timeout=1000", "--host=" + r.pg.Host, "--port=" + strconv.Itoa(int(r.pg.Port)), "--username=" + r.pg.User, "--dbname=" + r.pg.Database, "--snapshot=" + snapshot}
		e = runChild(cutCtx, o.Tools.PGDump, args, env, nil, &exportFileWriter{archive, h}, o.Limits.ArchiveBytes, h)
		e = errors.Join(e, h.syncFile(archive), h.closeFile(archive))
		if e != nil {
			return e
		}
		if e = h.at("archive-written"); e != nil {
			return e
		}
		archive, e = openRegular(dest.root, "catalog.dump")
		if e != nil {
			return e
		}
		e = runChild(cutCtx, o.Tools.PGRestore, []string{"--no-owner", "--no-acl", "--file=-", "/proc/self/fd/3"}, []string{"LC_ALL=C"}, archive, io.Discard, 512<<20, h)
		_, seekErr := archive.Seek(0, io.SeekStart)
		n, digest, hashErr := hashFile(cutCtx, archive, o.Limits.ArchiveBytes)
		e = errors.Join(e, seekErr)
		e = errors.Join(e, hashErr, archive.Close())
		if e != nil {
			return e
		}
		if n < 8 {
			return ErrPackage
		}
		manifest.Members = append(manifest.Members, Member{Name: "catalog.dump", Size: n, SHA256: digest})
		source, e := os.OpenRoot(r.identity.CanonicalPath)
		if e != nil {
			return e
		}
		defer func() { cutErr = errors.Join(cutErr, source.Close()) }()
		for i, obj := range inv.Objects {
			member, e := copyObject(cutCtx, source, dest.root, obj, fmt.Sprintf("object-%06d.bin", i+1), h)
			if e != nil {
				return e
			}
			manifest.Members = append(manifest.Members, member)
		}
		if e = h.at("payloads-written"); e != nil {
			return e
		}
		b, e := json.Marshal(manifest)
		if e != nil {
			return e
		}
		used := int64(len(b)) + 4096
		for _, member := range manifest.Members {
			if member.Size > o.Limits.TotalBytes-used {
				return ErrLimit
			}
			used += member.Size
		}
		if used > o.Limits.TotalBytes || int64(len(b)) > o.Limits.ManifestBytes {
			return ErrLimit
		}
		if e = writeMember(dest.root, "manifest.json", b, h); e != nil {
			return e
		}
		if _, _, e = checkGeneration(cutCtx, dest.root, o.Limits, false); e != nil {
			return e
		}
		return errors.Join(dest.check(), dest.sync(h), h.at("staged-verified"))
	}, cutHooks)
	if e != nil || !inspected.Complete {
		return result, errors.Join(e, errors.New("source cut did not close successfully"))
	}
	// SQL, children and credentials have joined. Standalone export also released
	// its lease; adoption retains the outer lease. Publication reads only staging.
	if e = h.at("source-cleaned"); e != nil {
		return result, e
	}
	if e = errors.Join(ctx.Err(), dest.check()); e != nil {
		return result, e
	}
	_, raw, e := checkGeneration(ctx, dest.root, o.Limits, false)
	if e != nil {
		return result, e
	}
	digest := sha256.Sum256(raw)
	marker, e := json.Marshal(completionMarker{manifest.Version, result.Generation, hex.EncodeToString(digest[:])})
	if e != nil {
		return result, e
	}
	if e = writeMember(dest.root, "complete.pending", marker, h); e != nil {
		return result, e
	}
	if e = dest.sync(h); e != nil {
		return result, e
	}
	if e = h.at("before-publication"); e != nil {
		return result, e
	}
	if e = errors.Join(ctx.Err(), dest.check()); e != nil {
		return result, e
	}
	// Linux atomic no-replace rename is the sole completion publication point.
	if e = unix.Renameat2(int(dest.dir.Fd()), "complete.pending", int(dest.dir.Fd()), "COMPLETE", unix.RENAME_NOREPLACE); e != nil {
		return result, e
	}
	result.Published = true
	result.Status = "published"
	if e = errors.Join(h.at("published"), dest.sync(h), dest.check()); e != nil {
		return result, e
	}
	result.Complete = true
	result.Status = "byte-complete; key/reader custody and independence unknown"
	return result, nil
}

type exportFileWriter struct {
	f *os.File
	h *exportHooks
}

func (w *exportFileWriter) Write(p []byte) (int, error) { return w.h.writeFile(w.f, p) }
func writeMember(root *os.Root, name string, b []byte, h *exportHooks) error {
	f, e := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	n, e := h.writeFile(f, b)
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	return errors.Join(e, h.syncFile(f), h.closeFile(f))
}
func copyObject(ctx context.Context, source, dest *os.Root, obj Object, name string, h *exportHooks) (member Member, err error) {
	src, e := openRegular(source, obj.Name)
	if e != nil {
		return member, e
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	before, e := src.Stat()
	if e != nil {
		return member, e
	}
	if before.Size() != obj.Size {
		return member, ErrCorrupt
	}
	dst, e := dest.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return member, e
	}
	hash := sha256.New()
	n, e := copyBounded(ctx, io.MultiWriter(&exportFileWriter{dst, h}, hash), src, obj.Size, func() error { return h.at("payload-copy") })
	e = errors.Join(e, h.syncFile(dst), h.closeFile(dst))
	if e != nil {
		return member, e
	}
	after, e := src.Stat()
	if e != nil {
		return member, e
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if n != obj.Size || digest != obj.SHA256 || !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return member, ErrCorrupt
	}
	return Member{Name: name, Size: n, SHA256: digest, ContainerID: obj.ID, SourceName: obj.Name, FormatMajor: obj.FormatMajor}, nil
}

type generationDirectory struct {
	path, name, parentPath string
	root, parent           *os.Root
	dir                    *os.File
	info, parentInfo       os.FileInfo
}

func overlap(a, b string) bool {
	contains := func(parent, child string) bool {
		rel, e := filepath.Rel(parent, child)
		return e == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
	}
	return contains(a, b) || contains(b, a)
}
func newGeneration(source, destination string) (g *generationDirectory, err error) {
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || len(destination) > 4096 {
		return nil, ErrConfiguration
	}
	parentPath, e := filepath.EvalSymlinks(filepath.Dir(destination))
	if e != nil {
		return nil, e
	}
	path := filepath.Join(parentPath, filepath.Base(destination))
	if overlap(source, path) {
		return nil, ErrConfiguration
	}
	parent, e := os.OpenRoot(parentPath)
	if e != nil {
		return nil, e
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, parent.Close())
		}
	}()
	parentInfo, e := os.Stat(parentPath)
	if e != nil {
		return nil, e
	}
	name := filepath.Base(path)
	if e = parent.Mkdir(name, 0700); e != nil {
		return nil, e
	}
	root, e := parent.OpenRoot(name)
	if e != nil {
		return nil, e
	}
	dir, e := root.Open(".")
	if e != nil {
		return nil, errors.Join(e, root.Close())
	}
	info, e := dir.Stat()
	if e != nil {
		return nil, errors.Join(e, dir.Close(), root.Close())
	}
	return &generationDirectory{path, name, parentPath, root, parent, dir, info, parentInfo}, nil
}
func (g *generationDirectory) check() error {
	p, e := os.Stat(g.parentPath)
	if e != nil || !os.SameFile(g.parentInfo, p) {
		return ErrPackage
	}
	got, e := g.parent.Lstat(g.name)
	if e != nil || !got.IsDir() || !os.SameFile(g.info, got) || got.Mode() != g.info.Mode() {
		return ErrPackage
	}
	return nil
}
func (g *generationDirectory) sync(h *exportHooks) error { return h.syncFile(g.dir) }
func (g *generationDirectory) close() error {
	return errors.Join(g.dir.Close(), g.root.Close(), g.parent.Close())
}
func (g *generationDirectory) capacity(need int64) error {
	var stat unix.Statfs_t
	if e := unix.Fstatfs(int(g.dir.Fd()), &stat); e != nil {
		return e
	}
	if need < 0 || stat.Bsize <= 0 || stat.Bavail < uint64((need+int64(stat.Bsize)-1)/int64(stat.Bsize)) {
		return errors.New("insufficient destination capacity")
	}
	return nil
}
