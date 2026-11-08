package recoveryexport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"syscall"
	"time"
)

const packageVersion = 1

var safeGeneration = regexp.MustCompile(`^[0-9a-f]{32}$`)
var safeDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)
var safeMember = regexp.MustCompile(`^(catalog\.dump|object-[0-9]{6}\.bin)$`)
var ErrPackage = errors.New("invalid or incomplete recovery generation")

type ExportLimits struct {
	ArchiveBytes, ManifestBytes, TotalBytes int64
	Members                                 int
	Duration                                time.Duration
}

func DefaultExportLimits() ExportLimits {
	return ExportLimits{64 << 20, 8 << 20, (4 << 30) + (72 << 20) + 4096, 20001, 2 * time.Minute}
}
func (l ExportLimits) valid() bool {
	d := DefaultExportLimits()
	return l.ArchiveBytes > 0 && l.ArchiveBytes <= d.ArchiveBytes && l.ManifestBytes > 0 && l.ManifestBytes <= d.ManifestBytes && l.TotalBytes > 0 && l.TotalBytes <= d.TotalBytes && l.Members > 0 && l.Members <= d.Members && l.Duration > 0 && l.Duration <= d.Duration
}

type Member struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	ContainerID int64  `json:"container_id,omitempty"`
	SourceName  string `json:"source_name,omitempty"`
	FormatMajor uint16 `json:"format_major,omitempty"`
}
type DatabaseRequirements struct {
	ServerVersion  int    `json:"server_version"`
	Encoding       string `json:"encoding"`
	Collate        string `json:"collate"`
	CType          string `json:"ctype"`
	LocaleProvider string `json:"locale_provider"`
	DumpVersion    string `json:"pg_dump_version"`
	RestoreVersion string `json:"pg_restore_version"`
	RestorePolicy  string `json:"restore_policy"`
}
type Manifest struct {
	Version            int                  `json:"package_version"`
	Generation         string               `json:"generation"`
	ApplicationVersion string               `json:"application_version"`
	CatalogVersion     int                  `json:"catalog_version"`
	Selection          string               `json:"selection"`
	CutTime            string               `json:"cut_time"`
	Snapshot           string               `json:"snapshot_observation"`
	LegacyRootHash     string               `json:"legacy_root_observation"`
	Reader             string               `json:"reader_requirement"`
	KeyRequired        bool                 `json:"key_required"`
	KeyReference       string               `json:"key_reference,omitempty"`
	KeyCustody         string               `json:"key_custody"`
	ReaderCustody      string               `json:"reader_custody"`
	Independence       string               `json:"destination_independence"`
	Database           DatabaseRequirements `json:"database"`
	CatalogRows        int                  `json:"catalog_rows"`
	Tables             int                  `json:"catalog_tables"`
	Members            []Member             `json:"members"`
	Excluded           []string             `json:"uncataloged_excluded"`
}
type completionMarker struct {
	Version        int    `json:"package_version"`
	Generation     string `json:"generation"`
	ManifestSHA256 string `json:"manifest_sha256"`
}
type CheckResult struct {
	ByteComplete bool     `json:"byte_complete"`
	Scope        string   `json:"scope"`
	Manifest     Manifest `json:"manifest"`
}

// Check verifies package integrity without a source connection, key or SQL
// execution. It is not authentication, catalog admission or a restoration proof.
func Check(ctx context.Context, path string, limits ExportLimits) (result CheckResult, err error) {
	if ctx == nil || !limits.valid() {
		return result, ErrConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, limits.Duration)
	defer cancel()
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return result, ErrPackage
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return result, err
	}
	defer func() {
		err = errors.Join(err, root.Close())
		if err != nil {
			result.ByteComplete = false
		}
	}()
	m, _, err := checkGeneration(ctx, root, limits, true)
	if err != nil {
		return result, err
	}
	after, e := os.Lstat(path)
	if e != nil || !os.SameFile(info, after) {
		return result, ErrPackage
	}
	return CheckResult{true, "declared package integrity only; no authentication or restore", m}, nil
}
func readRegular(root *os.Root, name string, max int64) ([]byte, error) {
	file, err := openRegular(root, name)
	if err != nil {
		return nil, err
	}
	b, e := io.ReadAll(io.LimitReader(file, max+1))
	err = errors.Join(e, file.Close())
	if int64(len(b)) > max {
		return nil, ErrLimit
	}
	return b, err
}
func openRegular(root *os.Root, name string) (*os.File, error) {
	before, e := root.Lstat(name)
	if e != nil || !before.Mode().IsRegular() {
		return nil, ErrPackage
	}
	f, e := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	after, e := f.Stat()
	if e != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() {
		return nil, errors.Join(ErrPackage, f.Close())
	}
	stat, ok := after.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, errors.Join(ErrPackage, f.Close())
	}
	return f, nil
}
func strictJSON(ctx context.Context, b []byte, v any, membersLimit int) error {
	d := json.NewDecoder(bytes.NewReader(b))
	var walk func(int, int) error
	walk = func(depth, arrayLimit int) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if depth > 12 {
			return ErrLimit
		}
		token, e := d.Token()
		if e != nil {
			return e
		}
		if s, ok := token.(string); ok && len(s) > 4096 {
			return ErrLimit
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := key.(string)
				if !ok || seen[s] || len(s) > 128 {
					return ErrPackage
				}
				// encoding/json matches struct fields case-insensitively,
				// including Unicode simple folds. Refuse aliases of a key
				// already seen so the two passes have one interpretation.
				for prior := range seen {
					if strings.EqualFold(s, prior) {
						return ErrPackage
					}
				}
				seen[s] = true
				if len(seen) > 64 {
					return ErrLimit
				}
				childLimit := -1
				if depth == 0 {
					switch {
					case strings.EqualFold(s, "members"):
						childLimit = membersLimit
					case strings.EqualFold(s, "objects"):
						childLimit = ImportReceiptMaxObjects
					case strings.EqualFold(s, "uncataloged_excluded"):
						childLimit = DefaultLimits().DirectoryEntries
					}
				}
				if e = walk(depth+1, childLimit); e != nil {
					return e
				}
			}
		case '[':
			for count := 0; d.More(); count++ {
				// Enforce slice caps before typed allocation (manifest Members,
				// Excluded, or readiness Objects), including lowered caps.
				if arrayLimit >= 0 && count >= arrayLimit {
					return ErrLimit
				}
				if e = walk(depth+1, -1); e != nil {
					return e
				}
			}
		default:
			return ErrPackage
		}
		_, e = d.Token()
		return e
	}
	if e := walk(0, -1); e != nil {
		return errors.Join(ErrPackage, e)
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrPackage
	}
	if e := ctx.Err(); e != nil {
		return errors.Join(ErrPackage, e)
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return errors.Join(ErrPackage, e)
	}
	if e := ctx.Err(); e != nil {
		return errors.Join(ErrPackage, e)
	}
	return nil
}
func hashFile(ctx context.Context, f *os.File, max int64) (int64, string, error) {
	h := sha256.New()
	n, e := copyBounded(ctx, h, f, max, nil)
	return n, hex.EncodeToString(h.Sum(nil)), e
}
func copyBounded(ctx context.Context, dst io.Writer, src io.Reader, max int64, step func() error) (int64, error) {
	buf := make([]byte, 64<<10)
	var total int64
	for {
		if e := ctx.Err(); e != nil {
			return total, e
		}
		n, e := src.Read(buf)
		if n > 0 {
			if int64(n) > max-total {
				return total, ErrLimit
			}
			w, x := dst.Write(buf[:n])
			total += int64(w)
			if x != nil {
				return total, x
			}
			if w != n {
				return total, io.ErrShortWrite
			}
			if step != nil {
				if x = step(); x != nil {
					return total, x
				}
			}
		}
		if e == io.EOF {
			return total, nil
		}
		if e != nil {
			return total, e
		}
	}
}
func checkGeneration(ctx context.Context, root *os.Root, l ExportLimits, complete bool) (m Manifest, raw []byte, err error) {
	raw, err = readRegular(root, "manifest.json", l.ManifestBytes)
	if err != nil {
		return
	}
	if err = strictJSON(ctx, raw, &m, l.Members); err != nil {
		return
	}
	if m.Version != packageVersion || !safeGeneration.MatchString(m.Generation) || m.CatalogVersion != 17 || m.Selection != "full-catalog" || m.Tables != 19 || m.CatalogRows < 1 || m.CatalogRows > DefaultLimits().Rows || m.Reader == "" || m.ApplicationVersion == "" || !safeDigest.MatchString(m.LegacyRootHash) || m.KeyCustody != "unknown" || m.ReaderCustody != "unknown" || m.Independence != "unknown" || m.Database.ServerVersion < 160000 || m.Database.ServerVersion >= 170000 || m.Database.RestorePolicy != "trusted operator; fresh database; --no-owner --no-acl --exit-on-error" || (m.KeyRequired && m.KeyReference == "") {
		err = ErrPackage
		return
	}
	if _, e := time.Parse(time.RFC3339Nano, m.CutTime); e != nil || !snapshotName.MatchString(m.Snapshot) || m.Database.LocaleProvider != "c" || m.Database.Encoding == "" || m.Database.Collate == "" || m.Database.CType == "" || !regexp.MustCompile(`^16\.[0-9]+$`).MatchString(m.Database.DumpVersion) || m.Database.RestoreVersion != m.Database.DumpVersion {
		err = ErrPackage
		return
	}
	if len(m.Members) < 1 || len(m.Members) > l.Members || len(m.Excluded) > DefaultLimits().DirectoryEntries {
		err = ErrLimit
		return
	}
	expected := map[string]bool{"manifest.json": true}
	ids := map[int64]bool{}
	names := map[string]bool{}
	total := int64(len(raw)) + 4096
	for i, member := range m.Members {
		if !safeMember.MatchString(member.Name) || expected[member.Name] || !safeDigest.MatchString(member.SHA256) || member.Size <= 0 || member.Size > l.TotalBytes-total {
			err = ErrPackage
			return
		}
		total += member.Size
		expected[member.Name] = true
		if i == 0 {
			if member.Name != "catalog.dump" || member.Size > l.ArchiveBytes || member.ContainerID != 0 {
				err = ErrPackage
				return
			}
		} else {
			if member.Name != fmt.Sprintf("object-%06d.bin", i) || member.ContainerID <= 0 || ids[member.ContainerID] || names[member.SourceName] || member.SourceName == "" || member.Size > DefaultLimits().ObjectBytes || member.FormatMajor > 1 {
				err = ErrPackage
				return
			}
			ids[member.ContainerID] = true
			names[member.SourceName] = true
		}
		f, e := openRegular(root, member.Name)
		if e != nil {
			err = e
			return
		}
		if i == 0 {
			var header [8]byte
			_, e = f.ReadAt(header[:], 0)
			if e != nil || string(header[:5]) != "PGDMP" || header[5] != 1 || header[6] != 15 || header[7] != 0 {
				err = errors.Join(ErrPackage, f.Close())
				return
			}
		}
		n, digest, e := hashFile(ctx, f, member.Size)
		err = errors.Join(e, f.Close())
		if err != nil {
			return
		}
		if n != member.Size || digest != member.SHA256 {
			err = ErrPackage
			return
		}
	}
	if complete {
		var marker completionMarker
		b, e := readRegular(root, "COMPLETE", 4096)
		if e != nil {
			err = e
			return
		}
		if err = strictJSON(ctx, b, &marker, l.Members); err != nil {
			return
		}
		digest := sha256.Sum256(raw)
		if marker.Version != packageVersion || marker.Generation != m.Generation || marker.ManifestSHA256 != hex.EncodeToString(digest[:]) {
			err = ErrPackage
			return
		}
		expected["COMPLETE"] = true
	}
	dir, e := root.Open(".")
	if e != nil {
		err = e
		return
	}
	entries, e := dir.ReadDir(l.Members + 4)
	err = errors.Join(e, dir.Close())
	if err != nil {
		return
	}
	if len(entries) != len(expected) {
		err = ErrPackage
		return
	}
	for _, entry := range entries {
		if !expected[entry.Name()] {
			err = ErrPackage
			return
		}
	}
	return
}
