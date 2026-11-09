package recoveryexport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

type bindingDirectory struct {
	path     string
	root     *os.Root
	file     *os.File
	info     os.FileInfo
	evidence BindingObjectEvidence
}

func observeBindingFile(f *os.File) (BindingObjectEvidence, error) {
	var st unix.Statx_t
	if e := unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_BASIC_STATS|unix.STATX_MNT_ID_UNIQUE, &st); e != nil {
		return BindingObjectEvidence{}, e
	}
	var fs unix.Statfs_t
	if e := unix.Fstatfs(int(f.Fd()), &fs); e != nil {
		return BindingObjectEvidence{}, e
	}
	if st.Mask&unix.STATX_MNT_ID_UNIQUE == 0 {
		return BindingObjectEvidence{}, ErrUnsupported
	}
	return BindingObjectEvidence{unix.Mkdev(st.Dev_major, st.Dev_minor), st.Ino, st.Mnt_id, fs.Type}, nil
}
func openBindingDirectory(path string, private bool) (_ *bindingDirectory, err error) {
	if _, e := EncodeBindingPath(path); e != nil {
		return nil, e
	}
	canonical, e := filepath.EvalSymlinks(path)
	if e != nil || canonical != path {
		return nil, ErrBindingUnavailable
	}
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	d := &bindingDirectory{path: path, file: f}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.close())
		}
	}()
	d.info, e = f.Stat()
	if e != nil {
		return nil, e
	}
	st, ok := d.info.Sys().(*syscall.Stat_t)
	if !ok || (private && (st.Uid != uint32(os.Geteuid()) || d.info.Mode().Perm()&0077 != 0)) {
		return nil, ErrConfiguration
	}
	d.evidence, e = observeBindingFile(f)
	if e != nil {
		return nil, e
	}
	d.root, e = os.OpenRoot(path)
	if e != nil {
		return nil, e
	}
	if e = d.check(); e != nil {
		return nil, e
	}
	return d, nil
}
func (d *bindingDirectory) check() error {
	if d == nil || d.root == nil {
		return ErrBindingUnavailable
	}
	now, e := os.Lstat(d.path)
	if e != nil || !os.SameFile(d.info, now) || now.Mode() != d.info.Mode() {
		return ErrBindingMismatch
	}
	before, ok := d.info.Sys().(*syscall.Stat_t)
	after, valid := now.Sys().(*syscall.Stat_t)
	if !ok || !valid || before.Uid != after.Uid || before.Gid != after.Gid {
		return ErrBindingMismatch
	}
	f, e := d.root.Open(".")
	if e != nil {
		return e
	}
	got, e := observeBindingFile(f)
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if got != d.evidence {
		return ErrBindingMismatch
	}
	return nil
}
func (d *bindingDirectory) close() error {
	if d == nil {
		return nil
	}
	var e error
	if d.root != nil {
		e = d.root.Close()
		d.root = nil
	}
	if d.file != nil {
		e = errors.Join(e, d.file.Close())
		d.file = nil
	}
	return e
}
func bindingSessionEvidence(root *bindingDirectory, source bool) (out BindingEvidence, err error) {
	out.Class = bindingEvidenceClass
	boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil || len(boot) > 64 {
		return out, ErrUnsupported
	}
	out.Boot = strings.TrimSuffix(string(boot), "\n")
	var ns unix.Stat_t
	if e = unix.Stat("/proc/self/ns/mnt", &ns); e != nil {
		return out, e
	}
	out.Namespace = ns.Ino
	out.Root = root.evidence
	if !source {
		for n, dest := range map[string]*BindingObjectEvidence{".coldkeep-control": &out.Control, ".coldkeep-control/repository.lock": &out.Lock} {
			f, e := root.root.OpenFile(n, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
			if e != nil {
				return out, e
			}
			info, e := f.Stat()
			if e != nil {
				return out, errors.Join(e, f.Close())
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 || (strings.HasSuffix(n, ".lock") && (!info.Mode().IsRegular() || st.Nlink != 1)) {
				return out, errors.Join(ErrConfiguration, f.Close())
			}
			*dest, e = observeBindingFile(f)
			e = errors.Join(e, f.Close())
			if e != nil {
				return out, e
			}
		}
	}
	if !validEvidence(out, source) {
		return out, ErrUnsupported
	}
	return out, root.check()
}
func readBindingFile(ctx context.Context, d *bindingDirectory, name string, max int, v any) (err error) {
	if e := d.check(); e != nil {
		return e
	}
	f, e := d.root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, e := f.Stat()
	if e != nil {
		return e
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return ErrConfiguration
	}
	if info.Size() > int64(max) {
		return ErrLimit
	}
	b, e := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if e != nil {
		return e
	}
	if e = decodeBindingRecord(ctx, b, max, v); e != nil {
		return e
	}
	return d.check()
}
func bindingFileAbsent(d *bindingDirectory, name string) error {
	_, e := d.root.Lstat(name)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	return ErrBindingMismatch
}

// Staging is exclusive and deterministic. A crash residue can only be replaced
// after the caller has validated its matching durable intent and catalog state.
func publishBindingFile(ctx context.Context, d *bindingDirectory, name string, v any, max int, check func() error, h *bindingHooks) (err error) {
	b := []byte(bindingJSON(v))
	if e := decodeBindingRecord(ctx, b, max, new(map[string]any)); e != nil {
		return e
	}
	if e := errors.Join(ctx.Err(), d.check(), check()); e != nil {
		return e
	}
	staging := name + ".pending"
	f, e := d.root.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, f.Close(), h.at("file-close:"+name, nil)) }()
	if e = h.at("file-write:"+name, nil); e != nil {
		return e
	}
	n, e := f.Write(b)
	if e != nil {
		return e
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	if e = h.at("file-sync:"+name, nil); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = errors.Join(ctx.Err(), check(), d.check()); e != nil {
		return e
	}
	if e = h.at("file-rename:"+name, nil); e != nil {
		return e
	}
	if e = errors.Join(ctx.Err(), d.check(), check()); e != nil {
		return e
	}
	if e = d.root.Rename(staging, name); e != nil {
		return e
	}
	if e = h.at("directory-sync:"+name, nil); e != nil {
		return e
	}
	return errors.Join(d.file.Sync(), check(), d.check())
}
func removeBindingStaging(ctx context.Context, d *bindingDirectory, name string, max int, value any) error {
	if e := errors.Join(ctx.Err(), d.check()); e != nil {
		return e
	}
	f, e := d.root.OpenFile(name+".pending", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	info, e := f.Stat()
	if e != nil {
		return errors.Join(e, f.Close())
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 || info.Size() > int64(max) {
		return errors.Join(ErrConfiguration, f.Close())
	}
	raw, e := io.ReadAll(io.LimitReader(f, int64(max)+1))
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if !bytes.HasPrefix([]byte(bindingJSON(value)), raw) {
		return ErrBindingMismatch
	}
	return errors.Join(d.root.Remove(name+".pending"), d.file.Sync())
}
func validateInstallationLocation(d *bindingDirectory, root *bindingDirectory, paths ...string) error {
	if !utf8.ValidString(d.path) || d.evidence.Mount == root.evidence.Mount {
		return ErrConfiguration
	}
	for _, p := range append(paths, root.path) {
		if p == "" {
			continue
		}
		id, e := filepath.Abs(p)
		if e != nil {
			return e
		}
		// Resolve the existing prefix so aliases cannot bypass containment.
		for {
			if _, e = os.Lstat(id); e == nil {
				break
			}
			next := filepath.Dir(id)
			if next == id {
				return ErrConfiguration
			}
			id = next
		}
		id, e = filepath.EvalSymlinks(id)
		if e != nil {
			return e
		}
		if pathContains(d.path, id) || pathContains(id, d.path) {
			return ErrConfiguration
		}
	}
	return nil
}
func pathContains(parent, child string) bool {
	rel, e := filepath.Rel(parent, child)
	return e == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, "../"))
}
