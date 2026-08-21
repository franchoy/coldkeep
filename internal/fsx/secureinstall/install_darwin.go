//go:build darwin

package secureinstall

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var darwinOps = struct {
	renameNoReplace func(int, string, int, string, uint32) error
	link            func(int, string, int, string, int) error
	beforeCreate    func() error
	beforePublish   func() error
}{
	renameNoReplace: unix.RenameatxNp,
	link:            unix.Linkat,
}

type darwinPending struct {
	parentFD    int
	objectFD    int
	writerFile  *os.File
	parentPath  string
	tempName    string
	finalName   string
	published   bool
	tempPresent bool
}

func beginPlatform(request Request) (nativePending, error) {
	parentFD, parentPath, finalName, err := darwinOpenParent(request)
	if err != nil {
		return nil, err
	}
	tempName, err := temporaryName()
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, err
	}
	if darwinOps.beforeCreate != nil {
		if err := darwinOps.beforeCreate(); err != nil {
			_ = unix.Close(parentFD)
			return nil, fmt.Errorf("secure install before-create hook: %w", err)
		}
	}
	if err := darwinValidateParentIdentity(parentFD, parentPath); err != nil {
		_ = unix.Close(parentFD)
		return nil, err
	}
	objectFD, err := unix.Openat(parentFD, tempName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("secure install create temporary object: %w", err)
	}
	writerFD, err := unix.Dup(objectFD)
	if err != nil {
		_ = unix.Unlinkat(parentFD, tempName, 0)
		_ = unix.Close(objectFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("secure install duplicate temporary object: %w", err)
	}
	return &darwinPending{
		parentFD: parentFD, objectFD: objectFD, writerFile: os.NewFile(uintptr(writerFD), tempName),
		parentPath: parentPath, tempName: tempName, finalName: finalName,
		tempPresent: true,
	}, nil
}

func (p *darwinPending) writer() *os.File { return p.writerFile }

func (p *darwinPending) publish(overwrite bool) error {
	if darwinOps.beforePublish != nil {
		if err := darwinOps.beforePublish(); err != nil {
			return fmt.Errorf("secure install before-publish hook: %w", err)
		}
	}
	if err := darwinValidateParentIdentity(p.parentFD, p.parentPath); err != nil {
		return err
	}
	if err := darwinValidateTemporaryIdentity(p.parentFD, p.tempName, p.objectFD); err != nil {
		return err
	}
	if overwrite {
		if err := unix.Renameat(p.parentFD, p.tempName, p.parentFD, p.finalName); err != nil {
			return fmt.Errorf("secure install publish overwrite: %w", err)
		}
		p.published = true
		p.tempPresent = false
	} else {
		err := darwinOps.renameNoReplace(p.parentFD, p.tempName, p.parentFD, p.finalName, unix.RENAME_EXCL)
		switch {
		case err == nil:
			p.published = true
			p.tempPresent = false
		case errors.Is(err, unix.EEXIST):
			return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
		case errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.EINVAL), errors.Is(err, unix.ENOSYS):
			if linkErr := darwinOps.link(p.parentFD, p.tempName, p.parentFD, p.finalName, 0); linkErr != nil {
				if errors.Is(linkErr, unix.EEXIST) {
					return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
				}
				if errors.Is(linkErr, unix.ENOTSUP) || errors.Is(linkErr, unix.EOPNOTSUPP) || errors.Is(linkErr, unix.ENOSYS) || errors.Is(linkErr, unix.EPERM) {
					return fmt.Errorf("%w: rename=%v link=%v", ErrAtomicNoReplaceUnsupported, err, linkErr)
				}
				return fmt.Errorf("secure install atomic link publication: %w", linkErr)
			}
			p.published = true
			if unlinkErr := unix.Unlinkat(p.parentFD, p.tempName, 0); unlinkErr != nil {
				return fmt.Errorf("secure install remove linked temporary name: %w", unlinkErr)
			}
			p.tempPresent = false
		default:
			return fmt.Errorf("secure install no-replace publication: %w", err)
		}
	}
	if err := unix.Fsync(p.parentFD); err != nil {
		return fmt.Errorf("secure install sync retained parent: %w", err)
	}
	return nil
}

func (p *darwinPending) applyMetadata(metadata Metadata) []metadataFailure {
	failures := make([]metadataFailure, 0, 3)
	if metadata.Mode != nil {
		if err := unix.Fchmod(p.objectFD, uint32(metadata.Mode.Perm())); err != nil {
			failures = append(failures, metadataFailure{"chmod", err})
		}
	}
	if metadata.ModifiedAt != nil {
		tv := unix.NsecToTimeval(metadata.ModifiedAt.UnixNano())
		if err := unix.Futimes(p.objectFD, []unix.Timeval{tv, tv}); err != nil {
			failures = append(failures, metadataFailure{"chtimes", err})
		}
	}
	if metadata.UID != nil && metadata.GID != nil {
		if err := unix.Fchown(p.objectFD, *metadata.UID, *metadata.GID); err != nil {
			failures = append(failures, metadataFailure{"chown", err})
		}
	}
	return failures
}

func (p *darwinPending) abort() error {
	var errs []error
	if p.writerFile != nil {
		if err := p.writerFile.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
		p.writerFile = nil
	}
	if p.tempPresent && p.parentFD >= 0 {
		if err := unix.Unlinkat(p.parentFD, p.tempName, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			errs = append(errs, err)
		} else {
			p.tempPresent = false
		}
	}
	if p.objectFD >= 0 {
		errs = append(errs, unix.Close(p.objectFD))
		p.objectFD = -1
	}
	if p.parentFD >= 0 {
		errs = append(errs, unix.Close(p.parentFD))
		p.parentFD = -1
	}
	return errors.Join(errs...)
}

func darwinOpenParent(request Request) (int, string, string, error) {
	anchor, err := nearestExistingDirectory(request.TrustedRoot)
	if err != nil {
		return -1, "", "", err
	}
	rel, err := filepath.Rel(anchor, request.Destination)
	if err != nil {
		return -1, "", "", err
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	fd, err := unix.Open(anchor, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, "", "", err
	}
	parentPath := anchor
	for _, part := range parts[:len(parts)-1] {
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(fd, part, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				_ = unix.Close(fd)
				return -1, "", "", mkdirErr
			}
			next, openErr = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		if openErr != nil {
			_ = unix.Close(fd)
			return -1, "", "", openErr
		}
		_ = unix.Close(fd)
		fd = next
		parentPath = filepath.Join(parentPath, part)
	}
	return fd, parentPath, parts[len(parts)-1], nil
}

func darwinValidateParentIdentity(fd int, path string) error {
	var retained unix.Stat_t
	if err := unix.Fstat(fd, &retained); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParentChanged, err)
	}
	current, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(current.Dev) != uint64(retained.Dev) || current.Ino != retained.Ino {
		return ErrParentChanged
	}
	return nil
}

func darwinValidateTemporaryIdentity(parentFD int, tempName string, objectFD int) error {
	var retained unix.Stat_t
	if err := unix.Fstat(objectFD, &retained); err != nil {
		return err
	}
	var named unix.Stat_t
	if err := unix.Fstatat(parentFD, tempName, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if uint64(named.Dev) != uint64(retained.Dev) || named.Ino != retained.Ino {
		return fmt.Errorf("secure install temporary name no longer identifies retained object")
	}
	return nil
}
