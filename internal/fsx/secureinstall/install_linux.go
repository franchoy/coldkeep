//go:build linux

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

var linuxOps = struct {
	renameNoReplace func(int, string, int, string, uint) error
	link            func(int, string, int, string, int) error
	beforeCreate    func() error
	beforePublish   func() error
}{
	renameNoReplace: unix.Renameat2,
	link:            unix.Linkat,
}

type linuxPending struct {
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
	parentFD, parentPath, finalName, err := linuxOpenParent(request)
	if err != nil {
		return nil, err
	}
	cleanupParent := true
	defer func() {
		if cleanupParent {
			_ = unix.Close(parentFD)
		}
	}()

	if linuxOps.beforeCreate != nil {
		if err := linuxOps.beforeCreate(); err != nil {
			return nil, fmt.Errorf("secure install before-create hook: %w", err)
		}
	}
	if err := linuxValidateParentIdentity(parentFD, parentPath); err != nil {
		return nil, err
	}
	tempName, err := temporaryName()
	if err != nil {
		return nil, err
	}
	objectFD, err := unix.Openat(parentFD, tempName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("secure install create temporary object: %w", err)
	}
	cleanupObject := true
	defer func() {
		if cleanupObject {
			_ = unix.Unlinkat(parentFD, tempName, 0)
			_ = unix.Close(objectFD)
		}
	}()
	writerFD, err := unix.Dup(objectFD)
	if err != nil {
		return nil, fmt.Errorf("secure install duplicate temporary object: %w", err)
	}
	writer := os.NewFile(uintptr(writerFD), tempName)
	if writer == nil {
		_ = unix.Close(writerFD)
		return nil, fmt.Errorf("secure install construct temporary writer")
	}

	cleanupParent = false
	cleanupObject = false
	return &linuxPending{
		parentFD: parentFD, objectFD: objectFD, writerFile: writer,
		parentPath: parentPath, tempName: tempName, finalName: finalName,
		tempPresent: true,
	}, nil
}

func (p *linuxPending) writer() *os.File { return p.writerFile }

func (p *linuxPending) publish(overwrite bool) error {
	if linuxOps.beforePublish != nil {
		if err := linuxOps.beforePublish(); err != nil {
			return fmt.Errorf("secure install before-publish hook: %w", err)
		}
	}
	if err := linuxValidateParentIdentity(p.parentFD, p.parentPath); err != nil {
		return err
	}
	if err := linuxValidateTemporaryIdentity(p.parentFD, p.tempName, p.objectFD); err != nil {
		return err
	}
	if overwrite {
		if err := unix.Renameat(p.parentFD, p.tempName, p.parentFD, p.finalName); err != nil {
			return fmt.Errorf("secure install publish overwrite: %w", err)
		}
		p.published = true
		p.tempPresent = false
	} else {
		err := linuxOps.renameNoReplace(p.parentFD, p.tempName, p.parentFD, p.finalName, unix.RENAME_NOREPLACE)
		switch {
		case err == nil:
			p.published = true
			p.tempPresent = false
		case errors.Is(err, unix.EEXIST):
			return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
		case linuxRenameNoReplaceUnsupported(err):
			if linkErr := linuxOps.link(p.parentFD, p.tempName, p.parentFD, p.finalName, 0); linkErr != nil {
				if errors.Is(linkErr, unix.EEXIST) {
					return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
				}
				if linuxLinkUnsupported(linkErr) {
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

func (p *linuxPending) applyMetadata(metadata Metadata) []metadataFailure {
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

func (p *linuxPending) abort() error {
	var errs []error
	if p.writerFile != nil {
		if err := p.writerFile.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
		p.writerFile = nil
	}
	if p.tempPresent && p.parentFD >= 0 && p.tempName != "" {
		if err := unix.Unlinkat(p.parentFD, p.tempName, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			errs = append(errs, fmt.Errorf("secure install remove retained temporary object: %w", err))
		} else {
			p.tempPresent = false
		}
	}
	if p.objectFD >= 0 {
		if err := unix.Close(p.objectFD); err != nil {
			errs = append(errs, err)
		}
		p.objectFD = -1
	}
	if p.parentFD >= 0 {
		if err := unix.Close(p.parentFD); err != nil {
			errs = append(errs, err)
		}
		p.parentFD = -1
	}
	return errors.Join(errs...)
}

func linuxOpenParent(request Request) (int, string, string, error) {
	rel, err := filepath.Rel(request.TrustedRoot, request.Destination)
	if err != nil {
		return -1, "", "", err
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	finalName := parts[len(parts)-1]
	parentParts := parts[:len(parts)-1]
	fd, err := unix.Open(request.TrustedRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", "", fmt.Errorf("secure install open trusted root: %w", err)
	}
	parentPath := request.TrustedRoot
	for _, part := range parentParts {
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(fd, part, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				_ = unix.Close(fd)
				return -1, "", "", fmt.Errorf("secure install create parent component %q: %w", part, mkdirErr)
			}
			next, openErr = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		if openErr != nil {
			_ = unix.Close(fd)
			return -1, "", "", fmt.Errorf("secure install open parent component %q: %w", part, openErr)
		}
		_ = unix.Close(fd)
		fd = next
		parentPath = filepath.Join(parentPath, part)
	}
	return fd, parentPath, finalName, nil
}

func linuxValidateParentIdentity(fd int, parentPath string) error {
	var retained unix.Stat_t
	if err := unix.Fstat(fd, &retained); err != nil {
		return fmt.Errorf("secure install stat retained parent: %w", err)
	}
	info, err := os.Stat(parentPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParentChanged, err)
	}
	current, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(current.Dev) != uint64(retained.Dev) || current.Ino != retained.Ino {
		return ErrParentChanged
	}
	return nil
}

func linuxValidateTemporaryIdentity(parentFD int, tempName string, objectFD int) error {
	var retained unix.Stat_t
	if err := unix.Fstat(objectFD, &retained); err != nil {
		return fmt.Errorf("secure install stat retained temporary object: %w", err)
	}
	var named unix.Stat_t
	if err := unix.Fstatat(parentFD, tempName, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("secure install stat temporary name: %w", err)
	}
	if uint64(named.Dev) != uint64(retained.Dev) || named.Ino != retained.Ino {
		return fmt.Errorf("secure install temporary name no longer identifies retained object")
	}
	return nil
}

func linuxRenameNoReplaceUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP)
}

func linuxLinkUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EPERM)
}
