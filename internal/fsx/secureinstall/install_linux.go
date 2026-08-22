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
	syncParent      func(int) error
	chmod           func(int, uint32) error
	times           func(int, []unix.Timeval) error
	chown           func(int, int, int) error
	beforeCreate    func() error
	beforePublish   func() error
	beforeMetadata  func() error
}{
	renameNoReplace: unix.Renameat2,
	link:            unix.Linkat,
	syncParent:      unix.Fsync,
	chmod:           unix.Fchmod,
	times:           unix.Futimes,
	chown:           unix.Fchown,
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

	objectFD, writer, tempName, err := linuxCreateTemporary(parentFD, parentPath)
	if err != nil {
		return nil, err
	}

	cleanupParent = false
	return &linuxPending{
		parentFD: parentFD, objectFD: objectFD, writerFile: writer,
		parentPath: parentPath, tempName: tempName, finalName: finalName,
		tempPresent: true,
	}, nil
}

func linuxCreateTemporary(parentFD int, parentPath string) (int, *os.File, string, error) {
	if linuxOps.beforeCreate != nil {
		if err := linuxOps.beforeCreate(); err != nil {
			return -1, nil, "", fmt.Errorf("secure install before-create hook: %w", err)
		}
	}
	if err := linuxValidateParentIdentity(parentFD, parentPath); err != nil {
		return -1, nil, "", err
	}
	tempName, err := temporaryName()
	if err != nil {
		return -1, nil, "", err
	}
	objectFD, err := unix.Openat(parentFD, tempName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return -1, nil, "", fmt.Errorf("secure install create temporary object: %w", err)
	}
	writer, err := linuxDuplicateWriter(objectFD, tempName)
	if err != nil {
		_ = unix.Unlinkat(parentFD, tempName, 0)
		_ = unix.Close(objectFD)
		return -1, nil, "", err
	}
	return objectFD, writer, tempName, nil
}

func linuxDuplicateWriter(objectFD int, tempName string) (*os.File, error) {
	writerFD, err := unix.Dup(objectFD)
	if err != nil {
		return nil, fmt.Errorf("secure install duplicate temporary object: %w", err)
	}
	writer := os.NewFile(uintptr(writerFD), tempName)
	if writer == nil {
		_ = unix.Close(writerFD)
		return nil, fmt.Errorf("secure install construct temporary writer")
	}
	return writer, nil
}

func (p *linuxPending) writer() *os.File { return p.writerFile }

func (p *linuxPending) publish(overwrite bool) error {
	if err := p.preparePublish(); err != nil {
		return err
	}
	if overwrite {
		if err := p.publishOverwrite(); err != nil {
			return err
		}
	} else if err := p.publishNoReplace(); err != nil {
		return err
	}
	return p.finishPublish()
}

func (p *linuxPending) preparePublish() error {
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
	return nil
}

func (p *linuxPending) publishOverwrite() error {
	if err := unix.Renameat(p.parentFD, p.tempName, p.parentFD, p.finalName); err != nil {
		return fmt.Errorf("secure install publish overwrite: %w", err)
	}
	p.published = true
	p.tempPresent = false
	return nil
}

func (p *linuxPending) publishNoReplace() error {
	err := linuxOps.renameNoReplace(p.parentFD, p.tempName, p.parentFD, p.finalName, unix.RENAME_NOREPLACE)
	switch {
	case err == nil:
		p.published = true
		p.tempPresent = false
		return nil
	case errors.Is(err, unix.EEXIST):
		return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
	case linuxRenameNoReplaceUnsupported(err):
		return p.publishByLink(err)
	default:
		return fmt.Errorf("secure install no-replace publication: %w", err)
	}
}

func (p *linuxPending) publishByLink(renameErr error) error {
	if linkErr := linuxOps.link(p.parentFD, p.tempName, p.parentFD, p.finalName, 0); linkErr != nil {
		if errors.Is(linkErr, unix.EEXIST) {
			return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
		}
		if linuxLinkUnsupported(linkErr) {
			return fmt.Errorf("%w: rename=%v link=%v", ErrAtomicNoReplaceUnsupported, renameErr, linkErr)
		}
		return fmt.Errorf("secure install atomic link publication: %w", linkErr)
	}
	p.published = true
	if unlinkErr := unix.Unlinkat(p.parentFD, p.tempName, 0); unlinkErr != nil {
		return fmt.Errorf("secure install remove linked temporary name: %w", unlinkErr)
	}
	p.tempPresent = false
	return nil
}

func (p *linuxPending) finishPublish() error {
	if err := linuxOps.syncParent(p.parentFD); err != nil {
		return fmt.Errorf("secure install sync retained parent: %w", err)
	}
	if linuxOps.beforeMetadata != nil {
		if err := linuxOps.beforeMetadata(); err != nil {
			return fmt.Errorf("secure install before-metadata hook: %w", err)
		}
	}
	return nil
}

func (p *linuxPending) applyMetadata(metadata Metadata) []metadataFailure {
	failures := make([]metadataFailure, 0, 3)
	if metadata.Mode != nil {
		if err := linuxOps.chmod(p.objectFD, uint32(metadata.Mode.Perm())); err != nil {
			failures = append(failures, metadataFailure{"chmod", err})
		}
	}
	if metadata.ModifiedAt != nil {
		tv := unix.NsecToTimeval(metadata.ModifiedAt.UnixNano())
		if err := linuxOps.times(p.objectFD, []unix.Timeval{tv, tv}); err != nil {
			failures = append(failures, metadataFailure{"chtimes", err})
		}
	}
	if metadata.UID != nil && metadata.GID != nil {
		if err := linuxOps.chown(p.objectFD, *metadata.UID, *metadata.GID); err != nil {
			failures = append(failures, metadataFailure{"chown", err})
		}
	}
	return failures
}

func (p *linuxPending) abort() error {
	var errs []error
	errs = append(errs, p.closeWriter(), p.removeTemporary(), p.closeObject(), p.closeParent())
	return errors.Join(errs...)
}

func (p *linuxPending) closeWriter() error {
	if p.writerFile == nil {
		return nil
	}
	err := p.writerFile.Close()
	p.writerFile = nil
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

func (p *linuxPending) removeTemporary() error {
	if !p.tempPresent || p.parentFD < 0 || p.tempName == "" {
		return nil
	}
	err := unix.Unlinkat(p.parentFD, p.tempName, 0)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("secure install remove retained temporary object: %w", err)
	}
	p.tempPresent = false
	return nil
}

func (p *linuxPending) closeObject() error {
	if p.objectFD < 0 {
		return nil
	}
	err := unix.Close(p.objectFD)
	p.objectFD = -1
	return err
}

func (p *linuxPending) closeParent() error {
	if p.parentFD < 0 {
		return nil
	}
	err := unix.Close(p.parentFD)
	p.parentFD = -1
	return err
}

func linuxOpenParent(request Request) (int, string, string, error) {
	anchor, err := nearestExistingDirectory(request.TrustedRoot)
	if err != nil {
		return -1, "", "", err
	}
	rel, err := filepath.Rel(anchor, request.Destination)
	if err != nil {
		return -1, "", "", err
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	finalName := parts[len(parts)-1]
	parentParts := parts[:len(parts)-1]
	fd, err := unix.Open(anchor, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, "", "", fmt.Errorf("secure install open trusted root: %w", err)
	}
	parentPath := anchor
	for _, part := range parentParts {
		next, openErr := linuxOpenParentComponent(fd, part)
		if openErr != nil {
			_ = unix.Close(fd)
			return -1, "", "", openErr
		}
		_ = unix.Close(fd)
		fd = next
		parentPath = filepath.Join(parentPath, part)
	}
	return fd, parentPath, finalName, nil
}

func linuxOpenParentComponent(parentFD int, part string) (int, error) {
	next, err := unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if !errors.Is(err, unix.ENOENT) {
		if err != nil {
			return -1, fmt.Errorf("secure install open parent component %q: %w", part, err)
		}
		return next, nil
	}
	if mkdirErr := unix.Mkdirat(parentFD, part, 0o755); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
		return -1, fmt.Errorf("secure install create parent component %q: %w", part, mkdirErr)
	}
	next, err = unix.Openat(parentFD, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, fmt.Errorf("secure install open parent component %q: %w", part, err)
	}
	return next, nil
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
