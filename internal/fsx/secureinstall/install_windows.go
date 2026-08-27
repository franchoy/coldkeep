//go:build windows

package secureinstall

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

type fileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

type fileBasicInformation struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	FileAttributes uint32
	_              uint32
}

var windowsOps = struct {
	beforeCreate  func() error
	beforePublish func() error
}{}

const windowsRenameMaxCodeUnits = windows.MAX_LONG_PATH

var errWindowsRenameNameTooLong = errors.New("secure install destination name exceeds Windows rename limit")

type windowsPending struct {
	parentHandle windows.Handle
	objectHandle windows.Handle
	writerFile   *os.File
	parentPath   string
	tempName     string
	finalName    string
	published    bool
	tempPresent  bool
}

func beginPlatform(request Request) (nativePending, error) {
	parentHandle, parentPath, finalName, err := windowsOpenParent(request)
	if err != nil {
		return nil, err
	}
	cleanupParent := true
	defer func() {
		if cleanupParent {
			_ = windows.CloseHandle(parentHandle)
		}
	}()
	tempName, err := temporaryName()
	if err != nil {
		return nil, err
	}
	objectHandle, writer, err := windowsCreateTemporary(parentHandle, parentPath, tempName)
	if err != nil {
		return nil, err
	}
	cleanupParent = false
	return &windowsPending{
		parentHandle: parentHandle, objectHandle: objectHandle, writerFile: writer,
		parentPath: parentPath, tempName: tempName, finalName: finalName,
		tempPresent: true,
	}, nil
}

func windowsCreateTemporary(parentHandle windows.Handle, parentPath, tempName string) (windows.Handle, *os.File, error) {
	if windowsOps.beforeCreate != nil {
		if err := windowsOps.beforeCreate(); err != nil {
			return 0, nil, fmt.Errorf("secure install before-create hook: %w", err)
		}
	}
	if err := windowsValidateParentIdentity(parentHandle, parentPath); err != nil {
		return 0, nil, err
	}
	objectHandle, err := windowsCreateRelative(
		parentHandle,
		tempName,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE|windows.FILE_READ_ATTRIBUTES|windows.FILE_WRITE_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_CREATE,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("secure install create temporary object: %w", err)
	}
	writer, err := windowsDuplicateWriter(objectHandle, tempName)
	if err != nil {
		_ = windowsDeleteObject(objectHandle)
		_ = windows.CloseHandle(objectHandle)
		return 0, nil, err
	}
	return objectHandle, writer, nil
}

func windowsDuplicateWriter(objectHandle windows.Handle, tempName string) (*os.File, error) {
	process, err := windows.GetCurrentProcess()
	if err != nil {
		return nil, err
	}
	var writerHandle windows.Handle
	if err := windows.DuplicateHandle(process, objectHandle, process, &writerHandle, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, fmt.Errorf("secure install duplicate temporary object: %w", err)
	}
	writer := os.NewFile(uintptr(writerHandle), tempName)
	if writer == nil {
		_ = windows.CloseHandle(writerHandle)
		return nil, fmt.Errorf("secure install construct temporary writer")
	}
	return writer, nil
}

func (p *windowsPending) writer() *os.File { return p.writerFile }

func (p *windowsPending) publish(overwrite bool) error {
	if windowsOps.beforePublish != nil {
		if err := windowsOps.beforePublish(); err != nil {
			return fmt.Errorf("secure install before-publish hook: %w", err)
		}
	}
	if err := windowsValidateParentIdentity(p.parentHandle, p.parentPath); err != nil {
		return err
	}
	buffer, err := windowsRenameBuffer(p.parentHandle, p.finalName, overwrite)
	if err != nil {
		return err
	}
	if err := p.renameRetainedObject(buffer, overwrite); err != nil {
		return err
	}
	p.published = true
	p.tempPresent = false
	return nil
}

func windowsRenameBuffer(parentHandle windows.Handle, finalName string, overwrite bool) ([]byte, error) {
	name, err := windows.UTF16FromString(finalName)
	if err != nil {
		return nil, fmt.Errorf("secure install encode destination name: %w", err)
	}
	nameUnits := len(name) - 1
	if nameUnits > windowsRenameMaxCodeUnits {
		return nil, fmt.Errorf(
			"%w: %d UTF-16 code units exceeds %d",
			errWindowsRenameNameTooLong,
			nameUnits,
			windowsRenameMaxCodeUnits,
		)
	}
	nameBytes := nameUnits * 2
	var layout fileRenameInformation
	bufferSize := int(unsafe.Offsetof(layout.FileName)) + nameBytes
	buffer := make([]byte, bufferSize)
	rename := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	if overwrite {
		rename.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS
	}
	rename.RootDirectory = parentHandle
	rename.FileNameLength = uint32(nameBytes)
	if nameUnits > 0 {
		fileName := unsafe.Slice(&rename.FileName[0], nameUnits)
		copy(fileName, name[:nameUnits])
	}
	return buffer, nil
}

func (p *windowsPending) renameRetainedObject(buffer []byte, overwrite bool) error {
	var iosb windows.IO_STATUS_BLOCK
	bufferSize := len(buffer)
	if err := windows.NtSetInformationFile(p.objectHandle, &iosb, &buffer[0], uint32(bufferSize), windows.FileRenameInformation); err != nil {
		if !overwrite && errors.Is(err, windows.STATUS_OBJECT_NAME_COLLISION) {
			return fmt.Errorf("%w: %s", ErrDestinationExists, p.finalName)
		}
		return fmt.Errorf("secure install retained-object publication: %w", err)
	}
	return nil
}

func (p *windowsPending) applyMetadata(metadata Metadata) []metadataFailure {
	failures := make([]metadataFailure, 0, 3)
	if metadata.Mode != nil {
		if err := p.applyMode(*metadata.Mode); err != nil {
			failures = append(failures, metadataFailure{"chmod", err})
		}
	}
	if metadata.ModifiedAt != nil {
		mtime := windows.NsecToFiletime(metadata.ModifiedAt.UnixNano())
		if err := windows.SetFileTime(p.objectHandle, nil, nil, &mtime); err != nil {
			failures = append(failures, metadataFailure{"chtimes", err})
		}
	}
	if metadata.UID != nil && metadata.GID != nil {
		failures = append(failures, metadataFailure{"chown", errors.New("ownership metadata is unsupported on Windows")})
	}
	return failures
}

func (p *windowsPending) applyMode(mode os.FileMode) error {
	var basic fileBasicInformation
	if err := windows.GetFileInformationByHandleEx(
		p.objectHandle,
		windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&basic)),
		uint32(unsafe.Sizeof(basic)),
	); err != nil {
		return err
	}
	if mode.Perm()&0o200 == 0 {
		basic.FileAttributes |= windows.FILE_ATTRIBUTE_READONLY
	} else {
		basic.FileAttributes &^= windows.FILE_ATTRIBUTE_READONLY
	}
	return windows.SetFileInformationByHandle(
		p.objectHandle,
		windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&basic)),
		uint32(unsafe.Sizeof(basic)),
	)
}

func (p *windowsPending) abort() error {
	var errs []error
	errs = append(errs, p.closeWriter(), p.removeTemporary(), p.closeObject(), p.closeParent())
	return errors.Join(errs...)
}

func (p *windowsPending) closeWriter() error {
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

func (p *windowsPending) removeTemporary() error {
	if !p.tempPresent || p.objectHandle == 0 {
		return nil
	}
	if err := windowsDeleteObject(p.objectHandle); err != nil {
		return fmt.Errorf("secure install remove retained temporary object: %w", err)
	}
	p.tempPresent = false
	return nil
}

func (p *windowsPending) closeObject() error {
	if p.objectHandle == 0 {
		return nil
	}
	err := windows.CloseHandle(p.objectHandle)
	p.objectHandle = 0
	return err
}

func (p *windowsPending) closeParent() error {
	if p.parentHandle == 0 {
		return nil
	}
	err := windows.CloseHandle(p.parentHandle)
	p.parentHandle = 0
	return err
}

func windowsOpenParent(request Request) (windows.Handle, string, string, error) {
	anchor, err := nearestExistingDirectory(request.TrustedRoot)
	if err != nil {
		return 0, "", "", err
	}
	rel, err := filepath.Rel(anchor, request.Destination)
	if err != nil {
		return 0, "", "", err
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	parent, err := windowsOpenTrustedRoot(anchor)
	if err != nil {
		return 0, "", "", err
	}
	parentPath := anchor
	for _, part := range parts[:len(parts)-1] {
		next, openErr := windowsOpenParentComponent(parent, part)
		if openErr != nil {
			_ = windows.CloseHandle(parent)
			return 0, "", "", openErr
		}
		_ = windows.CloseHandle(parent)
		parent = next
		parentPath = filepath.Join(parentPath, part)
	}
	return parent, parentPath, parts[len(parts)-1], nil
}

func windowsOpenTrustedRoot(anchor string) (windows.Handle, error) {
	rootName, err := windows.NewNTUnicodeString(windowsNTPath(anchor))
	if err != nil {
		return 0, err
	}
	oa := &windows.OBJECT_ATTRIBUTES{ObjectName: rootName, Attributes: windows.OBJ_CASE_INSENSITIVE}
	oa.Length = uint32(unsafe.Sizeof(*oa))
	var iosb windows.IO_STATUS_BLOCK
	var allocation int64
	var parent windows.Handle
	if err := windows.NtCreateFile(
		&parent,
		windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		oa,
		&iosb,
		&allocation,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	); err != nil {
		return 0, fmt.Errorf("secure install open trusted root: %w", err)
	}
	if err := windowsValidateOpenedRoot(parent); err != nil {
		_ = windows.CloseHandle(parent)
		return 0, err
	}
	return parent, nil
}

func windowsValidateOpenedRoot(parent windows.Handle) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(parent, &info); err != nil {
		return fmt.Errorf("secure install inspect trusted root: %w", err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("secure install trusted root is a reparse point")
	}
	return nil
}

func windowsOpenParentComponent(parent windows.Handle, part string) (windows.Handle, error) {
	next, err := windowsCreateRelative(
		parent,
		part,
		windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		windows.FILE_ATTRIBUTE_DIRECTORY,
	)
	if errors.Is(err, windows.STATUS_OBJECT_NAME_NOT_FOUND) || errors.Is(err, windows.STATUS_OBJECT_PATH_NOT_FOUND) {
		next, err = windowsCreateRelative(
			parent,
			part,
			windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
			windows.FILE_CREATE,
			windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
			windows.FILE_ATTRIBUTE_DIRECTORY,
		)
	}
	if err != nil {
		return 0, fmt.Errorf("secure install open or create parent component %q: %w", part, err)
	}
	if err := windowsValidateParentComponent(next, part); err != nil {
		_ = windows.CloseHandle(next)
		return 0, err
	}
	return next, nil
}

func windowsValidateParentComponent(handle windows.Handle, part string) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("secure install parent component %q is a reparse point", part)
	}
	return nil
}

func windowsCreateRelative(root windows.Handle, name string, access, disposition, options, attributes uint32) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	oa := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: root,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	oa.Length = uint32(unsafe.Sizeof(*oa))
	var handle windows.Handle
	var iosb windows.IO_STATUS_BLOCK
	var allocation int64
	err = windows.NtCreateFile(
		&handle,
		access,
		oa,
		&iosb,
		&allocation,
		attributes,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options,
		0,
		0,
	)
	return handle, err
}

func windowsValidateParentIdentity(retained windows.Handle, path string) error {
	var retainedInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(retained, &retainedInfo); err != nil {
		return err
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	current, err := windows.CreateFile(
		pathPtr,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParentChanged, err)
	}
	defer windows.CloseHandle(current)
	var currentInfo windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(current, &currentInfo); err != nil {
		return err
	}
	if retainedInfo.VolumeSerialNumber != currentInfo.VolumeSerialNumber ||
		retainedInfo.FileIndexHigh != currentInfo.FileIndexHigh ||
		retainedInfo.FileIndexLow != currentInfo.FileIndexLow {
		return ErrParentChanged
	}
	return nil
}

func windowsDeleteObject(handle windows.Handle) error {
	deleteFile := byte(1)
	var iosb windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(handle, &iosb, &deleteFile, 1, windows.FileDispositionInformation)
}

func windowsNTPath(path string) string {
	cleaned := filepath.Clean(path)
	if strings.HasPrefix(cleaned, `\\`) {
		return `\??\UNC\` + strings.TrimPrefix(cleaned, `\\`)
	}
	return `\??\` + cleaned
}
