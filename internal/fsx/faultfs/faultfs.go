package faultfs

import (
	"errors"
	"io"
	"io/fs"
	"sync"

	"github.com/franchoy/coldkeep/internal/fsx"
)

var (
	ErrFaultWrite  = errors.New("faultfs: write fault")
	ErrFaultSync   = errors.New("faultfs: sync fault")
	ErrFaultClose  = errors.New("faultfs: close fault")
	ErrFaultRename = errors.New("faultfs: rename fault")
	ErrFaultMkdir  = errors.New("faultfs: mkdir fault")
	ErrFaultStat   = errors.New("faultfs: stat fault")
	ErrFaultRemove = errors.New("faultfs: remove fault")
	ErrFaultENOSPC = errors.New("faultfs: no space left on device")
)

type Operation string

const (
	OpOpen     Operation = "open"
	OpOpenFile Operation = "open_file"
	OpStat     Operation = "stat"
	OpMkdirAll Operation = "mkdir_all"
	OpRename   Operation = "rename"
	OpRemove   Operation = "remove"
	OpReadDir  Operation = "read_dir"
	OpWalkDir  Operation = "walk_dir"
	OpWrite    Operation = "write"
	OpSync     Operation = "sync"
	OpClose    Operation = "close"
)

type Fault struct {
	Op            Operation
	After         int
	Err           error
	PartialWriteN int
	ENOSPCAfter   int64
}

type Script struct {
	mu     sync.Mutex
	faults []Fault
	calls  map[Operation]int
	bytes  int64
}

func NewScript(faults ...Fault) *Script {
	cp := append([]Fault(nil), faults...)
	return &Script{
		faults: cp,
		calls:  make(map[Operation]int),
	}
}

func (s *Script) CallCount(op Operation) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

func (s *Script) BytesWritten() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (s *Script) record(op Operation) (Fault, bool) {
	if s == nil {
		return Fault{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls[op]++
	callN := s.calls[op]

	for _, fault := range s.faults {
		if fault.Op != op {
			continue
		}
		after := fault.After
		if after == 0 {
			after = 1
		}
		if callN == after {
			return fault, true
		}
	}

	return Fault{}, false
}

func (s *Script) recordWriteAttempt(n int) (Fault, bool) {
	if s == nil {
		return Fault{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls[OpWrite]++
	callN := s.calls[OpWrite]

	for _, fault := range s.faults {
		if fault.Op != OpWrite {
			continue
		}

		if fault.ENOSPCAfter > 0 {
			if s.bytes+int64(n) > fault.ENOSPCAfter {
				return fault, true
			}
			continue
		}

		after := fault.After
		if after == 0 {
			after = 1
		}
		if callN == after {
			return fault, true
		}
	}

	return Fault{}, false
}

func (s *Script) recordPartialWrite(n int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes += int64(n)
}

type FS struct {
	Base   fsx.FS
	Script *Script
}

func New(base fsx.FS, script *Script) FS {
	return FS{Base: base, Script: script}
}

func (f FS) base() fsx.FS {
	if f.Base != nil {
		return f.Base
	}
	return fsx.Default()
}

func (f FS) Open(name string) (fsx.File, error) {
	if fault, ok := f.Script.record(OpOpen); ok {
		return nil, fault.errOrDefault(errors.New("faultfs: open fault"))
	}
	file, err := f.base().Open(name)
	if err != nil {
		return nil, err
	}
	return File{File: file, Script: f.Script}, nil
}

func (f FS) OpenFile(name string, flag int, perm fs.FileMode) (fsx.File, error) {
	if fault, ok := f.Script.record(OpOpenFile); ok {
		return nil, fault.errOrDefault(errors.New("faultfs: open_file fault"))
	}
	file, err := f.base().OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return File{File: file, Script: f.Script}, nil
}

func (f FS) Stat(name string) (fs.FileInfo, error) {
	if fault, ok := f.Script.record(OpStat); ok {
		return nil, fault.errOrDefault(ErrFaultStat)
	}
	return f.base().Stat(name)
}

func (f FS) MkdirAll(path string, perm fs.FileMode) error {
	if fault, ok := f.Script.record(OpMkdirAll); ok {
		return fault.errOrDefault(ErrFaultMkdir)
	}
	return f.base().MkdirAll(path, perm)
}

func (f FS) Rename(oldpath string, newpath string) error {
	if fault, ok := f.Script.record(OpRename); ok {
		return fault.errOrDefault(ErrFaultRename)
	}
	return f.base().Rename(oldpath, newpath)
}

func (f FS) Remove(name string) error {
	if fault, ok := f.Script.record(OpRemove); ok {
		return fault.errOrDefault(ErrFaultRemove)
	}
	return f.base().Remove(name)
}

func (f FS) ReadDir(name string) ([]fs.DirEntry, error) {
	if fault, ok := f.Script.record(OpReadDir); ok {
		return nil, fault.errOrDefault(errors.New("faultfs: read_dir fault"))
	}
	return f.base().ReadDir(name)
}

func (f FS) WalkDir(root string, fn fs.WalkDirFunc) error {
	if fault, ok := f.Script.record(OpWalkDir); ok {
		return fault.errOrDefault(errors.New("faultfs: walk_dir fault"))
	}
	return f.base().WalkDir(root, fn)
}

type File struct {
	fsx.File
	Script *Script
}

func (f File) Write(p []byte) (int, error) {
	if fault, ok := f.Script.recordWriteAttempt(len(p)); ok {
		if fault.PartialWriteN > 0 {
			n := fault.PartialWriteN
			if n > len(p) {
				n = len(p)
			}
			written, err := f.File.Write(p[:n])
			f.Script.recordPartialWrite(written)
			if err != nil {
				return written, err
			}
			return written, fault.errOrDefault(ErrFaultWrite)
		}
		if fault.ENOSPCAfter > 0 {
			return 0, fault.errOrDefault(ErrFaultENOSPC)
		}
		return 0, fault.errOrDefault(ErrFaultWrite)
	}

	written, err := f.File.Write(p)
	f.Script.recordPartialWrite(written)
	return written, err
}

func (f File) Sync() error {
	if fault, ok := f.Script.record(OpSync); ok {
		return fault.errOrDefault(ErrFaultSync)
	}
	return f.File.Sync()
}

func (f File) Close() error {
	if fault, ok := f.Script.record(OpClose); ok {
		return fault.errOrDefault(ErrFaultClose)
	}
	return f.File.Close()
}

func (f File) ReadAt(p []byte, off int64) (int, error) {
	return f.File.ReadAt(p, off)
}

func (f File) Seek(offset int64, whence int) (int64, error) {
	return f.File.Seek(offset, whence)
}

func (f File) Truncate(size int64) error {
	return f.File.Truncate(size)
}

func (fault Fault) errOrDefault(defaultErr error) error {
	if fault.Err != nil {
		return fault.Err
	}
	return defaultErr
}

var _ fsx.FS = FS{}
var _ fsx.File = File{}
var _ io.Writer = File{}
