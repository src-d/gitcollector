package testhelper

import (
	"fmt"
	"os"

	"gopkg.in/src-d/go-billy.v4"
)

// ErrBrokenFS is a default error that will be returned if some operation is mocked
var ErrBrokenFS = fmt.Errorf("mocked")

// BrokenFSOptions contains list of operations that will be broken in the scope of this mock up
type BrokenFSOptions struct {
	FailedOpen     bool
	FailedOpenFile bool
	FailedStat     bool
	FailedChroot   bool
	FailedCreate   bool
	FailedTempFile bool
}

// BrokenFS is a simple billy.Filesystem mockup
type BrokenFS struct {
	billy.Filesystem
	opts BrokenFSOptions
}

// NewBrokenFS is BrokenFS constructor
func NewBrokenFS(fs billy.Filesystem, opts BrokenFSOptions) *BrokenFS {
	return &BrokenFS{Filesystem: fs, opts: opts}
}

func (fs *BrokenFS) Open(filename string) (billy.File, error) {
	if fs.opts.FailedOpen {
		return nil, ErrBrokenFS
	}
	return fs.Filesystem.Open(filename)
}

func (fs *BrokenFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if fs.opts.FailedOpenFile {
		return nil, ErrBrokenFS
	}
	return fs.Filesystem.OpenFile(filename, flag, perm)
}

func (fs *BrokenFS) Stat(filename string) (os.FileInfo, error) {
	if fs.opts.FailedStat {
		return nil, ErrBrokenFS
	}
	return fs.Filesystem.Stat(filename)
}

func (fs *BrokenFS) Chroot(path string) (billy.Filesystem, error) {
	if fs.opts.FailedChroot {
		return nil, ErrBrokenFS
	}
	newFS, err := fs.Filesystem.Chroot(path)
	return NewBrokenFS(newFS, fs.opts), err
}

func (fs *BrokenFS) Create(filename string) (billy.File, error) {
	if fs.opts.FailedCreate {
		return nil, ErrBrokenFS
	}
	return fs.Filesystem.Create(filename)
}

func (fs *BrokenFS) TempFile(dir, prefix string) (billy.File, error) {
	if fs.opts.FailedTempFile {
		return nil, ErrBrokenFS
	}
	return fs.Filesystem.TempFile(dir, prefix)
}
