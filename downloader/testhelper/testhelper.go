package testhelper

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
)

// Helper is a struct that's used to simplify preparation and execution of download tests
type Helper struct {
	Dir    string
	Siva   string
	FS     billy.Filesystem
	TempFS billy.Filesystem
	Lib    *siva.Library
}

// NewHelper is Helper's constructor
func NewHelper() (*Helper, func(), error) {
	dir, err := ioutil.TempDir("", "gitcollector")
	if err != nil {
		return nil, func() {}, err
	}
	closer := func() { os.RemoveAll(dir) }

	sivaPath := filepath.Join(dir, "Siva")
	if err := os.Mkdir(sivaPath, 0775); err != nil {
		return nil, closer, err

	}

	downloaderPath := filepath.Join(dir, "downloader")
	if err := os.Mkdir(downloaderPath, 0775); err != nil {
		return nil, closer, err
	}

	fs := osfs.New(sivaPath)
	lib, err := siva.NewLibrary("test", fs, &siva.LibraryOptions{
		Bucket:        2,
		Transactional: true,
	})
	if err != nil {
		return nil, closer, err
	}

	return &Helper{
		Dir:    dir,
		Siva:   sivaPath,
		FS:     fs,
		TempFS: osfs.New(downloaderPath),
		Lib:    lib,
	}, closer, nil
}
