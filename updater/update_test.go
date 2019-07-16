package updater

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-log.v1"

	"github.com/stretchr/testify/require"
)

func TestUpdate(t *testing.T) {
	var req = require.New(t)

	locID := borges.LocationID(
		"f2cee90acf3c6644d51a37057845b98ab1580932")

	endpoints := []string{
		// 263 commits main
		"git://github.com/jtoy/awesome-tensorflow.git",
		// 257 commits forked
		"git://github.com/SiweiLuo/awesome-tensorflow.git",
		// 257 commits forked
		"git://github.com/youtang1993/awesome-tensorflow.git",
	}

	dir1, err := ioutil.TempDir("", "gitcollector")
	req.NoError(err)
	defer os.RemoveAll(dir1)

	lib1, loc1 := setupLocation(t, dir1, locID, endpoints)

	repo, err := loc1.Get("", borges.ReadOnlyMode)
	req.NoError(err)

	_, err = repo.FS().Stat("objects")
	req.True(os.IsNotExist(err))

	job := &library.Job{
		ID:         "foo",
		Type:       library.JobUpdate,
		Lib:        lib1,
		LocationID: locID,
		AuthToken:  func(string) string { return "" },
		Logger:     log.New(nil),
	}

	// Update all remotes
	req.NoError(Update(context.TODO(), job))

	repo, err = loc1.Get("", borges.ReadOnlyMode)
	req.NoError(err)

	_, err = repo.FS().Stat("objects")
	req.NoError(err)
	size1 := objectsSize(t, repo.FS())

	dir2, err := ioutil.TempDir("", "gitcollector")
	req.NoError(err)
	defer os.RemoveAll(dir2)

	lib2, loc2 := setupLocation(t, dir2, locID, endpoints)

	repo, err = loc2.Get("", borges.ReadOnlyMode)
	req.NoError(err)

	_, err = repo.FS().Stat("objects")
	req.True(os.IsNotExist(err))

	job.Lib = lib2
	job.Endpoints = []string{endpoints[1]}

	// Update just one remote
	req.NoError(Update(context.TODO(), job))

	repo, err = loc2.Get("", borges.ReadOnlyMode)
	req.NoError(err)

	_, err = repo.FS().Stat("objects")
	req.NoError(err)
	size2 := objectsSize(t, repo.FS())

	req.True(size1 > size2)
}

func setupLocation(
	t *testing.T,
	path string,
	locID borges.LocationID,
	endpoints []string,
) (borges.Library, *siva.Location) {
	t.Helper()
	var req = require.New(t)

	fs := osfs.New(path)
	lib, err := siva.NewLibrary("test", fs, &siva.LibraryOptions{
		Transactional: true,
	})
	req.NoError(err)

	l, err := lib.AddLocation(locID)
	req.NoError(err)

	loc, ok := l.(*siva.Location)
	req.True(ok)

	for _, ep := range endpoints {
		repoID, err := borges.NewRepositoryID(ep)
		req.NoError(err)

		repo, err := loc.Init(repoID)
		req.NoError(err)
		req.NoError(repo.Commit())
	}

	return lib, loc
}

func objectsSize(t *testing.T, fs billy.Filesystem) int64 {
	t.Helper()
	var req = require.New(t)

	entries, err := fs.ReadDir("objects/pack")
	req.NoError(err)

	var size int64
	for _, e := range entries {
		size += e.Size()
	}

	return size
}
