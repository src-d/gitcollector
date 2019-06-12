package downloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/gitcollector/updater"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-log.v1"
)

var (
	// ErrNotDownloadJob is returned when a not download job is found.
	ErrNotDownloadJob = errors.NewKind("not download job")

	// ErrRepoAlreadyExists is returned if there is an attempt to
	// retrieve an already downloaded git repository.
	ErrRepoAlreadyExists = errors.NewKind("%s already downloaded")
)

// Download is a library.JobFn function to download a git repository and store
// it in a borges.Library.
func Download(ctx context.Context, job *library.Job) error {
	logger := job.Logger.New(log.Fields{"job": "download", "id": job.ID})
	if len(job.Endpoints) == 0 || job.Lib == nil || job.TempFS == nil {
		err := ErrNotDownloadJob.New()
		logger.Errorf(err, "wrong job")
		return err
	}

	lib, ok := (job.Lib).(*siva.Library)
	if !ok {
		err := library.ErrNotSivaLibrary.New()
		logger.Errorf(err, "wrong library")
		return err
	}

	endpoint := job.Endpoints[0]
	logger = logger.New(log.Fields{"url": endpoint})

	repoID, err := library.NewRepositoryID(endpoint)
	if err != nil {
		logger.Errorf(err, "wrong repository endpoint %s", endpoint)
		return err
	}

	ok, _, locID, err := lib.Has(repoID)
	if err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	if ok {
		if job.Update {
			job.LocationID = locID
			return updater.Update(ctx, job)
		}

		logger.Errorf(err, "failed")
		return ErrRepoAlreadyExists.New(repoID)
	}

	logger.Infof("started")
	start := time.Now()
	if err := downloadRepository(
		logger,
		lib,
		job.TempFS,
		repoID,
		endpoint,
	); err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Infof("finished")
	return nil
}

func downloadRepository(
	logger log.Logger,
	lib *siva.Library,
	tmp billy.Filesystem,
	id borges.RepositoryID,
	endpoint string,
) error {
	clonePath := filepath.Join(
		cloneRootPath,
		fmt.Sprintf("%s_%d", id, time.Now().UnixNano()),
	)

	start := time.Now()
	repo, err := cloneRepo(tmp, clonePath, endpoint, string(id))
	if err != nil {
		return err
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("cloned")

	defer func() {
		if err := util.RemoveAll(tmp, clonePath); err != nil {
			logger.Warningf("couldn't remove %s", clonePath)
		}
	}()

	commit, err := headCommit(repo, string(id))
	if err != nil {
		return err
	}

	start = time.Now()
	root, err := rootCommit(repo, commit)
	if err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("root commit found")

	var (
		locID = borges.LocationID(root.Hash.String())
		r     borges.Repository
	)

	loc, err := lib.AddLocation(locID)
	if err != nil {
		if !siva.ErrLocationExists.Is(err) {
			return err
		}

		loc, err = lib.Location(locID)
		if err != nil {
			return err
		}

		r, err = loc.Get(id, borges.RWMode)
		if err != nil {
			r, err = loc.Init(id)
			if err != nil {
				return err
			}
		}
	}

	if r == nil {
		start = time.Now()
		r, err = createRootedRepo(loc, id, tmp, clonePath)
		if err != nil {
			return err
		}

		elapsed = time.Since(start).String()
		logger.Debugf("copied")
	}

	if _, err := createRemote(r.R(), string(id), endpoint); err != nil {
		if err := r.Close(); err != nil {
			logger.Warningf("couldn't close repository")
		}

		return err
	}

	start = time.Now()
	if err := r.R().Fetch(&git.FetchOptions{
		RemoteName: string(id),
	}); err != nil && err != git.NoErrAlreadyUpToDate {
		if err := r.Close(); err != nil {
			logger.Warningf("couldn't close repository")
		}

		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("fetched")

	start = time.Now()
	if err := r.Commit(); err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("commited")
	return nil
}

func createRootedRepo(
	loc borges.Location,
	repoID borges.RepositoryID,
	clonedFS billy.Filesystem,
	clonedPath string,
) (borges.Repository, error) {
	repo, err := loc.Init(repoID)
	if err != nil {
		return nil, err
	}

	if err := recursiveCopy(
		"/", repo.FS(),
		clonedPath, clonedFS,
	); err != nil {
		return nil, err
	}

	return repo, nil
}

func recursiveCopy(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
) error {
	stat, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = dstFS.MkdirAll(dst, stat.Mode())
		if err != nil {
			return err
		}

		files, err := srcFS.ReadDir(src)
		if err != nil {
			return err
		}

		for _, file := range files {
			srcPath := filepath.Join(src, file.Name())
			dstPath := filepath.Join(dst, file.Name())

			err = recursiveCopy(dstPath, dstFS, srcPath, srcFS)
			if err != nil {
				return err
			}
		}
	} else {
		err = copyFile(dst, dstFS, src, srcFS, stat.Mode())
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
	mode os.FileMode,
) error {
	_, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	fo, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer fo.Close()

	fd, err := dstFS.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = io.Copy(fd, fo)
	if err != nil {
		fd.Close()
		dstFS.Remove(dst)
		return err
	}

	return nil
}
