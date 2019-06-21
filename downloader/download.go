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
	"gopkg.in/src-d/go-git.v4/plumbing/transport/http"
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
	if job.Type != library.JobDownload ||
		len(job.Endpoints) == 0 ||
		job.Lib == nil ||
		job.TempFS == nil {
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

	ok, locID, err := libHas(ctx, lib, repoID)
	if err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	if ok {
		if job.AllowUpdate {
			job.Type = library.JobUpdate
			job.LocationID = locID
			return updater.Update(ctx, job)
		}

		err := ErrRepoAlreadyExists.New(repoID)
		logger.Warningf(err.Error())
		return err
	}

	logger.Infof("started")
	start := time.Now()
	if err := downloadRepository(
		ctx,
		logger,
		lib,
		job.TempFS,
		repoID,
		endpoint,
		job.AuthToken,
	); err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Infof("finished")
	return nil
}

func libHas(
	ctx context.Context,
	lib borges.Library,
	id borges.RepositoryID,
) (bool, borges.LocationID, error) {
	var (
		ok    bool
		locID borges.LocationID
		err   error
		done  = make(chan struct{})
	)

	go func() {
		ok, _, locID, err = lib.Has(id)
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		ok = false
		err = ctx.Err()
	}

	return ok, locID, err
}

func downloadRepository(
	ctx context.Context,
	logger log.Logger,
	lib *siva.Library,
	tmp billy.Filesystem,
	id borges.RepositoryID,
	endpoint string,
	authToken library.AuthTokenFn,
) error {
	clonePath := filepath.Join(
		cloneRootPath,
		fmt.Sprintf("%s_%d", id, time.Now().UnixNano()),
	)

	token := authToken(endpoint)

	start := time.Now()
	repo, err := cloneRepo(
		ctx, tmp, clonePath, endpoint, id.String(), token,
	)

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

	commit, err := headCommit(repo, id.String())
	if err != nil {
		return err
	}

	logger.With(log.Fields{
		"head": commit.Hash.String(),
	}).Debugf("head commit found")

	start = time.Now()
	root, err := rootCommit(repo, commit)
	if err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{
		"elapsed": elapsed,
		"root":    root.Hash.String(),
	}).Debugf("root commit found")

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
		r, err = createRootedRepo(ctx, loc, id, tmp, clonePath)
		if err != nil {
			return err
		}

		elapsed = time.Since(start).String()
		logger.With(log.Fields{"elapsed": elapsed}).Debugf("copied")
	}

	if _, err := createRemote(r.R(), id.String(), endpoint); err != nil {
		if err := r.Close(); err != nil {
			logger.Warningf("couldn't close repository")
		}

		return err
	}

	opts := &git.FetchOptions{
		RemoteName: id.String(),
	}

	if token != "" {
		opts.Auth = &http.BasicAuth{
			Username: "gitcollector",
			Password: token,
		}
	}

	start = time.Now()
	if err := r.R().FetchContext(
		ctx, opts,
	); err != nil && err != git.NoErrAlreadyUpToDate {
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
	ctx context.Context,
	loc borges.Location,
	repoID borges.RepositoryID,
	clonedFS billy.Filesystem,
	clonedPath string,
) (borges.Repository, error) {
	repo, err := loc.Init(repoID)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		err = recursiveCopy(
			"/", repo.FS(),
			clonedPath, clonedFS,
		)

		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		repo.Close()
		repo = nil
	}

	return repo, err
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
