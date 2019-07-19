package downloader

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/gitcollector/updater"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-errors.v1"
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
		logger.Infof(err.Error())
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
	repo, err := CloneRepository(
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

	start = time.Now()
	root, err := RootCommit(repo, id.String())
	if err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{
		"elapsed": elapsed,
		"root":    root.Hash.String(),
	}).Debugf("root commit found")

	start = time.Now()
	locID := borges.LocationID(root.Hash.String())
	r, err := PrepareRepository(
		ctx, lib, locID, id, endpoint, tmp, clonePath,
	)

	if err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{
		"elapsed": elapsed,
	}).Debugf("rooted repository ready")

	start = time.Now()
	if err := FetchChanges(ctx, r, id.String(), token); err != nil {
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
