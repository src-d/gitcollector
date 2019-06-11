package updater

import (
	"context"
	"time"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-log.v1"
)

var (
	// ErrNotUpdateJob is returned when a not update job is found.
	ErrNotUpdateJob = errors.NewKind("not update job")
)

// Update is a library.JobFn function to update a git repository alreayd stored
// in a borges.Library.
func Update(_ context.Context, job *library.Job) error {
	logger := job.Logger.New(log.Fields{"job": "update", "id": job.ID})
	if !job.Update {
		err := ErrNotUpdateJob.New()
		logger.Errorf(err, "wrong job")
		return err
	}

	lib, ok := (job.Lib).(*siva.Library)
	if !ok {
		err := library.ErrNotSivaLibrary.New()
		logger.Errorf(err, "wrong library")
		return err
	}

	logger = logger.New(log.Fields{"location": job.LocationID})
	location, err := lib.Location(job.LocationID)
	if err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	loc, ok := location.(*siva.Location)
	if !ok {
		err := library.ErrNotSivaLocation.New()
		logger.Errorf(err, "wrong location")
		return err
	}

	var remote string
	if len(job.Endpoints) > 0 {
		ep := job.Endpoints[0]
		id, err := library.NewRepositoryID(ep)
		if err != nil {
			logger.Errorf(err, "wrong repository endpoint %s", ep)
			return err
		}

		remote = id.String()
	}

	logger.Infof("started")
	start := time.Now()
	if err := updateRepository(logger, loc, remote); err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Infof("finished")
	return nil
}

func updateRepository(
	logger log.Logger,
	loc *siva.Location,
	remote string,
) error {
	repo, err := loc.Get("", borges.RWMode)
	if err != nil {
		return err
	}

	var remotes []*git.Remote
	if remote == "" {
		remotes, err = repo.R().Remotes()
		if err != nil {
			return err
		}
	} else {
		r, err := repo.R().Remote(remote)
		if err != nil {
			return err
		}

		remotes = append(remotes, r)
	}

	var alreadyUpdated int
	start := time.Now()
	for _, remote := range remotes {
		err := remote.Fetch(&git.FetchOptions{})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			if err := repo.Close(); err != nil {
				logger.Warningf("couldn't close repository")
			}

			return err
		}

		if err == git.NoErrAlreadyUpToDate {
			alreadyUpdated++
		}
	}

	if len(remotes) == alreadyUpdated {
		elapsed := time.Since(start).String()
		logger.With(log.Fields{"elapsed": elapsed}).
			Debugf("location already up to date")
		return repo.Close()
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("fetched")

	start = time.Now()
	if err := repo.Commit(); err != nil {
		return err
	}

	elapsed = time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Debugf("commited")
	return nil
}
