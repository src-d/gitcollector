package updater

import (
	"context"
	"time"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/http"
	"gopkg.in/src-d/go-log.v1"
)

var (
	// ErrNotUpdateJob is returned when a not update job is found.
	ErrNotUpdateJob = errors.NewKind("not update job")
)

// Update is a library.JobFn function to update a git repository alreayd stored
// in a borges.Library.
func Update(ctx context.Context, job *library.Job) error {
	logger := job.Logger.New(log.Fields{"job": "update", "id": job.ID})
	if job.Type != library.JobUpdate {
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

	repo, err := loc.Get("", borges.RWMode)
	if err != nil {
		logger.Errorf(err, "couldn't get repository")
		return err
	}

	var remote string
	if len(job.Endpoints) == 1 {
		// job redirected from download
		ep := job.Endpoints[0]

		logger = logger.New(log.Fields{"url": ep})

		id, err := library.NewRepositoryID(ep)
		if err != nil {
			logger.Errorf(err, "wrong repository endpoint")
			return err
		}

		remote = id.String()
	}

	remotes, err := remotesToUpdate(repo, remote)
	if err != nil {
		logger.Errorf(err, "couldn't get remotes")
		return err
	}

	if len(job.Endpoints) == 0 {
		// it will update the whole location, add all the endpoints
		// to be updated to the job
		var endpoints []string
		for _, remote := range remotes {
			endpoints = append(endpoints, remote.Config().URLs[0])
		}

		job.Endpoints = endpoints
	}

	logger.Infof("started")
	start := time.Now()
	if err := updateRepository(
		ctx,
		logger,
		repo,
		remotes,
		job.AuthToken,
	); err != nil {
		logger.Errorf(err, "failed")
		return err
	}

	elapsed := time.Since(start).String()
	logger.With(log.Fields{"elapsed": elapsed}).Infof("finished")
	return nil
}

func remotesToUpdate(
	repo borges.Repository,
	remote string,
) ([]*git.Remote, error) {
	var (
		remotes []*git.Remote
		err     error
	)

	if remote == "" {
		remotes, err = repo.R().Remotes()
		if err != nil {
			return nil, err
		}
	} else {
		r, err := repo.R().Remote(remote)
		if err != nil {
			return nil, err
		}

		remotes = append(remotes, r)
	}

	return remotes, nil
}

func updateRepository(
	ctx context.Context,
	logger log.Logger,
	repo borges.Repository,
	remotes []*git.Remote,
	authToken library.AuthTokenFn,
) error {
	var alreadyUpdated int
	start := time.Now()
	for _, remote := range remotes {
		opts := &git.FetchOptions{}
		urls := remote.Config().URLs
		if len(urls) > 0 {
			token := authToken(urls[0])
			if token != "" {
				opts.Auth = &http.BasicAuth{
					Username: "gitcollector",
					Password: token,
				}
			}
		}

		err := remote.FetchContext(ctx, opts)
		if err != nil && err != git.NoErrAlreadyUpToDate {
			if err := repo.Close(); err != nil {
				logger.Warningf("couldn't close repository")
			}

			return err
		}

		name := remote.Config().Name
		if err == git.NoErrAlreadyUpToDate {
			alreadyUpdated++
			logger.With(log.Fields{"remote": name}).
				Debugf("already up to date")
		}

		if err == nil {
			logger.With(log.Fields{"remote": name}).
				Debugf("updated")
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
