package library

import (
	"context"

	"github.com/src-d/gitcollector"
	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-log.v1"

	"github.com/google/uuid"
)

var (
	// ErrJobFnNotFound is returned when theres is no function to
	// process a job.
	ErrJobFnNotFound = errors.NewKind(
		"process function not found for library.Job")
)

// JobType represents the type of the Job.
type JobType uint8

const (
	// JobDownload represents a Download Job.
	JobDownload = 1 << iota
	// JobUpdate represents an Update Job.
	JobUpdate
)

// Job represents a gitcollector.Job to perform a task on a borges.Library.
type Job struct {
	ID          string
	Type        JobType
	Lib         borges.Library
	Endpoints   []string
	TempFS      billy.Filesystem
	LocationID  borges.LocationID
	AllowUpdate bool
	AuthToken   AuthTokenFn
	ProcessFn   JobFn
	Logger      log.Logger
}

var _ gitcollector.Job = (*Job)(nil)

// JobFn represents the task to be performed by a Job.
type JobFn func(context.Context, *Job) error

// Process implements the Job interface.
func (j *Job) Process(ctx context.Context) error {
	if j.ProcessFn == nil {
		return ErrJobFnNotFound.New()
	}

	return j.ProcessFn(ctx, j)
}

// AuthTokenFn retrieve and authentication token if any for the given endpoint.
type AuthTokenFn func(endpoint string) string

func getAuthTokenByOrg(tokens map[string]string) AuthTokenFn {
	if tokens == nil {
		tokens = map[string]string{}
	}

	return func(endpoint string) string {
		org := GetOrgFromEndpoint(endpoint)
		return tokens[org]
	}
}

var (
	errWrongJob   = errors.NewKind("wrong job found")
	errNotJobID   = errors.NewKind("couldn't assign an ID to a job")
	errClosedChan = errors.NewKind("channel closed")
)

// NewDownloadJobScheduleFn builds a new gitcollector.ScheduleFn that only
// schedules download jobs.
func NewDownloadJobScheduleFn(
	lib borges.Library,
	download chan gitcollector.Job,
	downloadFn JobFn,
	updateOnDownload bool,
	authTokens map[string]string,
	jobLogger log.Logger,
	temp billy.Filesystem,
) gitcollector.JobScheduleFn {
	return func(ctx context.Context) (gitcollector.Job, error) {
		job, err := jobFrom(ctx, download)
		if err != nil {
			if errClosedChan.Is(err) {
				err = gitcollector.ErrJobSource.New()
			}

			return nil, err
		}

		job.Lib = lib
		job.TempFS = temp
		job.ProcessFn = downloadFn
		job.AllowUpdate = updateOnDownload
		job.AuthToken = getAuthTokenByOrg(authTokens)
		job.Logger = jobLogger
		return job, nil
	}
}

// NewUpdateJobScheduleFn builds a new gitcollector.SchedulerFn that only
// schedules update jobs.
func NewUpdateJobScheduleFn(
	lib borges.Library,
	update chan gitcollector.Job,
	updateFn JobFn,
	authTokens map[string]string,
	jobLogger log.Logger,
) gitcollector.JobScheduleFn {
	return func(ctx context.Context) (gitcollector.Job, error) {
		job, err := jobFrom(ctx, update)
		if err != nil {
			if errClosedChan.Is(err) {
				err = gitcollector.ErrJobSource.New()
			}

			return nil, err
		}

		job.Lib = lib
		job.ProcessFn = updateFn
		job.AuthToken = getAuthTokenByOrg(authTokens)
		job.Logger = jobLogger
		return job, nil
	}
}

// NewJobScheduleFn builds a new gitcollector.ScheduleFn that schedules download
// and update jobs in different queues.
func NewJobScheduleFn(
	lib borges.Library,
	download, update chan gitcollector.Job,
	downloadFn, updateFn JobFn,
	updateOnDownload bool,
	authTokens map[string]string,
	jobLogger log.Logger,
	temp billy.Filesystem,
) gitcollector.JobScheduleFn {
	setupJob := func(job *Job) error {
		if job.Lib == nil {
			job.Lib = lib
		}

		switch job.Type {
		case JobDownload:
			job.TempFS = temp
			job.AllowUpdate = updateOnDownload
			job.ProcessFn = downloadFn
		case JobUpdate:
			job.ProcessFn = updateFn
		default:
			return errWrongJob.New()
		}

		job.AuthToken = getAuthTokenByOrg(authTokens)
		job.Logger = jobLogger
		return nil
	}

	return func(ctx context.Context) (gitcollector.Job, error) {
		if download == nil && update == nil {
			return nil, gitcollector.ErrJobSource.New()
		}

		var (
			job *Job
			err error
		)

		if download != nil || len(download) > 0 {
			job, err = jobFrom(ctx, download)
			if err != nil {
				if !(errClosedChan.Is(err) ||
					gitcollector.ErrNewJobsNotFound.Is(err)) {
					return nil, err
				}

				if errClosedChan.Is(err) {
					download = nil
				}
			}
		}

		if job != nil {
			if err := setupJob(job); err != nil {
				return nil, gitcollector.
					ErrNewJobsNotFound.New()
			}

			return job, nil
		}

		if update == nil && download == nil {
			return nil, gitcollector.ErrJobSource.New()
		}

		if update == nil {
			return nil, gitcollector.ErrNewJobsNotFound.New()
		}

		job, err = jobFrom(ctx, update)
		if err != nil {
			if errClosedChan.Is(err) {
				update = nil
			}

			return nil, gitcollector.ErrNewJobsNotFound.New()
		}

		if err := setupJob(job); err != nil {
			return nil, gitcollector.ErrNewJobsNotFound.New()
		}

		return job, nil
	}
}

func jobFrom(ctx context.Context, queue chan gitcollector.Job) (*Job, error) {
	if queue == nil {
		return nil, errClosedChan.New()
	}

	select {
	case j, ok := <-queue:
		if !ok {
			return nil, errClosedChan.New()
		}

		job, ok := j.(*Job)
		if !ok {
			return nil, errWrongJob.New()
		}

		id, err := uuid.NewRandom()
		if err != nil {
			return nil, errNotJobID.Wrap(err)
		}

		job.ID = id.String()
		return job, nil
	case <-ctx.Done():
		return nil, gitcollector.ErrNewJobsNotFound.New()
	}
}
