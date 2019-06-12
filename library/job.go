package library

import (
	"context"
	"time"

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

// Job represents a gitcollector.Job to perform a task on a borges.Library.
type Job struct {
	ID         string
	Lib        borges.Library
	Endpoints  []string
	TempFS     billy.Filesystem
	LocationID borges.LocationID
	Update     bool
	ProcessFn  JobFn
	Logger     log.Logger
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

var (
	errWrongJob = errors.NewKind("wrong job found")
	errNotJobID = errors.NewKind("couldn't assign an ID to a job")
)

// NewDownloadJobScheduleFn builds a new gitcollector.ScheduleFn that only
// schedules download jobs.
func NewDownloadJobScheduleFn(
	lib borges.Library,
	download chan gitcollector.Job,
	updateOnDownload bool,
	jobLogger log.Logger,
	temp billy.Filesystem,
) gitcollector.ScheduleFn {
	return func(
		opts *gitcollector.JobSchedulerOpts,
	) (gitcollector.Job, error) {
		job, err := jobFrom(download, opts.JobTimeout)
		if err != nil {
			return nil, err
		}

		job.Lib = lib
		job.TempFS = temp
		job.Update = updateOnDownload
		job.Logger = jobLogger
		return job, nil
	}
}

// NewUpdateJobScheduleFn builds a new gitcollector.SchedulerFn that only
// schedules update jobs.
func NewUpdateJobScheduleFn(
	lib borges.Library,
	update chan gitcollector.Job,
	jobLogger log.Logger,
) gitcollector.ScheduleFn {
	return func(
		opts *gitcollector.JobSchedulerOpts,
	) (gitcollector.Job, error) {
		job, err := jobFrom(update, opts.JobTimeout)
		if err != nil {
			return nil, err
		}

		job.Lib = lib
		job.Logger = jobLogger
		return job, nil
	}
}

// NewJobScheduleFn builds a new gitcollector.ScheduleFn that schedules download
// and update jobs in different queues.
func NewJobScheduleFn(
	lib borges.Library,
	download,
	update chan gitcollector.Job,
	updateOnDownload bool,
	jobLogger log.Logger,
	temp billy.Filesystem,
) gitcollector.ScheduleFn {
	var (
		downloadClosed bool
		updateClosed   bool
	)

	return func(
		opts *gitcollector.JobSchedulerOpts,
	) (gitcollector.Job, error) {
		var (
			job *Job
			err error
		)

		job, err = jobFrom(download, opts.JobTimeout)
		if err != nil {
			if !(gitcollector.ErrClosedChannel.Is(err) ||
				gitcollector.ErrNewJobsNotFound.Is(err)) {
				return nil, err
			}

			if gitcollector.ErrClosedChannel.Is(err) {
				downloadClosed = true
			}

			if updateClosed {
				return nil, err
			}

			job, err = jobFrom(update, opts.JobTimeout)
			if gitcollector.ErrClosedChannel.Is(err) {
				updateClosed = true
			}

			if downloadClosed && updateClosed {
				return nil, gitcollector.ErrClosedChannel.New()
			}

			if err != nil {
				return nil, err
			}
		}

		if job.Lib == nil {
			job.Lib = lib
		}

		// download job
		if len(job.Endpoints) > 0 {
			job.TempFS = temp
			job.Update = updateOnDownload
		}

		job.Logger = jobLogger
		return job, nil
	}
}

func jobFrom(queue chan gitcollector.Job, timeout time.Duration) (*Job, error) {
	select {
	case j, ok := <-queue:
		if !ok {
			return nil, gitcollector.ErrClosedChannel.New()
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
	case <-time.After(timeout):
		return nil, gitcollector.ErrNewJobsNotFound.New()
	}
}
