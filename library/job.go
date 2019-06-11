package library

import (
	"context"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrJobFnNotFound is returned when theres is no function to
	// process a job.
	ErrJobFnNotFound = errors.NewKind(
		"process function not found for library.Job")
)

// Job represents a gitcollector.Job to perform a task on a borges.Library.
type Job struct {
	Lib        borges.Library
	Endpoints  []string
	TempFS     billy.Filesystem
	LocationID borges.LocationID
	ProcessFn  JobFn
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

// JobScheduler is a gitcollector.JobScheduler implementation to schedule Jobs.
type JobScheduler struct {
	lib      borges.Library
	temp     billy.Filesystem
	download chan *Job
	update   chan *Job
	jobs     chan gitcollector.Job
	cancel   chan struct{}
}

var _ gitcollector.JobScheduler = (*JobScheduler)(nil)

const (
	schedCapacity      = 1000
	retrieveJobTimeout = 3 * time.Second
	waitNewJobs        = 30 * time.Second
)

var (
	errNewJobsNotFound = errors.NewKind("couldn't find new jobs to schedule")
	errClosedChannel   = errors.NewKind("channel is closed")
)

// NewJobScheduler builds a new JobScheduler.
func NewJobScheduler(
	download, update chan *Job,
	lib borges.Library,
	temp billy.Filesystem,
) *JobScheduler {
	return &JobScheduler{
		lib:      lib,
		temp:     temp,
		download: download,
		update:   update,
		jobs:     make(chan gitcollector.Job, schedCapacity),
		cancel:   make(chan struct{}),
	}
}

// Jobs implements the gitcollector.JobScheduler interface.
func (s *JobScheduler) Jobs() chan gitcollector.Job {
	return s.jobs
}

// Finish implements the gitcollector.JobScheduler interface.
func (s *JobScheduler) Finish() {
	s.cancel <- struct{}{}
}

// Schedule implements the gitcollector.JobScheduler interface.
func (s *JobScheduler) Schedule() {
	for {
		select {
		case <-s.cancel:
			return
		default:
			job, err := s.schedule()
			if err != nil {
				if errNewJobsNotFound.Is(err) {
					select {
					case <-s.cancel:
						return
					case <-time.After(waitNewJobs):
						continue
					}
				}

				if errClosedChannel.Is(err) {
					// TODO: log errors

				}
			}

			select {
			case s.jobs <- job:
			case <-s.cancel:
				return
			}
		}
	}
}

func (s *JobScheduler) schedule() (gitcollector.Job, error) {
	if len(s.download) == 0 && len(s.update) == 0 {
		return nil, errNewJobsNotFound.New()
	}

	if len(s.download) > 0 {
		return s.getJobFrom(s.download)
	}

	return s.getJobFrom(s.update)
}

func (s *JobScheduler) getJobFrom(queue chan *Job) (*Job, error) {
	select {
	case job, ok := <-queue:
		if !ok {
			return nil, errClosedChannel.New()
		}

		if job.Lib == nil {
			job.Lib = s.lib
		}

		if job.TempFS == nil && len(job.Endpoints) > 0 {
			job.TempFS = s.temp
		}

		return job, nil
	case <-time.After(retrieveJobTimeout):
		return nil, errNewJobsNotFound.New()
	}
}
