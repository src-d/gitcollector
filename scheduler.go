package gitcollector

import (
	"context"
	"time"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrNewJobsNotFound must be returned by a JobScheduleFn when it can't
	// find new Jobs.
	ErrNewJobsNotFound = errors.NewKind(
		"couldn't find new jobs to schedule")

	// ErrJobSource must be returned by a JobScheduleFn when the source of
	// job is closed.
	ErrJobSource = errors.NewKind("job source is closed")
)

// JobScheduleFn is a function to schedule the next Job.
type JobScheduleFn func(context.Context) (Job, error)

type jobScheduler struct {
	jobs     chan Job
	schedule JobScheduleFn
	cancel   chan struct{}
	opts     *WorkerPoolOpts
}

const (
	schedCapacity = 1000
	jobTimeout    = 3 * time.Second
	newJobTimeout = 30 * time.Second
)

func newJobScheduler(
	schedule JobScheduleFn,
	opts *WorkerPoolOpts,
) *jobScheduler {
	if opts.SchedulerCapacity <= 0 {
		opts.SchedulerCapacity = schedCapacity
	}

	if opts.WaitJobTimeout <= 0 {
		opts.WaitJobTimeout = jobTimeout
	}

	if opts.WaitNewJobTimeout <= 0 {
		opts.WaitNewJobTimeout = newJobTimeout
	}

	return &jobScheduler{
		jobs:     make(chan Job, opts.SchedulerCapacity),
		schedule: schedule,
		cancel:   make(chan struct{}),
		opts:     opts,
	}
}

func (s *jobScheduler) finish() {
	s.cancel <- struct{}{}
}

func (s *jobScheduler) Schedule() {
	for {
		select {
		case <-s.cancel:
			return
		default:
			ctx, cancel := context.WithTimeout(
				context.Background(),
				s.opts.WaitJobTimeout,
			)

			defer cancel()
			job, err := s.schedule(ctx)
			if err != nil {
				if ErrNewJobsNotFound.Is(err) {
					if s.opts.NotWaitNewJobs {
						continue
					}

					select {
					case <-s.cancel:
						return
					case <-time.After(
						s.opts.WaitNewJobTimeout):
					}
				}

				if ErrJobSource.Is(err) {
					close(s.jobs)
					return
				}

				continue
			}

			select {
			case s.jobs <- job:
				s.opts.Metrics.Discover(job)
			case <-s.cancel:
				return
			}
		}
	}
}
