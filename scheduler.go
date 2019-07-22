package gitcollector

import (
	"context"
	"time"

	"github.com/jpillora/backoff"
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
	backoff  *backoff.Backoff
}

const (
	schedCapacity = 1000
	schedTimeout  = 5 * time.Second

	// backoff default configuration
	backoffMinDuration = 250 * time.Millisecond
	backoffMaxDuration = 1024 * time.Second
	backoffFactor      = 2
	backoffJitter      = true
)

func newJobScheduler(
	schedule JobScheduleFn,
	opts *WorkerPoolOpts,
) *jobScheduler {
	if opts.SchedulerCapacity <= 0 {
		opts.SchedulerCapacity = schedCapacity
	}

	if opts.ScheduleJobTimeout <= 0 {
		opts.ScheduleJobTimeout = schedTimeout
	}

	return &jobScheduler{
		jobs:     make(chan Job, opts.SchedulerCapacity),
		schedule: schedule,
		cancel:   make(chan struct{}),
		opts:     opts,
		backoff: &backoff.Backoff{
			Min:    backoffMinDuration,
			Max:    backoffMaxDuration,
			Factor: backoffFactor,
			Jitter: backoffJitter,
		},
	}
}

func (s *jobScheduler) finish() {
	s.cancel <- struct{}{}
}

func (s *jobScheduler) Schedule() {
	s.backoff.Reset()
	for {
		select {
		case <-s.cancel:
			return
		default:
			ctx, cancel := context.WithTimeout(
				context.Background(),
				s.opts.ScheduleJobTimeout,
			)

			defer cancel()
			job, err := s.schedule(ctx)
			if err != nil {
				if ErrNewJobsNotFound.Is(err) {
					if s.opts.NotWaitNewJobs {
						continue
					}

				}

				if ErrJobSource.Is(err) {
					close(s.jobs)
					return
				}

				select {
				case <-s.cancel:
					return
				case <-time.After(s.backoff.Duration()):
				}

				continue
			}

			select {
			case s.jobs <- job:
				s.backoff.Reset()
				s.opts.Metrics.Discover(job)
			case <-s.cancel:
				return
			}
		}
	}
}
