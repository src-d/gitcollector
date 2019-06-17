package gitcollector

import (
	"time"

	"gopkg.in/src-d/go-errors.v1"
)

// ScheduleFn is a function to schedule the next Job.
type ScheduleFn func(*JobSchedulerOpts) (Job, error)

// JobSchedulerOpts are configuration options for a JobScheduler.
type JobSchedulerOpts struct {
	Capacity       int
	NotWaitNewJobs bool
	NewJobTimeout  time.Duration
	JobTimeout     time.Duration
}

const (
	schedCapacity = 1000
	jobTimeout    = 3 * time.Second
	newJobTimeout = 30 * time.Second
)

var (
	// ErrNewJobsNotFound is returned if there's no more jobs to schedule.
	ErrNewJobsNotFound = errors.NewKind(
		"couldn't find new jobs to schedule")

	// ErrClosedChannel is returned if the jobs source is closed.
	ErrClosedChannel = errors.NewKind("channel is closed")
)

// JobScheduler schedules the Jobs to be processed.
type JobScheduler struct {
	jobs     chan Job
	schedule ScheduleFn
	cancel   chan struct{}
	metrics  MetricsCollector
	opts     *JobSchedulerOpts
}

// NewJobScheduler builds a new JobScheduler.
func NewJobScheduler(
	schedule ScheduleFn,
	opts *JobSchedulerOpts,
) *JobScheduler {
	if opts.Capacity <= 0 {
		opts.Capacity = schedCapacity
	}

	if opts.JobTimeout <= 0 {
		opts.JobTimeout = jobTimeout
	}

	if opts.NewJobTimeout <= 0 {
		opts.NewJobTimeout = newJobTimeout
	}

	return &JobScheduler{
		jobs:     make(chan Job, opts.Capacity),
		schedule: schedule,
		cancel:   make(chan struct{}),
		opts:     opts,
	}
}

// Jobs returns the channel where the JobScheduler will schedule the Jobs.
func (s *JobScheduler) Jobs() chan Job {
	return s.jobs
}

// Finish finishes to schedule Jobs.
func (s *JobScheduler) Finish() {
	s.cancel <- struct{}{}
}

// Schedule schedules Jobs.
func (s *JobScheduler) Schedule() {
	for {
		select {
		case <-s.cancel:
			return
		default:
			job, err := s.schedule(s.opts)
			if err != nil {
				if ErrNewJobsNotFound.Is(err) {
					if s.opts.NotWaitNewJobs {
						continue
					}

					select {
					case <-s.cancel:
						return
					case <-time.After(s.opts.NewJobTimeout):
					}
				}

				if ErrClosedChannel.Is(err) {
					close(s.jobs)
					return
				}

				continue
			}

			select {
			case s.jobs <- job:
				s.metrics.Discover(job)
			case <-s.cancel:
				return
			}
		}
	}
}
