package provider

import (
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrUpdatesStopped is returned when a provider has been stopped.
	ErrUpdatesStopped = errors.NewKind("provider stopped")

	// ErrUpdatesStop is returned when a provider fails on Stop.
	ErrUpdatesStop = errors.NewKind("provider failed on stop")
)

// UpdatesOpts represents configuration options for an Updates.
type UpdatesOpts struct {
	// TriggerOnce triggers the update just once and exits.
	TriggerOnce bool
	// TriggerInterval is the time interval elapsed between updates.
	TriggerInterval time.Duration
	// EnqueueTimeout is the time a job waits to be enqueued.
	EnqueueTimeout time.Duration
	// StopTimeout is the time the service waits to be stopped after a Stop
	// call is performed.
	StopTimeout time.Duration
}

// Updates is a gitcollector.Provider implementation. It will periodically
// trigger the gitcollector.Jobs production to update the git repositories hold
// in a borges.Library
type Updates struct {
	lib    borges.Library
	queue  chan<- gitcollector.Job
	cancel chan struct{}
	opts   *UpdatesOpts
}

var _ gitcollector.Provider = (*Updates)(nil)

const (
	triggerInterval = 24 * 7 * time.Hour
	stopTimeout     = 500 * time.Microsecond
	enqueueTimeout  = 500 * time.Second
)

// NewUpdates builds a new Updates.
func NewUpdates(
	lib borges.Library,
	queue chan<- gitcollector.Job,
	opts *UpdatesOpts,
) *Updates {
	if opts == nil {
		opts = &UpdatesOpts{}
	}

	if opts.TriggerInterval <= 0 {
		opts.TriggerInterval = triggerInterval
	}

	if opts.StopTimeout <= 0 {
		opts.StopTimeout = stopTimeout
	}

	if opts.EnqueueTimeout <= 0 {
		opts.EnqueueTimeout = enqueueTimeout
	}

	return &Updates{
		lib:    lib,
		queue:  queue,
		cancel: make(chan struct{}),
		opts:   opts,
	}
}

// Start implements the gitcollector.Provider interface.
func (p *Updates) Start() error {
	if err := p.update(); err != nil {
		return err
	}

	if p.opts.TriggerOnce {
		return ErrUpdatesStopped.New()
	}

	for {
		select {
		case <-p.cancel:
			return ErrUpdatesStopped.New()
		case <-time.After(p.opts.TriggerInterval):
			if err := p.update(); err != nil {
				return err
			}
		}
	}
}

var errEnqueueTimeout = errors.NewKind("update queue is full")

func (p *Updates) update() error {
	var done = make(chan error)
	go func() {
		defer close(done)

		iter, err := p.lib.Locations()
		if err != nil {
			done <- err
			return
		}

		iter.ForEach(func(l borges.Location) error {
			job := &library.Job{
				Type:       library.JobUpdate,
				LocationID: l.ID(),
			}

			select {
			case p.queue <- job:
				return nil
			case <-time.After(p.opts.EnqueueTimeout):
				return errEnqueueTimeout.New()
			}
		})
	}()

	select {
	case <-p.cancel:
		return ErrUpdatesStopped.New()
	case err := <-done:
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop implements the gitcollector.Provider interface.
func (p *Updates) Stop() error {
	select {
	case p.cancel <- struct{}{}:
		return nil
	case <-time.After(p.opts.StopTimeout):
		return ErrUpdatesStop.New()
	}
}
