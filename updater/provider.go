package updater

import (
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-errors.v1"
)

// UpdatesProviderOpts represents configuration options for an UpdatesProvider.
type UpdatesProviderOpts struct {
	TriggerOnce     bool
	TriggerInterval time.Duration
	EnqueueTimeout  time.Duration
	StopTimeout     time.Duration
}

// UpdatesProvider is gitcollector.Provider implementation. It will periodically
// trigger the gitcollector.Jobs production to update the git repositories hold
// in a borges.Library
type UpdatesProvider struct {
	lib    borges.Library
	queue  chan<- gitcollector.Job
	cancel chan struct{}
	opts   *UpdatesProviderOpts
}

var _ gitcollector.Provider = (*UpdatesProvider)(nil)

const (
	triggerInterval = 24 * 7 * time.Hour
	stopTimeout     = 500 * time.Microsecond
	enqueueTimeout  = 500 * time.Second
)

// NewUpdatesProvider builds a new UpdatesProviders.
func NewUpdatesProvider(
	lib borges.Library,
	queue chan<- gitcollector.Job,
	opts *UpdatesProviderOpts,
) *UpdatesProvider {
	if opts == nil {
		opts = &UpdatesProviderOpts{}
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

	return &UpdatesProvider{
		lib:    lib,
		queue:  queue,
		cancel: make(chan struct{}),
		opts:   opts,
	}
}

// Start implements the gitcollector.Provider interface.
func (p *UpdatesProvider) Start() error {
	// TODO: Add logging
	if err := p.update(); err != nil {
		return err
	}

	if p.opts.TriggerOnce {
		return gitcollector.ErrProviderStopped.New()
	}

	for {
		select {
		case <-p.cancel:
			return gitcollector.ErrProviderStopped.New()
		case <-time.After(p.opts.TriggerInterval):
			if err := p.update(); err != nil {
				return err
			}
		}
	}
}

var errEnqueueTimeout = errors.NewKind("update queue is full")

func (p *UpdatesProvider) update() error {
	var done = make(chan error)
	go func() {
		defer close(done)

		iter, err := p.lib.Locations()
		if err != nil {
			done <- err
			// TODO: log errors
			return
		}

		err = iter.ForEach(func(l borges.Location) error {
			job := &library.Job{
				LocationID: l.ID(),
				ProcessFn:  Update,
			}

			select {
			case p.queue <- job:
				return nil
			case <-time.After(p.opts.EnqueueTimeout):
				return errEnqueueTimeout.New()
			}
		})

		if err != nil {
			// TODO: log errors
		}
	}()

	select {
	case <-p.cancel:
		return gitcollector.ErrProviderStopped.New()
	case err := <-done:
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop implements the gitcollector.Provider interface.
func (p *UpdatesProvider) Stop() error {
	select {
	case p.cancel <- struct{}{}:
		return nil
	case <-time.After(p.opts.StopTimeout):
		return gitcollector.ErrProviderStop.New()
	}
}
