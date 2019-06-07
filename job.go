package gitcollector

import (
	"context"

	"gopkg.in/src-d/go-errors.v1"
)

// Job represents a gitcollector task.
type Job interface {
	// Process perform the necessary work on the job.
	Process(context.Context) error
}

// JobScheduler schedule the Jobs to be processed.
type JobScheduler interface {
	// Jobs returns a channel where the Jobs will be scheduled.
	Jobs() chan Job
	// Schedule start to schedule Jobs.
	Schedule()
	// Finish stop to schedule Jobs.
	Finish()
}

var (
	// ErrProviderStopped is returned when a provider has been stopped.
	ErrProviderStopped = errors.NewKind("provider stopped")

	// ErrProviderStop is returned when a provider fails on Stop.
	ErrProviderStop = errors.NewKind("provider failed on stop")
)

// Provider interface represents a service to generate new Jobs.
type Provider interface {
	Start() error
	Stop() error
}
