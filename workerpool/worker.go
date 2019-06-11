package workerpool

import (
	"context"

	"github.com/src-d/gitcollector"
)

// Worker is in charge of process a Job.
type Worker struct {
	jobs   chan gitcollector.Job
	cancel chan bool
}

// NewWorker builds a new Worker.
func NewWorker(jobs chan gitcollector.Job) *Worker {
	return &Worker{
		jobs:   jobs,
		cancel: make(chan bool),
	}
}

// Start starts the Worker.
func (w *Worker) Start() {
	// TODO: Add logging.
	for w.consumeJob() {
	}
}

func (w *Worker) consumeJob() bool {
	select {
	case <-w.cancel:
		return false
	case job, ok := <-w.jobs:
		if !ok {
			return false
		}

		var done = make(chan struct{})
		go func() {
			defer close(done)
			if err := job.Process(context.TODO()); err != nil {
				// TODO: log errors
			}
		}()

		select {
		case now := <-w.cancel:
			if !now {
				<-done
			}

			return false
		case <-done:
		}
	}

	return true
}

// Stop stops the Worker.
func (w *Worker) Stop(immediate bool) {
	w.cancel <- immediate
}
