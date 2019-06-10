package workerpool

import (
	"context"

	"github.com/src-d/gitcollector"
	"gopkg.in/src-d/go-errors.v1"
)

// Worker is in charge of process gitcollector.Jobs.
type Worker struct {
	id     string
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

var (
	errJobsClosed    = errors.NewKind("jobs channel was closed")
	errWorkerStopped = errors.NewKind("worker was stopped")
)

// Start starts the Worker. It shouldn't be restarted after a call to Stop.
func (w *Worker) Start() error {
	var (
		ok  = true
		err error
	)

	for ok {
		ok, err = w.consumeJob()
	}

	return err
}

func (w *Worker) consumeJob() (bool, error) {
	select {
	case <-w.cancel:
		return false, errWorkerStopped.New()
	case job, ok := <-w.jobs:
		if !ok {
			close(w.cancel)
			return false, errJobsClosed.New()
		}

		var done = make(chan struct{})
		go func() {
			defer close(done)
			if err := job.Process(
				context.TODO(),
			); err != nil {
				// the job is in charge to log its own errors
			}
		}()

		select {
		case now := <-w.cancel:
			if !now {
				<-done
			}

			return false, errWorkerStopped.New()
		case <-done:
		}
	}

	return true, nil
}

// Stop stops the Worker.
func (w *Worker) Stop(immediate bool) error {
	// if the jobs channel is found closed then the cancel channel will
	// be also closed. Subsequent calls to the Stop method mustn't panic.
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = errJobsClosed.New()
		}
	}()

	w.cancel <- immediate
	return err
}
