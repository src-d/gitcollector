package gitcollector

import (
	"context"

	"gopkg.in/src-d/go-errors.v1"
)

// Worker is in charge of process gitcollector.Jobs.
type Worker struct {
	id      string
	jobs    chan Job
	cancel  chan bool
	metrics MetricsCollector
}

// NewWorker builds a new Worker.
func NewWorker(jobs chan Job, metrics MetricsCollector) *Worker {
	return &Worker{
		jobs:    jobs,
		cancel:  make(chan bool),
		metrics: metrics,
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
				w.metrics.Fail(job)
				return
			}

			w.metrics.Success(job)
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
