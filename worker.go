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
	stopped bool
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
func (w *Worker) Start() {
	if w.stopped {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		if err := w.consumeJob(ctx); err != nil {
			if errJobsClosed.Is(err) || errWorkerStopped.Is(err) {
				close(w.cancel)
			}

			return
		}
	}
}

func (w *Worker) consumeJob(ctx context.Context) error {
	select {
	case <-w.cancel:
		return errWorkerStopped.New()
	case job, ok := <-w.jobs:
		if !ok {
			return errJobsClosed.New()
		}

		var done = make(chan struct{})
		go func() {
			defer close(done)
			if err := job.Process(ctx); err != nil {
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

			return errWorkerStopped.New()
		case <-done:
			return nil
		}
	}
}

// Stop stops the Worker.
func (w *Worker) Stop(immediate bool) {
	if w.stopped {
		return
	}

	w.cancel <- immediate
	w.stopped = true
}
