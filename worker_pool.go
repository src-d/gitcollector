package gitcollector

import (
	"sync"
	"time"
)

// WorkerPoolOpts are configuration options for a JobScheduler.
type WorkerPoolOpts struct {
	SchedulerCapacity  int
	ScheduleJobTimeout time.Duration
	NotWaitNewJobs     bool
	Metrics            MetricsCollector
}

// WorkerPool holds a pool of workers to process Jobs.
type WorkerPool struct {
	scheduler *jobScheduler
	workers   []*worker
	resize    chan struct{}
	wg        sync.WaitGroup
	opts      *WorkerPoolOpts
}

// NewWorkerPool builds a new WorkerPool.
func NewWorkerPool(
	schedule JobScheduleFn,
	opts *WorkerPoolOpts,
) *WorkerPool {
	resize := make(chan struct{}, 1)
	resize <- struct{}{}
	if opts.Metrics == nil {
		opts.Metrics = &hollowMetricsCollector{}
	}

	return &WorkerPool{
		scheduler: newJobScheduler(schedule, opts),
		resize:    resize,
		opts:      opts,
	}
}

// Run notify workers to start.
func (wp *WorkerPool) Run() {
	go wp.opts.Metrics.Start()
	go wp.scheduler.Schedule()
}

// Size returns the current number of workers in the pool.
func (wp *WorkerPool) Size() int {
	<-wp.resize
	defer func() { wp.resize <- struct{}{} }()

	return len(wp.workers)
}

// SetWorkers set the number of Workers in the pool to n.
func (wp *WorkerPool) SetWorkers(n int) {
	<-wp.resize
	defer func() { wp.resize <- struct{}{} }()

	if n < 0 {
		n = 0
	}

	diff := n - len(wp.workers)
	if diff == 0 {
		return
	} else if diff > 0 {
		wp.add(diff)
	} else {
		wp.remove(-diff)
	}
}

func (wp *WorkerPool) add(n int) {
	wp.wg.Add(n)
	for i := 0; i < n; i++ {
		w := newWorker(wp.scheduler.jobs, wp.opts.Metrics)
		go func() {
			w.start()
			wp.wg.Done()
		}()

		wp.workers = append(wp.workers, w)
	}
}

func (wp *WorkerPool) remove(n int) {
	var (
		i             = len(wp.workers) - n
		workersToStop = wp.workers[i:]
		wg            sync.WaitGroup
	)

	wg.Add(len(workersToStop))
	for _, w := range workersToStop {
		worker := w
		go func() {
			worker.stop(false)
			wg.Done()
		}()
	}

	wp.workers = wp.workers[:i]
	wg.Wait()
}

// Wait waits for the workers to finish.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
	wp.workers = nil
	wp.opts.Metrics.Stop(false)
}

// Close stops all the workers in the pool waiting for the jobs to finish.
func (wp *WorkerPool) Close() {
	wp.SetWorkers(0)
	wp.wg.Wait()
	wp.scheduler.finish()
	wp.opts.Metrics.Stop(false)
}

// Stop stops all the workers in the pool immediately.
func (wp *WorkerPool) Stop() {
	<-wp.resize
	defer func() { wp.resize <- struct{}{} }()

	for _, w := range wp.workers {
		w.stop(true)
	}

	wp.wg.Wait()
	wp.workers = nil
	wp.scheduler.finish()
	wp.opts.Metrics.Stop(true)
}

type hollowMetricsCollector struct{}

var _ MetricsCollector = (*hollowMetricsCollector)(nil)

func (mc *hollowMetricsCollector) Start()       {}
func (mc *hollowMetricsCollector) Stop(bool)    {}
func (mc *hollowMetricsCollector) Success(Job)  {}
func (mc *hollowMetricsCollector) Fail(Job)     {}
func (mc *hollowMetricsCollector) Discover(Job) {}
