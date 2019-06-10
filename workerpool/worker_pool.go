package workerpool

import (
	"sync"

	"github.com/src-d/gitcollector"
)

// WorkerPool holds a pool of workers.
type WorkerPool struct {
	scheduler gitcollector.JobScheduler
	workers   []*Worker
	resize    chan struct{}
	wg        sync.WaitGroup
}

// New builds a new WorkerPool.
func New(scheduler gitcollector.JobScheduler) *WorkerPool {
	resize := make(chan struct{}, 1)
	resize <- struct{}{}
	return &WorkerPool{
		scheduler: scheduler,
		resize:    resize,
	}
}

// Run notify workers to start.
func (wp *WorkerPool) Run() {
	go func() { wp.scheduler.Schedule() }()
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
		w := NewWorker(wp.scheduler.Jobs())
		go func() {
			w.Start()
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
			worker.Stop(false)
			wg.Done()
		}()
	}

	wp.workers = wp.workers[:i]
	wg.Wait()
}

// Wait waits for the workers to finish. A worker will finish when the queue to
// retrieve jobs from is closed.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// Close stops all the workers in the pool waiting for the jobs to finish.
func (wp *WorkerPool) Close() {
	wp.SetWorkers(0)
	wp.wg.Wait()
	wp.scheduler.Finish()
}

// Stop stops all the workers in the pool immediately.
func (wp *WorkerPool) Stop() {
	<-wp.resize
	defer func() { wp.resize <- struct{}{} }()

	for _, w := range wp.workers {
		w.Stop(true)
	}

	wp.wg.Wait()
	wp.workers = nil
	wp.scheduler.Finish()
}
