package gitcollector

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	var require = require.New(t)
	require.True(true)

	queue := make(chan Job, 20)
	sched := &testScheduler{
		queue:  queue,
		cancel: make(chan struct{}),
	}

	wp := NewWorkerPool(sched)

	numWorkers := []int{2, 8, 0}
	for _, n := range numWorkers {
		wp.SetWorkers(n)
		require.Equal(wp.Size(), n)
	}

	var (
		ids = []string{
			"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		}

		mu      sync.Mutex
		got     []string
		process = func(id string) error {
			mu.Lock()
			defer mu.Unlock()

			got = append(got, id)
			return nil
		}
	)

	wp.SetWorkers(10)
	wp.Run()

	for _, id := range ids {
		queue <- &testJob{
			id:      id,
			process: process,
		}
	}
	close(queue)

	wp.Wait()
	wp.Close()
	require.ElementsMatch(ids, got)

	queue = make(chan Job, 20)
	sched.queue = queue
	wp.SetWorkers(20)
	wp.Run()

	for range ids {
		queue <- &testJob{}
	}

	wp.Stop()
	require.Len(wp.workers, 0)
}

type testJob struct {
	id      string
	process func(id string) error
}

var _ Job = (*testJob)(nil)

func (j *testJob) Process(_ context.Context) error {
	if j.process == nil {
		return nil
	}

	return j.process(j.id)
}

type testScheduler struct {
	queue  chan Job
	cancel chan struct{}
}

func (s *testScheduler) Jobs() chan Job { return s.queue }
func (s *testScheduler) Schedule()      { s.cancel <- struct{}{} }
func (s *testScheduler) Finish()        {}
