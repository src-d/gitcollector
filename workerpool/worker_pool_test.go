package workerpool

import (
	"context"
	"sync"
	"testing"

	"github.com/src-d/gitcollector"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	var require = require.New(t)
	require.True(true)

	queue := make(chan gitcollector.Job, 20)
	wp := New(
		&testScheduler{
			queue:  queue,
			cancel: make(chan struct{}),
		},
	)

	numWorkers := []int{2, 8, 0}
	for _, n := range numWorkers {
		wp.SetWorkers(n)
		require.Len(wp.workers, n)
	}

	var (
		ids = []string{
			"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		}

		mu      sync.Mutex
		wg      sync.WaitGroup
		got     []string
		process = func(id string) error {
			mu.Lock()
			defer func() {
				wg.Done()
				mu.Unlock()
			}()

			got = append(got, id)
			return nil
		}
	)

	wp.SetWorkers(10)
	wp.Run()

	wg.Add(len(ids))
	for _, id := range ids {
		queue <- &testJob{
			id:      id,
			process: process,
		}
	}

	wg.Wait()
	wp.Close()
	require.ElementsMatch(ids, got)

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

var _ gitcollector.Job = (*testJob)(nil)

func (j *testJob) Process(_ context.Context) error {
	if j.process == nil {
		return nil
	}

	return j.process(j.id)
}

type testScheduler struct {
	queue  chan gitcollector.Job
	cancel chan struct{}
}

func (s *testScheduler) Jobs() chan gitcollector.Job { return s.queue }
func (s *testScheduler) Schedule()                   { s.cancel <- struct{}{} }
func (s *testScheduler) Finish()                     {}
