package gitcollector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	var require = require.New(t)
	require.True(true)

	queue := make(chan Job, 20)
	wp := NewWorkerPool(
		NewJobScheduler(testScheduleFn(queue), &JobSchedulerOpts{}),
		nil,
	)

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
	require.ElementsMatch(ids, got)

	queue = make(chan Job, 20)
	wp.scheduler = NewJobScheduler(
		testScheduleFn(queue),
		&JobSchedulerOpts{},
	)

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

func testScheduleFn(queue chan Job) ScheduleFn {
	return func(*JobSchedulerOpts) (Job, error) {
		select {
		case job, ok := <-queue:
			if !ok {
				return nil, ErrClosedChannel.New()
			}

			return job, nil
		case <-time.After(50 * time.Millisecond):
			return nil, ErrNewJobsNotFound.New()
		}
	}
}
