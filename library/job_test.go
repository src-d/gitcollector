package library

import (
	"context"
	"sync"
	"testing"

	"github.com/src-d/gitcollector"
	"gopkg.in/src-d/go-log.v1"

	"github.com/stretchr/testify/require"
)

func TestJobScheduleFn(t *testing.T) {
	download := make(chan gitcollector.Job, 2)
	update := make(chan gitcollector.Job, 20)
	sched := NewJobScheduleFn(
		nil,
		download, update,
		false,
		log.New(nil),
		nil,
	)

	queues := []chan gitcollector.Job{download, update}
	testScheduleFn(t, sched, queues)
}

func TestDownloadJobScheduleFn(t *testing.T) {
	download := make(chan gitcollector.Job, 5)
	sched := NewDownloadJobScheduleFn(
		nil,
		download,
		false,
		log.New(nil),
		nil,
	)

	queues := []chan gitcollector.Job{download}
	testScheduleFn(t, sched, queues)
}

func TestUpdateJobScheduleFn(t *testing.T) {
	update := make(chan gitcollector.Job, 5)
	sched := NewUpdateJobScheduleFn(nil, update, log.New(nil))
	queues := []chan gitcollector.Job{update}
	testScheduleFn(t, sched, queues)
}

func testScheduleFn(
	t *testing.T,
	sched gitcollector.ScheduleFn,
	queues []chan gitcollector.Job,
) {
	var req = require.New(t)

	wp := gitcollector.NewWorkerPool(gitcollector.NewJobScheduler(
		sched,
		&gitcollector.JobSchedulerOpts{
			NotWaitNewJobs: true,
		},
	))

	var (
		endpoints = []string{
			"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		}

		mu        sync.Mutex
		got       []string
		processFn = func(_ context.Context, j *Job) error {
			mu.Lock()
			defer mu.Unlock()

			got = append(got, j.Endpoints[0])
			return nil
		}
	)

	wp.SetWorkers(10)
	wp.Run()

	for _, e := range endpoints {
		for _, queue := range queues {
			queue <- &Job{
				Endpoints: []string{e},
				ProcessFn: processFn,
			}
		}
	}

	var expected []string
	for _, queue := range queues {
		expected = append(expected, endpoints...)
		close(queue)
	}

	wp.Wait()
	req.ElementsMatch(expected, got)
}
