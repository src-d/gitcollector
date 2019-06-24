package library

import (
	"context"
	"sync"
	"testing"

	"github.com/src-d/gitcollector"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-log.v1"
)

func TestJobScheduleFn(t *testing.T) {
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

	download := make(chan gitcollector.Job, 2)
	update := make(chan gitcollector.Job, 20)
	sched := NewJobScheduleFn(
		nil,
		download, update,
		processFn, processFn,
		false,
		nil,
		log.New(nil),
		nil,
	)

	queues := []chan gitcollector.Job{download, update}
	expected := testScheduleFn(sched, endpoints, queues)
	require.ElementsMatch(t, expected, got)
}

func TestDownloadJobScheduleFn(t *testing.T) {
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

	download := make(chan gitcollector.Job, 5)
	sched := NewDownloadJobScheduleFn(
		nil,
		download,
		processFn,
		false,
		nil,
		log.New(nil),
		nil,
	)

	queues := []chan gitcollector.Job{download}
	expected := testScheduleFn(sched, endpoints, queues)
	require.ElementsMatch(t, expected, got)
}

func TestUpdateJobScheduleFn(t *testing.T) {
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

	update := make(chan gitcollector.Job, 5)
	sched := NewUpdateJobScheduleFn(
		nil, update, processFn, nil, log.New(nil),
	)
	queues := []chan gitcollector.Job{update}
	expected := testScheduleFn(sched, endpoints, queues)
	require.ElementsMatch(t, expected, got)
}

func testScheduleFn(
	sched gitcollector.JobScheduleFn,
	endpoints []string,
	queues []chan gitcollector.Job,
) []string {
	wp := gitcollector.NewWorkerPool(
		sched,
		&gitcollector.WorkerPoolOpts{
			NotWaitNewJobs: true,
		},
	)

	wp.SetWorkers(10)
	wp.Run()

	for _, e := range endpoints {
		for i, queue := range queues {
			var t JobType = JobDownload
			if i != 0 {
				t = JobUpdate
			}

			queue <- &Job{
				Type:      t,
				Endpoints: []string{e},
			}
		}
	}

	var expected []string
	for _, queue := range queues {
		expected = append(expected, endpoints...)
		close(queue)
	}

	wp.Wait()
	return expected
}
