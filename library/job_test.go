package library

import (
	"context"
	"sync"
	"testing"

	"github.com/src-d/gitcollector"
	"gopkg.in/src-d/go-log.v1"

	"github.com/stretchr/testify/require"
)

func TestJobAndJobScheduler(t *testing.T) {
	var require = require.New(t)
	require.True(true)

	download := make(chan gitcollector.Job, 20)
	update := make(chan gitcollector.Job, 20)
	wp := gitcollector.NewWorkerPool(gitcollector.NewJobScheduler(
		NewJobScheduleFn(
			nil,
			download, update,
			false,
			log.New(nil),
			nil,
		),
		&gitcollector.JobSchedulerOpts{},
	))

	var (
		endpoints = []string{
			"a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
		}

		mu        sync.Mutex
		wg        sync.WaitGroup
		got       []string
		processFn = func(_ context.Context, j *Job) error {
			mu.Lock()
			defer func() {
				wg.Done()
				mu.Unlock()
			}()

			got = append(got, j.Endpoints[0])
			return nil
		}
	)

	wp.SetWorkers(10)
	wp.Run()

	wg.Add(len(endpoints))
	for _, e := range endpoints {
		download <- &Job{
			Endpoints: []string{e},
			ProcessFn: processFn,
		}
	}

	wg.Wait()
	wp.Close()
	require.ElementsMatch(endpoints, got)
}
