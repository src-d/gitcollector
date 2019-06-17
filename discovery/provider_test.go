package discovery

import (
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"

	"github.com/stretchr/testify/require"
)

func TestGHProvider(t *testing.T) {
	var req = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	queue := make(chan gitcollector.Job, 200)
	provider := NewGHProvider(
		org,
		queue,
		&GHProviderOpts{
			TimeNewRepos:   1 * time.Second,
			ResultsPerPage: 70,
			AuthToken:      "",
		},
	)

	var (
		consumedJobs = make(chan gitcollector.Job, 200)
		stopErr      = make(chan error, 1)
	)

	go func() {
		var stop bool
		for !stop {
			select {
			case job := <-queue:
				select {
				case consumedJobs <- job:
				case <-time.After(timeToStop):
					stop = true
				}
			case <-time.After(timeToStop):
				stop = true
			}
		}

		stopErr <- provider.Stop()
	}()

	err := provider.Start()
	req.True(
		ErrNewRepositoriesNotFound.Is(err) ||
			gitcollector.ErrProviderStopped.Is(err),
	)

	req.NoError(<-stopErr)
	close(consumedJobs)
	for job := range consumedJobs {
		j, ok := job.(*library.Job)
		req.True(ok)
		req.Len(j.Endpoints, 1)
		for _, ep := range j.Endpoints {
			req.True(strings.Contains(ep, org))
		}
	}
}
