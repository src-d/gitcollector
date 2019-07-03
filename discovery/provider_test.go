package discovery

import (
	"fmt"
	"os"
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

	token, _ := testToken()
	queue := make(chan gitcollector.Job, 50)
	provider := NewGHProvider(
		queue,
		NewGHOrgReposIter(org, &GHReposIterOpts{
			TimeNewRepos:   1 * time.Second,
			ResultsPerPage: 100,
			AuthToken:      token,
		}),
		&GHProviderOpts{
			MaxJobBuffer: 50,
		},
	)

	var (
		consumedJobs = make(chan gitcollector.Job, 200)
		stop         bool
		done         = make(chan struct{})
	)

	go func() {
		defer func() { done <- struct{}{} }()
		for !stop {
			select {
			case job, ok := <-queue:
				if !ok {
					return
				}

				select {
				case consumedJobs <- job:
				case <-time.After(timeToStop):
					stop = true
				}
			}
		}
	}()

	err := provider.Start()
	req.True(gitcollector.ErrProviderStopped.Is(err))

	close(queue)
	<-done
	req.False(stop)
	close(consumedJobs)

	for j := range consumedJobs {
		job, ok := j.(*library.Job)
		req.True(ok)
		req.True(job.Type == library.JobDownload)
		req.Len(job.Endpoints, 1)
		req.True(strings.Contains(job.Endpoints[0], org))
	}
}

func TestGHProviderSkipForks(t *testing.T) {
	var req = require.New(t)
	const org = "src-d"

	token, skip := testToken()
	if skip != nil {
		t.Skip(skip.Error())
	}

	queue := make(chan gitcollector.Job, 200)
	provider := NewGHProvider(
		queue,
		NewGHOrgReposIter(org, &GHReposIterOpts{
			AuthToken: token,
		}),
		&GHProviderOpts{
			SkipForks:    true,
			MaxJobBuffer: 50,
		},
	)

	done := make(chan struct{})
	var err error
	go func() {
		err = provider.Start()
		close(done)
	}()

	<-done
	req.True(ErrNewRepositoriesNotFound.Is(err), err.Error())
	close(queue)
	forkedRepos := []string{"or-tools", "PyHive", "go-oniguruma"}
	for job := range queue {
		j, ok := job.(*library.Job)
		req.True(ok)
		req.Len(j.Endpoints, 1)

		for _, forked := range forkedRepos {
			req.False(strings.Contains(j.Endpoints[0], forked))
		}
	}
}

func testToken() (string, error) {
	token := os.Getenv("GITHUB_TOKEN")
	ci := os.Getenv("TRAVIS")
	var err error
	if token == "" && ci == "true" {
		err = fmt.Errorf("test running on travis CI but " +
			"couldn't find GITHUB_TOKEN")
	}

	return token, err
}
