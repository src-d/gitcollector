package provider

import (
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/discovery"
	"github.com/src-d/gitcollector/library"
	"github.com/stretchr/testify/require"
)

func TestGitHub(t *testing.T) {
	var req = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	queue := make(chan gitcollector.Job, 50)
	provider := NewGitHubOrg(
		org,
		[]string{},
		"",
		queue,
		&discovery.GitHubOpts{
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
	req.True(discovery.ErrDiscoveryStopped.Is(err))

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
