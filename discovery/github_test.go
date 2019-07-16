package discovery

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-github/github"

	"github.com/stretchr/testify/require"
)

func TestGitHub(t *testing.T) {
	var req = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	token, _ := testToken()
	queue := make(chan *github.Repository, 50)
	advertiseRepos := func(
		_ context.Context,
		repos []*github.Repository,
	) error {
		for _, repo := range repos {
			queue <- repo
		}

		return nil
	}

	discovery := NewGitHub(
		advertiseRepos,
		NewGHOrgReposIter(org, &GHReposIterOpts{
			TimeNewRepos:   1 * time.Second,
			ResultsPerPage: 100,
			AuthToken:      token,
		}),
		&GitHubOpts{
			MaxJobBuffer: 50,
		},
	)

	var (
		consumedRepos = make(chan *github.Repository, 200)
		stop          bool
		done          = make(chan struct{})
	)

	go func() {
		defer func() { done <- struct{}{} }()
		for !stop {
			select {
			case repo, ok := <-queue:
				if !ok {
					return
				}

				select {
				case consumedRepos <- repo:
				case <-time.After(timeToStop):
					stop = true
				}
			}
		}
	}()

	err := discovery.Start()
	req.True(ErrDiscoveryStopped.Is(err))

	close(queue)
	<-done
	req.False(stop)
	close(consumedRepos)

	for repo := range consumedRepos {
		ep, err := GetGHEndpoint(repo)
		req.NoError(err)
		req.True(strings.Contains(ep, org))
	}
}

func TestGitHubSkipForks(t *testing.T) {
	var req = require.New(t)
	const org = "src-d"

	token, skip := testToken()
	if skip != nil {
		t.Skip(skip.Error())
	}

	queue := make(chan *github.Repository, 200)
	advertiseRepos := func(
		_ context.Context,
		repos []*github.Repository,
	) error {
		for _, repo := range repos {
			queue <- repo
		}

		return nil
	}

	discovery := NewGitHub(
		advertiseRepos,
		NewGHOrgReposIter(org, &GHReposIterOpts{
			AuthToken: token,
		}),
		&GitHubOpts{
			SkipForks:    true,
			MaxJobBuffer: 50,
		},
	)

	done := make(chan struct{})
	var err error
	go func() {
		err = discovery.Start()
		close(done)
	}()

	<-done
	req.True(ErrNewRepositoriesNotFound.Is(err), err.Error())
	close(queue)
	forkedRepos := []string{"or-tools", "PyHive", "go-oniguruma"}
	for repo := range queue {
		ep, err := GetGHEndpoint(repo)
		req.NoError(err)
		req.True(strings.Contains(ep, org))

		for _, forked := range forkedRepos {
			req.False(strings.Contains(ep, forked))
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
