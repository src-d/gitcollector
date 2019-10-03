package discovery

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector/testutils"

	"github.com/google/go-github/v28/github"
	"github.com/stretchr/testify/require"
)

const couldNotFindNewRepos = "couldn't find new repositories"

func TestGitHub(t *testing.T) {
	var req = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	token, _ := getToken()
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
		NewGHOrgReposIter(org, []string{}, &GHReposIterOpts{
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

	token, skip := getToken()
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
		NewGHOrgReposIter(org, []string{}, &GHReposIterOpts{
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

func TestExcludeRepos(t *testing.T) {
	var req = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	token, _ := getToken()
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
		NewGHOrgReposIter(org, []string{"gitcollector"}, &GHReposIterOpts{
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
		req.NotEqual("gitcollector", *repo.Name)
	}
}

// TODO request rate error ?

// TestProxyMockUps
// setup https proxy that returns error responses with different status codes
// <expected> check that errors returned correspond to expected ones
func TestProxyMockUps(t *testing.T) {
	for _, tst := range []struct {
		name        string
		code        int
		errContains string
	}{
		{"301", http.StatusMovedPermanently, "Moved Permanently"},
		{"400", http.StatusBadRequest, "Bad Request"},
		{"403", http.StatusForbidden, "Forbidden"},
		{"404", http.StatusNotFound, "Not Found"},
		{"500", http.StatusInternalServerError, "Internal Server Error"},
		{"501", http.StatusNotImplemented, "Not Implemented"},
		{"502", http.StatusBadGateway, "Bad Gateway"},
		{"503", http.StatusServiceUnavailable, "Service Unavailable"},
	} {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			testProxyMockUp(t, tst.code, tst.errContains)
		})
	}
}

func testProxyMockUp(t *testing.T, code int, errContains string) {
	const org = "bblfsh"

	healthyTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = healthyTransport }()

	proxy, err := testutils.NewProxy(
		healthyTransport,
		&testutils.Options{
			Code:    code,
			KeyPath: "../_testdata/server.key",
			PemPath: "../_testdata/server.pem",
		})
	require.NoError(t, err)

	require.NoError(t, proxy.Start())
	defer func() { proxy.Stop() }()

	require.NoError(t, testutils.SetTransportProxy())

	token, _ := getToken()
	queue := make(chan *github.Repository, 50)
	advertiseRepos := func(
		_ context.Context,
		repos []*github.Repository,
	) error {
		time.Sleep(time.Minute)
		for _, repo := range repos {
			queue <- repo
		}

		return nil
	}

	discovery := NewGitHub(
		advertiseRepos,
		NewGHOrgReposIter(org, []string{}, &GHReposIterOpts{
			TimeNewRepos:   1 * time.Second,
			ResultsPerPage: 100,
			AuthToken:      token,
		}),
		&GitHubOpts{
			MaxJobBuffer:     50,
			AdvertiseTimeout: time.Second,
		},
	)

	err = discovery.Start()
	require.Error(t, err)
	require.Contains(t, err.Error(), errContains)
}

type advertiseCase struct {
	name           string
	advErr         error
	delay, timeout time.Duration
	errContains    string
}

// TestAdvertiseErrors checks basic advertise cases:
// 1) advertise function completed successfully
// 2) advertise function has returned an error
// 3) advertise function is being evaluated longer than AdvertiseTimeout
func TestAdvertiseErrors(t *testing.T) {
	for _, tst := range []advertiseCase{
		{"NoAdvertiseError", nil, 0, 0, couldNotFindNewRepos},
		{"AdvertiseError", fmt.Errorf("advertise err"), 0, 0, "advertise err"},
		{"AdvertiseTimeout", nil, 2 * time.Second, time.Second, "context deadline exceeded"},
	} {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			testAdvertise(t, tst)
		})
	}
}

func testAdvertise(t *testing.T, ac advertiseCase) {
	const org = "bblfsh"

	token, _ := getToken()
	queue := make(chan *github.Repository, 50)
	advertiseRepos := func(
		ctx context.Context,
		repos []*github.Repository,
	) error {
		time.Sleep(ac.delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		for _, repo := range repos {
			queue <- repo
		}

		return ac.advErr
	}

	discovery := NewGitHub(
		advertiseRepos,
		NewGHOrgReposIter(org, []string{}, &GHReposIterOpts{
			TimeNewRepos:   1 * time.Second,
			ResultsPerPage: 100,
			AuthToken:      token,
		}),
		&GitHubOpts{
			MaxJobBuffer:     50,
			AdvertiseTimeout: ac.timeout,
		},
	)

	err := discovery.Start()
	require.Error(t, err)
	require.Contains(t, err.Error(), ac.errContains)
}

func getToken() (string, error) {
	token := os.Getenv("GITHUB_TOKEN")
	ci := os.Getenv("TRAVIS")
	var err error
	if token == "" && ci == "true" {
		err = fmt.Errorf("test running on travis CI but " +
			"couldn't find GITHUB_TOKEN")
	}

	return token, err
}
