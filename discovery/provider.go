package discovery

import (
	"context"
	"net/http"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/downloader"
	"github.com/src-d/gitcollector/library"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/google/go-github/github"
	"golang.org/x/oauth2"
)

var (
	// ErrEndpointsNotFound is the returned error when couldn't find
	// endpoints for a certain repository.
	ErrEndpointsNotFound = errors.NewKind("endpoinds not found for %s")

	// ErrNewRepositoriesNotFound is returned when there aren't new
	// repositories in the organization.
	ErrNewRepositoriesNotFound = errors.NewKind(
		"couldn't find new repositories")
)

// GHProviderOpts represents configuration options for a GHProvider.
type GHProviderOpts struct {
	HTTPTimeout    time.Duration
	ResultsPerPage int
	TimeNewRepos   time.Duration
	StopTimeout    time.Duration
}

// GHProvider is a gitcollector.Provider implementation. It will retrieve the
// information for all the repositories for the given github organization
// to produce gitcollector.Jobs.
type GHProvider struct {
	iter   *orgReposIter
	queue  chan<- gitcollector.Job
	cancel chan struct{}
}

var _ gitcollector.Provider = (*GHProvider)(nil)

const stopTimeout = 500 * time.Microsecond

// NewGHProvider builds a new Provider
func NewGHProvider(
	org, token string,
	queue chan<- gitcollector.Job,
	opts *GHProviderOpts,
) *GHProvider {
	if opts == nil {
		opts = &GHProviderOpts{}
	}

	if opts.StopTimeout <= 0 {
		opts.StopTimeout = stopTimeout
	}

	return &GHProvider{
		iter:   newOrgReposIter(org, token, opts),
		queue:  queue,
		cancel: make(chan struct{}),
	}
}

// Start implements the gitcollector.Provider interface.
func (p *GHProvider) Start() error {
	// TODO: Add logging
	for {
		select {
		case <-p.cancel:
			return gitcollector.ErrProviderStopped.New()
		default:
			repo, retry, err := p.iter.Next()
			if err != nil {
				if retry <= 0 {
					return err
				}

				select {
				case <-time.After(retry):
				case <-p.cancel:
					return gitcollector.ErrProviderStopped.New()
				}

				continue
			}

			endpoints, err := getEndpoints(repo)
			if err != nil {
				// TODO: log errors
				continue
			}

			p.queue <- &library.Job{
				Endpoints: endpoints,
				IsFork:    repo.GetFork(),
				ProcessFn: downloader.Download,
			}
		}
	}
}

func getEndpoints(r *github.Repository) ([]string, error) {
	var endpoints []string
	getURLs := []func() string{
		r.GetGitURL,
		r.GetSSHURL,
		r.GetHTMLURL,
	}

	for _, getURL := range getURLs {
		ep := getURL()
		if ep != "" {
			endpoints = append(endpoints, ep)
		}
	}

	if len(endpoints) < 1 {
		return nil, ErrEndpointsNotFound.New(r.GetFullName())
	}

	return endpoints, nil
}

// Stop implements the gitcollector.Provider interface
func (p *GHProvider) Stop() error {
	select {
	case p.cancel <- struct{}{}:
		return nil
	case <-time.After(stopTimeout):
		return gitcollector.ErrProviderStop.New()
	}
}

const (
	httpTimeout    = 30 * time.Second
	resultsPerPage = 100
	waitNewRepos   = 24 * time.Hour
)

type orgReposIter struct {
	org          string
	client       *github.Client
	repos        []*github.Repository
	checkpoint   int
	opts         *github.RepositoryListByOrgOptions
	waitNewRepos time.Duration
}

func newOrgReposIter(org, token string, conf *GHProviderOpts) *orgReposIter {
	to := conf.HTTPTimeout
	if to <= 0 {
		to = httpTimeout
	}

	rpp := conf.ResultsPerPage
	if rpp <= 0 {
		rpp = resultsPerPage
	}

	wnr := conf.TimeNewRepos
	if wnr <= 0 {
		wnr = waitNewRepos
	}

	return &orgReposIter{
		org:    org,
		client: newGithubClient(token, to),
		opts: &github.RepositoryListByOrgOptions{
			ListOptions: github.ListOptions{PerPage: rpp},
		},
		waitNewRepos: wnr,
	}
}

func newGithubClient(token string, timeout time.Duration) *github.Client {
	var client *http.Client
	if token == "" {
		client = &http.Client{}
	} else {
		client = oauth2.NewClient(
			context.Background(),
			oauth2.StaticTokenSource(
				&oauth2.Token{AccessToken: token},
			),
		)
	}

	client.Timeout = timeout
	return github.NewClient(client)
}

func (p *orgReposIter) Next() (*github.Repository, time.Duration, error) {
	if len(p.repos) == 0 {
		if retry, err := p.requestRepos(); err != nil {
			return nil, retry, err
		}
	}

	var next *github.Repository
	next, p.repos = p.repos[0], p.repos[1:]
	return next, 0, nil
}

func (p *orgReposIter) requestRepos() (time.Duration, error) {
	var bufRepos []*github.Repository

	repos, res, err := p.client.Repositories.ListByOrg(
		context.Background(),
		p.org,
		p.opts,
	)

	if err != nil {
		if _, ok := err.(*github.RateLimitError); !ok {
			return -1, err
		}

		return timeToRetry(res), err
	}

	if len(repos) == 0 {
		return p.waitNewRepos, ErrNewRepositoriesNotFound.New()
	}

	repos = repos[p.checkpoint:]
	for _, r := range repos {
		bufRepos = append(bufRepos, r)
	}

	if len(bufRepos) < resultsPerPage {
		p.checkpoint = len(bufRepos)
	} else {
		p.checkpoint = 0
	}

	if res.NextPage == 0 {
		p.opts.Page++
	} else {
		p.opts.Page = res.NextPage
	}

	p.repos = bufRepos
	return -1, nil
}

func timeToRetry(res *github.Response) time.Duration {
	now := time.Now().UTC().Unix()
	resetTime := res.Rate.Reset.UTC().Unix()
	timeToReset := time.Duration(resetTime-now) * time.Second
	remaining := res.Rate.Remaining
	if timeToReset < 0 || timeToReset > 1*time.Hour {
		// If this happens, the system clock is probably wrong, so we
		// assume we are at the beginning of the window and consider
		// only total requests per hour.
		timeToReset = 1 * time.Hour
		remaining = res.Rate.Limit
	}

	return timeToReset / time.Duration(remaining+1)
}
