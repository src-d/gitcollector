package discovery

import (
	"context"
	"net/http"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/google/go-github/github"
	"github.com/jpillora/backoff"
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
	WaitNewRepos   bool
	TimeNewRepos   time.Duration
	StopTimeout    time.Duration
	EnqueueTimeout time.Duration
	MaxJobBuffer   int
	AuthToken      string
}

// GHProvider is a gitcollector.Provider implementation. It will retrieve the
// information for all the repositories for the given github organization
// to produce gitcollector.Jobs.
type GHProvider struct {
	iter    *orgReposIter
	queue   chan<- gitcollector.Job
	cancel  chan struct{}
	stopped chan struct{}
	backoff *backoff.Backoff
	opts    *GHProviderOpts
}

var _ gitcollector.Provider = (*GHProvider)(nil)

const (
	stopTimeout    = 10 * time.Second
	enqueueTimeout = 5 * time.Second
	maxJobBuffer   = 100
)

// NewGHProvider builds a new Provider
func NewGHProvider(
	org string,
	queue chan<- gitcollector.Job,
	opts *GHProviderOpts,
) *GHProvider {
	if opts == nil {
		opts = &GHProviderOpts{}
	}

	if opts.StopTimeout <= 0 {
		opts.StopTimeout = stopTimeout
	}

	if opts.EnqueueTimeout <= 0 {
		opts.EnqueueTimeout = enqueueTimeout
	}

	if opts.MaxJobBuffer <= 0 {
		opts.MaxJobBuffer = maxJobBuffer
	}

	return &GHProvider{
		iter:    newOrgReposIter(org, opts),
		queue:   queue,
		cancel:  make(chan struct{}),
		stopped: make(chan struct{}, 1),
		backoff: newBackoff(),
		opts:    opts,
	}
}

func newBackoff() *backoff.Backoff {
	const (
		minDuration = 500 * time.Millisecond
		maxDuration = 5 * time.Second
		factor      = 4
	)

	return &backoff.Backoff{
		Min:    minDuration,
		Max:    maxDuration,
		Factor: factor,
		Jitter: true,
	}
}

// Start implements the gitcollector.Provider interface.
func (p *GHProvider) Start() error {
	defer func() { p.stopped <- struct{}{} }()

	var retryJobs []*library.Job
	for {
		select {
		case <-p.cancel:
			return gitcollector.ErrProviderStopped.New()
		default:
			var (
				job     *library.Job
				retried bool
			)

			if len(retryJobs) > 0 {
				job = retryJobs[0]
				retried = true
			} else {
				repo, retry, err := p.iter.Next()
				if err != nil {
					if ErrNewRepositoriesNotFound.Is(err) &&
						!p.opts.WaitNewRepos {
						return gitcollector.
							ErrProviderStopped.New()
					}

					if retry <= 0 {
						return err
					}

					select {
					case <-time.After(retry):
					case <-p.cancel:
						return gitcollector.
							ErrProviderStopped.New()
					}

					continue
				}

				endpoints, err := getEndpoints(repo)
				if err != nil {
					continue
				}

				job = &library.Job{
					Endpoints: endpoints,
				}
			}

			select {
			case p.queue <- job:
				if retried {
					p.backoff.Reset()
				}
			case <-time.After(p.opts.EnqueueTimeout):
				if !retried &&
					len(retryJobs) < p.opts.MaxJobBuffer {
					retryJobs = append(retryJobs, job)
				}

				select {
				case <-time.After(p.backoff.Duration()):
				case <-p.cancel:
					return gitcollector.
						ErrProviderStopped.New()
				}
			}
		}
	}
}

func getEndpoints(r *github.Repository) ([]string, error) {
	var endpoints []string
	getURLs := []func() string{
		r.GetHTMLURL,
		r.GetGitURL,
		r.GetSSHURL,
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
	case <-p.stopped:
		return nil
	case p.cancel <- struct{}{}:
		return nil
	case <-time.After(p.opts.StopTimeout):
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

func newOrgReposIter(org string, conf *GHProviderOpts) *orgReposIter {
	to := conf.HTTPTimeout
	if to <= 0 {
		to = httpTimeout
	}

	rpp := conf.ResultsPerPage
	if rpp <= 0 || rpp > 100 {
		rpp = resultsPerPage
	}

	wnr := conf.TimeNewRepos
	if wnr <= 0 {
		wnr = waitNewRepos
	}

	return &orgReposIter{
		org:    org,
		client: newGithubClient(conf.AuthToken, to),
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

	bufRepos := repos
	if p.checkpoint > 0 {
		i := p.checkpoint
		if len(repos) < p.checkpoint {
			// return err?
			i = 0
		}

		bufRepos = repos[i:]
	}

	if len(repos) < p.opts.PerPage {
		p.checkpoint = len(repos)
	}

	err = nil
	if res.NextPage == 0 {
		if len(repos) == p.opts.PerPage {
			p.opts.Page++
		}

		err = ErrNewRepositoriesNotFound.New()
	} else {
		p.opts.Page = res.NextPage
	}

	p.repos = bufRepos
	return p.waitNewRepos, err
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
