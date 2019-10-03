package discovery

import (
	"context"
	"net/http"
	"time"

	"github.com/google/go-github/v28/github"
	"golang.org/x/oauth2"
)

// GHRepositoriesIter represents an iterator of *github.Repositories
type GHRepositoriesIter interface {
	Next(context.Context) (*github.Repository, time.Duration, error)
}

// GHReposIterOpts represents configuration options for a GHReposIter.
type GHReposIterOpts struct {
	HTTPTimeout    time.Duration
	ResultsPerPage int
	TimeNewRepos   time.Duration
	AuthToken      string
}

const (
	httpTimeout    = 30 * time.Second
	resultsPerPage = 100
	waitNewRepos   = 24 * time.Hour
)

// GHOrgReposIter is a GHRepositoriesIter by organization name.
type GHOrgReposIter struct {
	org           string
	excludedRepos map[string]struct{}
	client        *github.Client
	repos         []*github.Repository
	checkpoint    int
	opts          *github.RepositoryListByOrgOptions
	waitNewRepos  time.Duration
}

var _ GHRepositoriesIter = (*GHOrgReposIter)(nil)

// NewGHOrgReposIter builds a new GHOrgReposIter.
func NewGHOrgReposIter(org string, excludedRepos []string, opts *GHReposIterOpts) *GHOrgReposIter {
	if opts == nil {
		opts = &GHReposIterOpts{}
	}

	to := opts.HTTPTimeout
	if to <= 0 {
		to = httpTimeout
	}

	rpp := opts.ResultsPerPage
	if rpp <= 0 || rpp > 100 {
		rpp = resultsPerPage
	}

	wnr := opts.TimeNewRepos
	if wnr <= 0 {
		wnr = waitNewRepos
	}

	excludedReposSet := make(map[string]struct{})
	for _, excludedRepo := range excludedRepos {
		excludedReposSet[excludedRepo] = struct{}{}
	}

	return &GHOrgReposIter{
		org:           org,
		excludedRepos: excludedReposSet,
		client:        newGithubClient(opts.AuthToken, to),
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

// Next implements the GHRepositoriesIter interface.
func (p *GHOrgReposIter) Next(
	ctx context.Context,
) (*github.Repository, time.Duration, error) {
	for {
		if len(p.repos) == 0 {
			retry, err := p.requestRepos(ctx)
			if err != nil && len(p.repos) == 0 {
				return nil, retry, err
			}
		}

		var next *github.Repository
		next, p.repos = p.repos[0], p.repos[1:]
		if _, ok := p.excludedRepos[next.GetName()]; !ok {
			return next, 0, nil
		}
	}
}

func (p *GHOrgReposIter) requestRepos(
	ctx context.Context,
) (time.Duration, error) {
	repos, res, err := p.client.Repositories.ListByOrg(
		ctx,
		p.org,
		p.opts,
	)

	if err != nil {
		if _, ok := err.(*github.RateLimitError); !ok {
			return -1, err
		}

		return timeToRetry(res), ErrRateLimitExceeded.Wrap(err)
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
