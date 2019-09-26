package discovery

import (
	"context"
	"time"

	"github.com/google/go-github/v28/github"
	"github.com/jpillora/backoff"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrEndpointsNotFound is the returned error when couldn't find
	// endpoints for a certain repository.
	ErrEndpointsNotFound = errors.NewKind("endpoinds not found for %s")

	// ErrNewRepositoriesNotFound is returned when there aren't new
	// repositories in the organization.
	ErrNewRepositoriesNotFound = errors.NewKind(
		"couldn't find new repositories")

	// ErrRateLimitExceeded is returned when the api rate limit is reached.
	ErrRateLimitExceeded = errors.NewKind("rate limit requests exceeded")

	// ErrDiscoveryStopped is returned when a discovery has been stopped.
	ErrDiscoveryStopped = errors.NewKind("discovery stopped")

	// ErrDiscoveryStop is returned when a discovery fails on Stop.
	ErrDiscoveryStop = errors.NewKind("discovery failed on stop")

	// ErrAdvertiseTimeout is returned when an advertise functions exceeds
	// the timeout.
	ErrAdvertiseTimeout = errors.NewKind("advertise repositories timeout")
)

// AdvertiseGHRepositoriesFn is used by a GitHub to notify that a new
// repository has been discovered.
type AdvertiseGHRepositoriesFn func(context.Context, []*github.Repository) error

// GitHubOpts represents configuration options for a GitHub discovery.
type GitHubOpts struct {
	AdvertiseTimeout time.Duration
	SkipForks        bool
	WaitNewRepos     bool
	WaitOnRateLimit  bool
	StopTimeout      time.Duration
	MaxJobBuffer     int
	BatchSize        int
}

// GitHub will retrieve the information for all the repositories for the
// given GHRepositoriesIterator.
type GitHub struct {
	advertiseRepos AdvertiseGHRepositoriesFn
	iter           GHRepositoriesIter
	batch          []*github.Repository
	retryJobs      []*github.Repository
	cancel         chan struct{}
	backoff        *backoff.Backoff
	opts           *GitHubOpts
}

const (
	stopTimeout = 10 * time.Second
	batchSize   = 1
)

// NewGitHub builds a new GitHub.
func NewGitHub(
	advertiseRepos AdvertiseGHRepositoriesFn,
	iter GHRepositoriesIter,
	opts *GitHubOpts,
) *GitHub {
	if opts == nil {
		opts = &GitHubOpts{}
	}

	if opts.StopTimeout <= 0 {
		opts.StopTimeout = stopTimeout
	}

	if opts.BatchSize <= 0 {
		opts.BatchSize = batchSize
	}

	if opts.MaxJobBuffer <= 0 {
		opts.MaxJobBuffer = opts.BatchSize * 2
	}

	if opts.AdvertiseTimeout <= 0 {
		to := time.Duration(5*opts.BatchSize) * time.Second
		opts.AdvertiseTimeout = to
	}

	if advertiseRepos == nil {
		advertiseRepos = func(
			context.Context,
			[]*github.Repository,
		) error {
			return nil
		}
	}

	return &GitHub{
		advertiseRepos: advertiseRepos,
		iter:           iter,
		batch:          make([]*github.Repository, 0, opts.BatchSize),
		retryJobs:      make([]*github.Repository, 0, opts.MaxJobBuffer),
		cancel:         make(chan struct{}),
		backoff:        newBackoff(),
		opts:           opts,
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

// Start starts the GitHub.
func (p *GitHub) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		var err error
		defer func() {
			if ErrDiscoveryStopped.Is(err) && len(p.batch) > 0 {
				if de := p.sendBatch(ctx); err != nil {
					err = de
				}
			}
		}()

		done := make(chan struct{})
		go func() {
			err = p.discoverRepositories(ctx)
			close(done)
		}()

		select {
		case <-done:
			if err != nil {
				return err
			}
		case <-p.cancel:
			return ErrDiscoveryStopped.New()
		}
	}
}

func (p *GitHub) discoverRepositories(ctx context.Context) error {
	if len(p.retryJobs) > 0 {
		job := p.retryJobs[0]
		p.retryJobs = p.retryJobs[1:]
		p.batch = append(p.batch, job)
	} else {
		repo, retry, err := p.iter.Next(ctx)
		if err != nil {
			if ErrNewRepositoriesNotFound.Is(err) &&
				!p.opts.WaitNewRepos {
				return ErrDiscoveryStopped.Wrap(err)
			}

			if ErrRateLimitExceeded.Is(err) &&
				!p.opts.WaitOnRateLimit {
				return ErrDiscoveryStopped.Wrap(err)
			}

			if retry <= 0 {
				return err
			}

			time.Sleep(retry)
			return nil
		}

		if p.opts.SkipForks && repo.GetFork() {
			return nil
		}

		p.batch = append(p.batch, repo)
	}

	if len(p.batch) < p.opts.BatchSize {
		return nil
	}

	ctxto, cancel := context.WithTimeout(ctx, p.opts.AdvertiseTimeout)
	defer cancel()

	if err := p.sendBatch(ctxto); err != nil {
		if !ErrAdvertiseTimeout.Is(err) {
			return err
		}

		time.Sleep(p.backoff.Duration())
	} else {
		p.backoff.Reset()
	}

	return nil
}

func (p *GitHub) sendBatch(ctx context.Context) error {
	if err := p.advertiseRepos(ctx, p.batch); err != nil {
		return err
	}

	p.batch = make([]*github.Repository, 0, p.opts.BatchSize)
	return nil
}

// GetGHEndpoint gets the enpoint for a github repository.
func GetGHEndpoint(r *github.Repository) (string, error) {
	var endpoint string
	getURLs := []func() string{
		r.GetHTMLURL,
		r.GetGitURL,
		r.GetSSHURL,
	}

	for _, getURL := range getURLs {
		ep := getURL()
		if ep != "" {
			endpoint = ep
			break
		}
	}

	if endpoint == "" {
		return "", ErrEndpointsNotFound.New(r.GetFullName())
	}

	return endpoint, nil
}

// Stop stops the GitHub.
func (p *GitHub) Stop() error {
	select {
	case p.cancel <- struct{}{}:
		return nil
	case <-time.After(p.opts.StopTimeout):
		return ErrDiscoveryStop.New()
	}
}
