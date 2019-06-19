package discovery

import (
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/google/go-github/github"
	"github.com/jpillora/backoff"
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
)

// GHRepositoriesIter represents an iterator of *github.Repositories
type GHRepositoriesIter interface {
	Next() (*github.Repository, time.Duration, error)
}

// GHProviderOpts represents configuration options for a GHProvider.
type GHProviderOpts struct {
	WaitNewRepos    bool
	WaitOnRateLimit bool
	StopTimeout     time.Duration
	EnqueueTimeout  time.Duration
	MaxJobBuffer    int
}

// GHProvider is a gitcollector.Provider implementation. It will retrieve the
// information for all the repositories for the given github organization
// to produce gitcollector.Jobs.
type GHProvider struct {
	iter    GHRepositoriesIter
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
)

// NewGHProvider builds a new Provider
func NewGHProvider(
	queue chan<- gitcollector.Job,
	iter GHRepositoriesIter,
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
		opts.MaxJobBuffer = cap(queue) * 2
	}

	return &GHProvider{
		iter:    iter,
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
				retryJobs = retryJobs[1:]
				retried = true
			} else {
				repo, retry, err := p.iter.Next()
				if err != nil {
					if ErrNewRepositoriesNotFound.Is(err) &&
						!p.opts.WaitNewRepos {
						return gitcollector.
							ErrProviderStopped.
							Wrap(err)
					}

					if ErrRateLimitExceeded.Is(err) &&
						!p.opts.WaitOnRateLimit {
						return gitcollector.
							ErrProviderStopped.
							Wrap(err)
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

				endpoint, err := getEndpoint(repo)
				if err != nil {
					continue
				}

				job = &library.Job{
					Type:      library.JobDownload,
					Endpoints: []string{endpoint},
				}
			}

			select {
			case p.queue <- job:
				if retried {
					p.backoff.Reset()
				}
			case <-time.After(p.opts.EnqueueTimeout):
				if len(retryJobs) < p.opts.MaxJobBuffer {
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

func getEndpoint(r *github.Repository) (string, error) {
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
