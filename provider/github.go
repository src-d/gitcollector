package provider

import (
	"context"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/discovery"
	"github.com/src-d/gitcollector/library"

	"github.com/google/go-github/v28/github"
)

// NewGitHubOrg builds a new gitcollector.Provider
// based on a discovery.Github.
func NewGitHubOrg(
	org string,
	excludedRepos []string,
	authToken string,
	queue chan<- gitcollector.Job,
	opts *discovery.GitHubOpts,
) *discovery.GitHub {
	return discovery.NewGitHub(
		AdvertiseGHRepositoriesOnJobQueue(queue),
		discovery.NewGHOrgReposIter(org, excludedRepos, &discovery.GHReposIterOpts{
			AuthToken: authToken,
		}),
		opts,
	)
}

// AdvertiseGHRepositoriesOnJobQueue sends the discovered repositories as a
// gitcollector.Jobs to the given channel. It makes a discovery.GitHub plays
// as a gitcollector.Provider
func AdvertiseGHRepositoriesOnJobQueue(
	queue chan<- gitcollector.Job,
) discovery.AdvertiseGHRepositoriesFn {
	return func(ctx context.Context, repos []*github.Repository) error {
		for _, repo := range repos {
			endpoint, err := discovery.GetGHEndpoint(repo)
			if err != nil {
				return nil
			}

			job := &library.Job{
				Type:      library.JobDownload,
				Endpoints: []string{endpoint},
			}

			select {
			case queue <- job:
			case <-ctx.Done():
				return discovery.ErrAdvertiseTimeout.
					Wrap(ctx.Err())
			}
		}

		return nil
	}
}
