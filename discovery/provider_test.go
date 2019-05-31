package discovery

import (
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector"

	"github.com/stretchr/testify/require"
)

func TestGHProvider(t *testing.T) {
	var require = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	queue := make(chan *gitcollector.Job, 10)
	provider := NewGHProvider(
		org,
		"", //token
		queue,
		&GHProviderOpts{TimeNewRepos: 2 * time.Second},
	)

	require.NoError(provider.Stop())
	require.True(ErrProviderStop.Is(provider.Stop()))
	require.True(ErrProviderStop.Is(provider.Start()))

	go func() {
	Loop:
		for {
			select {
			case job := <-queue:
				require.Len(job.Endpoints, 3)
				for _, ep := range job.Endpoints {
					require.True(strings.Contains(ep, org))
				}
			case <-time.After(timeToStop):
				require.NoError(provider.Stop())
				break Loop
			}
		}
	}()

	require.True(ErrProviderStop.Is(provider.Start()))
}
