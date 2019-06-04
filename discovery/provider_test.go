package discovery

import (
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"

	"github.com/stretchr/testify/require"
)

func TestGHProvider(t *testing.T) {
	var require = require.New(t)

	const (
		org        = "src-d"
		timeToStop = 5 * time.Second
	)

	queue := make(chan gitcollector.Job, 10)
	provider := NewGHProvider(
		org,
		"", //token
		queue,
		&GHProviderOpts{TimeNewRepos: 2 * time.Second},
	)

	go func() {
		for {
			select {
			case job := <-queue:
				j, ok := job.(*library.Job)
				require.True(ok)
				require.Len(j.Endpoints, 3)
				for _, ep := range j.Endpoints {
					require.True(strings.Contains(ep, org))
				}
			case <-time.After(timeToStop):
				require.NoError(provider.Stop())
				return
			}
		}
	}()

	require.True(gitcollector.ErrProviderStopped.Is(provider.Start()))
}
