package provider

import (
	"sync"
	"testing"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/plain"
	"github.com/src-d/go-borges/util"
	"github.com/stretchr/testify/require"
)

func TestUpdates(t *testing.T) {
	var require = require.New(t)
	require.True(true)

	ids := []borges.LocationID{
		"a", "b", "c", "d", "e", "f",
	}

	lib := &testLib{locIDs: ids[:3]}

	queue := make(chan gitcollector.Job, 30)
	provider := NewUpdates(lib, queue, &UpdatesOpts{
		TriggerOnce:     true,
		TriggerInterval: 500 * time.Microsecond,
	})

	go runProvider(t, provider)

	time.Sleep(100 * time.Millisecond)
	require.Len(lib.locIDs, len(queue))

	provider.opts.TriggerOnce = false
	go runProvider(t, provider)

	time.Sleep(200 * time.Microsecond)
	for _, id := range ids[3:] {
		lib.addLocationID(id)
	}

	time.Sleep(250 * time.Millisecond)
	require.NoError(provider.Stop())

	require.Len(queue, cap(queue))
	for i := 0; i < cap(queue); i++ {
		job := <-queue
		j, ok := job.(*library.Job)
		require.True(ok)
		require.Contains(ids, j.LocationID)
		require.True(j.Type == library.JobUpdate)
	}
}

func runProvider(t *testing.T, provider *Updates) {
	t.Helper()
	require.True(
		t,
		ErrUpdatesStopped.Is(provider.Start()),
	)
}

type testLib struct {
	mu     sync.RWMutex
	locIDs []borges.LocationID
}

var _ borges.Library = (*testLib)(nil)

func (l *testLib) addLocationID(id borges.LocationID) {
	l.mu.Lock()
	l.locIDs = append(l.locIDs, id)
	l.mu.Unlock()
}

func (l *testLib) ID() borges.LibraryID { return "test" }

func (l *testLib) Init(id borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) Get(
	id borges.RepositoryID,
	mode borges.Mode,
) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) GetOrInit(id borges.RepositoryID) (borges.Repository, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) Has(
	id borges.RepositoryID,
) (bool, borges.LibraryID, borges.LocationID, error) {
	return false, "", "", borges.ErrNotImplemented.New()
}

func (l *testLib) Repositories(
	mode borges.Mode,
) (borges.RepositoryIterator, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) Location(id borges.LocationID) (borges.Location, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) Locations() (borges.LocationIterator, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var locs []borges.Location
	for _, id := range l.locIDs {
		loc, err := plain.NewLocation(id, nil, nil)
		if err != nil {
			return nil, err
		}

		locs = append(locs, loc)
	}

	return util.NewLocationIterator(locs), nil
}

func (l *testLib) Library(id borges.LibraryID) (borges.Library, error) {
	return nil, borges.ErrNotImplemented.New()
}

func (l *testLib) Libraries() (borges.LibraryIterator, error) {
	return nil, borges.ErrNotImplemented.New()
}
