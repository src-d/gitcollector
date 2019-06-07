package updater

import (
	"context"

	"github.com/src-d/gitcollector/library"
)

// Update is a library.JobFn function to update a git repository alreayd stored
// in a borges.Library.
func Update(_ context.Context, _ *library.Job) error {
	// TODO: implement repositories update
	return nil
}
