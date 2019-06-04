package downloader

import (
	"context"

	"github.com/src-d/gitcollector/library"
)

// Download is a library.JobFn function to download a git repository and store
// it in a borges.Library.
func Download(_ context.Context, _ *library.Job) error {
	// TODO: implement repsitories download
	return nil
}
