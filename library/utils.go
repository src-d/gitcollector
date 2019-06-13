package library

import (
	"strings"

	"github.com/src-d/go-borges"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrNotSivaLibrary is returned when a borges.Library is not a
	//  siva.Library
	ErrNotSivaLibrary = errors.NewKind("not siva library found")

	// ErrNotSivaLocation is returned when a borges.Library is no a
	// siva.Location
	ErrNotSivaLocation = errors.NewKind("not siva location found")
)

// NewRepositoryID builds a borges.RepositoryID from the given endpoint.
func NewRepositoryID(endpoint string) (borges.RepositoryID, error) {
	id, err := borges.NewRepositoryID(endpoint)
	if err != nil {
		return "", err
	}

	return borges.RepositoryID(strings.TrimSuffix(id.String(), ".git")), nil
}
