package downloader

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-log.v1"

	"github.com/stretchr/testify/require"
)

type test struct {
	locID   borges.LocationID
	repoIDs []borges.RepositoryID
}

func TestDownload(t *testing.T) {
	var req = require.New(t)

	dir, err := ioutil.TempDir("", "gitcollector")
	req.NoError(err)
	defer os.RemoveAll(dir)

	sivaPath := filepath.Join(dir, "siva")
	req.NoError(os.Mkdir(sivaPath, 0775))
	fs := osfs.New(sivaPath)

	downloaderPath := filepath.Join(dir, "downlader")
	req.NoError(os.Mkdir(downloaderPath, 0775))
	temp := osfs.New(downloaderPath)

	lib, err := siva.NewLibrary("test", fs, siva.LibraryOptions{
		Bucket:        2,
		Transactional: true,
	})
	req.NoError(err)

	tests := []*test{
		&test{
			locID: borges.LocationID("a6c64c655d15afda789f8138b83213782b6f77c7"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/prakhar1989/awesome-courses"),
				borges.RepositoryID("github.com/Leo-xxx/awesome-courses"),
				borges.RepositoryID("github.com/manjunath00/awesome-courses"),
			},
		},
		&test{
			locID: borges.LocationID("fe83b066a45d859cd40cbf512c4ec20351c4f9d9"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/MunGell/awesome-for-beginners"),
				borges.RepositoryID("github.com/dhruvil1514/awesome-for-beginners"),
				borges.RepositoryID("github.com/karellism/awesome-for-beginners"),
			},
		},
		&test{
			locID: borges.LocationID("1880dc904e1b2774be9c97a7b85efabdb910f974"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/jtleek/datasharing"),
				borges.RepositoryID("github.com/diptadhi/datasharing"),
				borges.RepositoryID("github.com/nmorr041/datasharing"),
			},
		},
		&test{
			locID: borges.LocationID("3974996807a9f596cf25ac3a714995c24bb97e2c"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/rtyley/small-test-repo"),
				borges.RepositoryID("github.com/kuldeep992/small-test-repo"),
				borges.RepositoryID("github.com/kuldeep-singh-blueoptima/small-test-repo"),
			},
		},
		&test{
			locID: borges.LocationID("6671f3b1147324f4fb1fbbe2aba843031738f59e"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/enaqx/awesome-pentest"),
				borges.RepositoryID("github.com/Inter1292/awesome-pentest"),
				borges.RepositoryID("github.com/apelsin83/awesome-pentest"),
			},
		},
		&test{
			locID: borges.LocationID("cce60e1b6fb7ad56d07cbcaee7a62030f7d01777"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/kahun/awesome-sysadmin"),
				borges.RepositoryID("github.com/apoliukh/awesome-sysadmin"),
				borges.RepositoryID("github.com/gauravaristocrat/awesome-sysadmin"),
			},
		},
		&test{
			locID: borges.LocationID("f2cee90acf3c6644d51a37057845b98ab1580932"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/jtoy/awesome-tensorflow"),
				borges.RepositoryID("github.com/SiweiLuo/awesome-tensorflow"),
				borges.RepositoryID("github.com/youtang1993/awesome-tensorflow"),
			},
		},
	}

	var jobs []*library.Job
	const ep = "git://%s.git"
	logger := log.New(nil)
	for _, test := range tests {
		for _, id := range test.repoIDs {
			job := &library.Job{
				Lib:       lib,
				Type:      library.JobDownload,
				Endpoints: []string{fmt.Sprintf(ep, id)},
				TempFS:    temp,
				AuthToken: func(string) string { return "" },
				Logger:    logger,
			}

			jobs = append(jobs, job)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(jobs))
	wg.Add(len(jobs))
	for _, job := range jobs {
		j := job
		go func() {
			errs <- Download(context.TODO(), j)
			wg.Done()
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		req.NoError(err)
	}

	for _, test := range tests {
		t.Run(string(test.locID), func(t *testing.T) {
			loc, err := lib.Location(test.locID)
			req.NoError(err)

			iter, err := loc.Repositories(borges.ReadOnlyMode)
			req.NoError(err)

			var repoIDs []borges.RepositoryID
			req.NoError(iter.ForEach(func(r borges.Repository) error {
				repoIDs = append(repoIDs, r.ID())
				return nil
			}))

			req.ElementsMatch(test.repoIDs, repoIDs)
		})
	}
}
