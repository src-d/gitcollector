package downloader

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/src-d/gitcollector/downloader/testhelper"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/gitcollector/testutils"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"gopkg.in/src-d/go-log.v1"
)

type protocol string

var (
	httpsProtocol = protocol("https")
	gitProtocol   = protocol("git")

	errBrokenFS = testhelper.ErrBrokenFS
)

// TODO move this data to some config
var (
	tests = []*test{
		{
			locID: borges.LocationID("a6c64c655d15afda789f8138b83213782b6f77c7"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/prakhar1989/awesome-courses"),
				borges.RepositoryID("github.com/Leo-xxx/awesome-courses"),
				borges.RepositoryID("github.com/manjunath00/awesome-courses"),
			},
		},
		{
			locID: borges.LocationID("fe83b066a45d859cd40cbf512c4ec20351c4f9d9"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/MunGell/awesome-for-beginners"),
				borges.RepositoryID("github.com/dhruvil1514/awesome-for-beginners"),
				borges.RepositoryID("github.com/karellism/awesome-for-beginners"),
			},
		},
		{
			locID: borges.LocationID("1880dc904e1b2774be9c97a7b85efabdb910f974"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/jtleek/datasharing"),
				borges.RepositoryID("github.com/diptadhi/datasharing"),
				borges.RepositoryID("github.com/nmorr041/datasharing"),
			},
		},
		{
			locID: borges.LocationID("3974996807a9f596cf25ac3a714995c24bb97e2c"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/rtyley/small-test-repo"),
				borges.RepositoryID("github.com/kuldeep992/small-test-repo"),
				borges.RepositoryID("github.com/kuldeep-singh-blueoptima/small-test-repo"),
			},
		},
		{
			locID: borges.LocationID("6671f3b1147324f4fb1fbbe2aba843031738f59e"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/enaqx/awesome-pentest"),
				borges.RepositoryID("github.com/Inter1292/awesome-pentest"),
				borges.RepositoryID("github.com/apelsin83/awesome-pentest"),
			},
		},
		{
			locID: borges.LocationID("cce60e1b6fb7ad56d07cbcaee7a62030f7d01777"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/kahun/awesome-sysadmin"),
				borges.RepositoryID("github.com/apoliukh/awesome-sysadmin"),
				borges.RepositoryID("github.com/gauravaristocrat/awesome-sysadmin"),
			},
		},
		{
			locID: borges.LocationID("f2cee90acf3c6644d51a37057845b98ab1580932"),
			repoIDs: []borges.RepositoryID{
				borges.RepositoryID("github.com/jtoy/awesome-tensorflow"),
				borges.RepositoryID("github.com/SiweiLuo/awesome-tensorflow"),
				borges.RepositoryID("github.com/youtang1993/awesome-tensorflow"),
			},
		},
	}

	repoIDsFlat = getRepoIDsFlat()

	testPrivateRepo = &test{
		locID: borges.LocationID("2ea758d7c7cbc249acfd6fc4a67f926cae28c10e"),
		repoIDs: []borges.RepositoryID{
			borges.RepositoryID("github.com/lwsanty/super-private-privacy-keep-out"),
		},
	}
)

type test struct {
	locID   borges.LocationID
	repoIDs []borges.RepositoryID
}

/* TODO:
- network errors
- resolveCommit https://codecov.io/gh/src-d/gitcollector/compare/4dc597c03b4a5106fbf3ba87f35835cf171fb8ee...1310d5b5c391847255f4227eb1746a31871418a3/src/downloader/git.go#L164
*/

func TestAll(t *testing.T) {
	h, close, err := testhelper.NewHelper()
	defer close()
	require.NoError(t, err)

	for _, tst := range []struct {
		name  string
		tFunc func(t *testing.T, h *testhelper.Helper)
	}{
		{"testLibraryCreationFailed", testLibraryCreationFailed},
		{"testFSFailedStatFail", testFSFailedStatFail},
		{"testFSFailedOpenFileFail", testFSFailedOpenFileFail},
		{"testAuthSuccess", testAuthSuccess},
		{"testAuthErrors", testAuthErrors},
		{"testContextCancelledFail", testContextCancelledFail},
		{"testWrongEndpointFail", testWrongEndpointFail},
		{"testAlreadyDownloadedFail", testAlreadyDownloadedFail},
		{"testDownloadConcurrentSuccess", testDownloadConcurrentSuccess},
		{"testPeriodicallyBrokenGithubAPI", testPeriodicallyBrokenGithubAPI},
	} {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
			tst.tFunc(t, h)
			close()
		})
	}
}

// testLibraryCreationFailed
// 1) try to create *siva.NewLibrary using fs with broken OpenFile method
// <expected> error that contains broken fs mocked error's text
func testLibraryCreationFailed(t *testing.T, h *testhelper.Helper) {
	testFS := testhelper.NewBrokenFS(h.FS, testhelper.BrokenFSOptions{FailedOpen: true})
	_, err := newLibrary(testFS)
	require.Error(t, err)
	require.Contains(t, err.Error(), errBrokenFS.Error())
}

// testFSFailedStatFail
// 1) try to execute download job using fs with broken Stats method
// <expected> error that contains broken fs mocked error's text
func testFSFailedStatFail(t *testing.T, h *testhelper.Helper) {
	testFSWithErrors(t, h, testhelper.BrokenFSOptions{FailedStat: true})
}

// testFSFailedOpenFileFail
// 1) try to execute download job using fs with broken OpenFile method
// <expected> error that contains broken fs mocked error's text
func testFSFailedOpenFileFail(t *testing.T, h *testhelper.Helper) {
	testFSWithErrors(t, h, testhelper.BrokenFSOptions{FailedOpenFile: true})
}

func testFSWithErrors(t *testing.T, h *testhelper.Helper, fsOpts testhelper.BrokenFSOptions) {
	testFS := testhelper.NewBrokenFS(h.FS, fsOpts)
	lib, err := newLibrary(testFS)
	require.NoError(t, err)

	testRepo := tests[0].repoIDs[0]
	err = Download(context.Background(), &library.Job{
		Lib:       lib,
		Type:      library.JobDownload,
		Endpoints: []string{endPoint(gitProtocol, testRepo)},
		TempFS:    h.TempFS,
		AuthToken: func(string) string { return "" },
		Logger:    log.New(nil),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), errBrokenFS.Error())
}

// testAuthSuccess
// 1) try to execute download job of a private repo with valid token via https protocol
// <expected> error: nil
func testAuthSuccess(t *testing.T, h *testhelper.Helper) {
	t.Skip("skip this test until separate org is created")

	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		t.Skip()
	}

	require.NoError(t, Download(context.Background(), &library.Job{
		Lib:       h.Lib,
		Type:      library.JobDownload,
		Endpoints: []string{endPoint(httpsProtocol, testPrivateRepo.repoIDs[0])},
		TempFS:    h.TempFS,
		AuthToken: func(string) string { return token },
		Logger:    log.New(nil),
	}))
}

// testAuthErrors
// 1) try to execute download job of a private repo with corrupted token via git protocol
// <expected> error: invalid auth method
// 2) try to execute download job with corrupted token via https protocol
// <expected> error: authentication required
func testAuthErrors(t *testing.T, h *testhelper.Helper) {
	getJob := func(p protocol) *library.Job {
		return &library.Job{
			Lib:       h.Lib,
			Type:      library.JobDownload,
			Endpoints: []string{endPoint(p, testPrivateRepo.repoIDs[0])},
			TempFS:    h.TempFS,
			AuthToken: func(string) string { return "42" },
			Logger:    log.New(nil),
		}
	}

	ctx := context.Background()
	require.Equal(t, transport.ErrInvalidAuthMethod, Download(ctx, getJob(gitProtocol)))
	require.Equal(t, transport.ErrAuthenticationRequired, Download(ctx, getJob(httpsProtocol)))
}

// testContextCancelledFail
// 1) prepare context and cancel it
// 2) start download try to execute download job with canceled context passed
// <expected> error: context canceled
func testContextCancelledFail(t *testing.T, h *testhelper.Helper) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	testRepo := tests[0].repoIDs[0]
	require.Equal(t, fmt.Errorf("context canceled"), Download(ctx, &library.Job{
		Lib:       h.Lib,
		Type:      library.JobDownload,
		Endpoints: []string{endPoint(gitProtocol, testRepo)},
		TempFS:    h.TempFS,
		AuthToken: func(string) string { return "" },
		Logger:    log.New(nil),
	}))
}

// testWrongEndpointFail
// 1) try to execute download job with corrupted endpoint to the repo
// <expected> returned error should have type *net.OpError
// <expected> error should contain "no such host"
func testWrongEndpointFail(t *testing.T, h *testhelper.Helper) {
	const corruptedEndpoint = "git://42.git"

	err := Download(context.Background(), &library.Job{
		Lib:       h.Lib,
		Type:      library.JobDownload,
		Endpoints: []string{corruptedEndpoint},
		TempFS:    h.TempFS,
		AuthToken: func(string) string { return "" },
		Logger:    log.New(nil),
	})
	require.Error(t, err)

	e, ok := err.(*net.OpError)
	if !ok {
		t.Fatal("received error " + err.Error() + " is not *net.OpError")
	}
	require.Contains(t, e.Err.Error(), "no such host")
}

// testAlreadyDownloadedFail
// 1) exec download job for a test repo
// 2) try to download it again
// <expected> error: already downloaded
func testAlreadyDownloadedFail(t *testing.T, h *testhelper.Helper) {
	testRepo := tests[0].repoIDs[0]
	job := &library.Job{
		Lib:       h.Lib,
		Type:      library.JobDownload,
		Endpoints: []string{endPoint(gitProtocol, testRepo)},
		TempFS:    h.TempFS,
		AuthToken: func(string) string { return "" },
		Logger:    log.New(nil),
	}

	ctx := context.Background()
	require.NoError(t, Download(ctx, job))
	require.True(t, ErrRepoAlreadyExists.Is(Download(ctx, job)))
}

// testDownloadConcurrentSuccess
// 1) start several download jobs for several orgs
// 2) for each org
// 	 2.1) get location by id
//	 <expected> error: nil
//	 <expected> repositories ids match the initial ones
func testDownloadConcurrentSuccess(t *testing.T, h *testhelper.Helper) {
	errs := concurrentDownloads(h, gitProtocol)
	for err := range errs {
		require.NoError(t, err)
	}

	for _, test := range tests {
		t.Run(string(test.locID), func(t *testing.T) {
			loc, err := h.Lib.Location(test.locID)
			require.NoError(t, err)

			iter, err := loc.Repositories(borges.ReadOnlyMode)
			require.NoError(t, err)

			var repoIDs []borges.RepositoryID
			require.NoError(t, iter.ForEach(func(r borges.Repository) error {
				repoIDs = append(repoIDs, r.ID())
				return nil
			}))

			require.ElementsMatch(t, test.repoIDs, repoIDs)
		})
	}
}

// testPeriodicallyBrokenGithubAPI
// 1) set up https proxy that returns 500 response once in several requests
// 2) start several download jobs for several orgs in parallel
// <expected> check that part of jobs failed with corresponding error
// <expected> check that another part of jobs was successfully downloaded
func testPeriodicallyBrokenGithubAPI(t *testing.T, h *testhelper.Helper) {
	if runtime.GOOS == "darwin" {
		t.Skip("cannot run these tests on osx")
	}

	const failEach = 5

	healthyTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = healthyTransport }()

	proxy, err := testutils.NewProxy(
		healthyTransport,
		&testutils.Options{
			FailEachNthRequest: failEach,
			FailEachNthCode:    http.StatusInternalServerError,
			KeyPath:            "../_testdata/server.key",
			PemPath:            "../_testdata/server.pem",
		})
	require.NoError(t, err)

	require.NoError(t, proxy.Start())
	defer func() { proxy.Stop() }()

	require.NoError(t, testutils.SetTransportProxy())

	errs := concurrentDownloads(h, httpsProtocol)
	var failedCounter int
	blackListRepoIDs := make(map[borges.RepositoryID]struct{})
	for err := range errs {
		if err != nil {
			require.Contains(t, err.Error(), "Internal Server Error")
			log.Infof("error: %q", err.Error())
			blackListRepoIDs[getRepoIDFromErrorText(err.Error())] = struct{}{}
			failedCounter++
		}
	}
	require.True(t, failedCounter >= failEach || failedCounter < len(repoIDsFlat), "act: %v", failedCounter)

	for _, test := range tests {
		t.Run(string(test.locID), func(t *testing.T) {
			loc, err := h.Lib.Location(test.locID)
			require.NoError(t, err)

			var expRepoIDs []borges.RepositoryID
			for _, rid := range test.repoIDs {
				if _, ok := blackListRepoIDs[rid]; !ok {
					expRepoIDs = append(expRepoIDs, rid)
				}
			}

			iter, err := loc.Repositories(borges.ReadOnlyMode)
			require.NoError(t, err)

			var actRepoIDs []borges.RepositoryID
			require.NoError(t, iter.ForEach(func(r borges.Repository) error {
				actRepoIDs = append(actRepoIDs, r.ID())
				return nil
			}))

			require.ElementsMatch(t, expRepoIDs, actRepoIDs)
		})
	}
}

func concurrentDownloads(h *testhelper.Helper, p protocol) chan error {
	var jobs []*library.Job
	for _, test := range tests {
		for _, id := range test.repoIDs {
			job := &library.Job{
				Lib:       h.Lib,
				Type:      library.JobDownload,
				Endpoints: []string{endPoint(p, id)},
				TempFS:    h.TempFS,
				AuthToken: func(string) string { return "" },
				Logger:    log.New(nil),
			}

			jobs = append(jobs, job)
		}
	}

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, len(jobs))
	wg.Add(len(jobs))
	for _, job := range jobs {
		j := job
		go func() {
			errs <- Download(ctx, j)
			wg.Done()
		}()
	}

	wg.Wait()
	close(errs)

	return errs
}

// newLibrary is a wrapper around siva.NewLibrary
func newLibrary(fs billy.Filesystem) (*siva.Library, error) {
	return siva.NewLibrary("test", fs, &siva.LibraryOptions{
		Bucket:        2,
		Transactional: true,
	})
}

func endPoint(p protocol, repoID interface{}) string {
	return fmt.Sprintf("%s://%s.git", p, repoID)
}

func getRepoIDsFlat() (res []borges.RepositoryID) {
	for _, test := range tests {
		for _, rid := range test.repoIDs {
			res = append(res, rid)
		}
	}
	return
}

func getRepoIDFromErrorText(text string) borges.RepositoryID {
	return borges.RepositoryID(getStringInBetween(text, "https://", ".git"))
}

func getStringInBetween(str string, start string, end string) (result string) {
	s := strings.Index(str, start)
	if s == -1 {
		return
	}
	s += len(start)
	e := strings.Index(str, end)
	return str[s:e]
}
