package subcmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/discovery"
	"github.com/src-d/gitcollector/downloader"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-cli.v0"
	"gopkg.in/src-d/go-log.v1"
)

// DownloadCmd is the gitcollector subcommand to download repositories.
type DownloadCmd struct {
	cli.Command `name:"download" short-description:"download repositories from a github organization"`

	LibPath         string `long:"library" description:"path where download to" env:"GITCOLLECTOR_LIBRARY" required:"true"`
	LibBucket       int    `long:"bucket" description:"library bucketization level" env:"GITCOLLECTOR_LIBRARY_BUCKET" default:"2"`
	Workers         int    `long:"workers" description:"number of workers, default to GOMAXPROCS" env:"GITCOLLECTOR_WORKERS"`
	NotAllowUpdates bool   `long:"no-updates" description:"don't allow updates on already downloaded repositories" env:"GITCOLLECTOR_NO_UPDATES"`
	Org             string `long:"org" env:"GITHUB_ORGANIZATION" description:"github organization" required:"true"`
	Token           string `long:"token" env:"GITHUB_TOKEN" description:"github token"`
}

// Execute runs the command.
func (c *DownloadCmd) Execute(args []string) error {
	info, err := os.Stat(c.LibPath)
	check(err, "wrong path to locate the library")

	if !info.IsDir() {
		check(
			fmt.Errorf("%s isn't a directory", c.LibPath),
			"wrong path to locate the library",
		)
	}

	fs := osfs.New(c.LibPath)

	downloaderTmpPath, err := ioutil.TempDir("", "gitcollector-downloader")
	check(err, "unable to create temporal directory")
	defer func() {
		if err := os.RemoveAll(downloaderTmpPath); err != nil {
			log.Warningf(
				"couldn't remove temporal directory %s: %s",
				downloaderTmpPath, err.Error(),
			)
		}
	}()

	log.Debugf("temporal dir: %s", downloaderTmpPath)
	temp := osfs.New(downloaderTmpPath)

	lib, err := siva.NewLibrary("test", fs, siva.LibraryOptions{
		Bucket:        2,
		Transactional: true,
	})
	check(err, "unable to create borges siva library")

	authTokens := map[string]string{}
	if c.Token != "" {
		log.Debugf("acces token found")
		authTokens[c.Org] = c.Token
	}

	workers := c.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(-1)
	}

	updateOnDownload := !c.NotAllowUpdates
	log.Debugf("allow updates on downloads: %v", updateOnDownload)
	download := make(chan gitcollector.Job, 100)
	wp := gitcollector.NewWorkerPool(
		gitcollector.NewJobScheduler(
			library.NewDownloadJobScheduleFn(
				lib,
				download,
				downloader.Download,
				updateOnDownload,
				authTokens,
				log.New(nil),
				temp,
			),
			&gitcollector.JobSchedulerOpts{},
		),
	)

	wp.SetWorkers(workers)
	log.Debugf("number of workers in the pool %d", workers)

	wp.Run()
	log.Debugf("worker pool is running")

	dp := discovery.NewGHProvider(
		c.Org,
		download,
		&discovery.GHProviderOpts{
			AuthToken: c.Token,
		},
	)

	log.Debugf("github provider started")
	if err := dp.Start(); err != nil &&
		!gitcollector.ErrProviderStopped.Is(err) {
		check(err, "github provider failed")
	}

	close(download)
	wp.Wait()
	log.Debugf("worker pool stopped successfully")
	return nil
}

func check(err error, message string) {
	if err != nil {
		log.Errorf(err, message)
		os.Exit(1)
	}
}
