package subcmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/discovery"
	"github.com/src-d/gitcollector/downloader"
	"github.com/src-d/gitcollector/library"
	"github.com/src-d/gitcollector/metrics"
	"github.com/src-d/gitcollector/provider"
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
	TmpPath         string `long:"tmp" description:"directory to place generated temporal files" default:"/tmp" env:"GITCOLLECTOR_TMP"`
	Workers         int    `long:"workers" description:"number of workers, default to GOMAXPROCS" env:"GITCOLLECTOR_WORKERS"`
	HalfCPU         bool   `long:"half-cpu" description:"set the number of workers to half of the set workers" env:"GITCOLLECTOR_HALF_CPU"`
	NotAllowUpdates bool   `long:"no-updates" description:"don't allow updates on already downloaded repositories" env:"GITCOLLECTOR_NO_UPDATES"`
	NoForks         bool   `long:"no-forks" description:"github forked repositories will not be downloaded" env:"GITCOLLECTOR_NO_FORKS"`
	Orgs            string `long:"orgs" env:"GITHUB_ORGANIZATIONS" description:"list of github organization names separated by comma" required:"true"`
	ExcludedRepos   string `long:"excluded-repos" env:"GITCOLLECTOR_EXCLUDED_REPOS" description:"list of repos to exclude separated by comma" required:"false"`
	Token           string `long:"token" env:"GITHUB_TOKEN" description:"github token"`
	MetricsDBURI    string `long:"metrics-db" env:"GITCOLLECTOR_METRICS_DB_URI" description:"uri to a database where metrics will be sent"`
	MetricsDBTable  string `long:"metrics-db-table" env:"GITCOLLECTOR_METRICS_DB_TABLE" default:"gitcollector_metrics" description:"table name where the metrics will be added"`
	MetricsSync     int64  `long:"metrics-sync-timeout" env:"GITCOLLECTOR_METRICS_SYNC" default:"30" description:"timeout in seconds to send metrics"`
}

// Execute runs the command.
func (c *DownloadCmd) Execute(args []string) error {
	start := time.Now()

	if c.Orgs == "" {
		log.Warningf("no organizations found, at least one " +
			"organization must be provided")

		return nil
	}

	o := strings.Split(c.Orgs, ",")
	orgs := make([]string, 0, len(o))
	for _, org := range o {
		orgs = append(orgs, strings.ToLower(org))
	}

	ers := strings.Split(c.ExcludedRepos, ",")
	excludedRepos := make([]string, 0, len(ers))
	for _, er := range ers {
		excludedRepos = append(excludedRepos, er)
	}

	info, err := os.Stat(c.LibPath)
	if err != nil {
		log.Errorf(err, "wrong path to locate the library")
		return err
	}

	if !info.IsDir() {
		err := fmt.Errorf("%s isn't a directory", c.LibPath)
		log.Errorf(err, "wrong path to locate the library")
		return err
	}

	fs := osfs.New(c.LibPath)

	tmpPath, err := ioutil.TempDir(
		c.TmpPath, "gitcollector-downloader")
	if err != nil {
		log.Errorf(err, "unable to create temporal directory")
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmpPath); err != nil {
			log.Warningf(
				"couldn't remove temporal directory %s: %s",
				tmpPath, err.Error(),
			)
		}
	}()

	log.Debugf("temporal dir: %s", tmpPath)
	temp := osfs.New(tmpPath)

	lib, err := siva.NewLibrary("test", fs, &siva.LibraryOptions{
		Bucket:        2,
		Transactional: true,
		TempFS:        temp,
	})
	if err != nil {
		log.Errorf(err, "unable to create borges siva library")
		return err
	}

	authTokens := map[string]string{}
	if c.Token != "" {
		log.Debugf("acces token found")
		for _, org := range orgs {
			authTokens[org] = c.Token
		}
	}

	workers := c.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(-1)
	}

	if c.HalfCPU && workers > 1 {
		workers = workers / 2
	}

	updateOnDownload := !c.NotAllowUpdates
	log.Debugf("allow updates on downloads: %v", updateOnDownload)

	download := make(chan gitcollector.Job, 100)

	schedule := library.NewDownloadJobScheduleFn(
		lib,
		download,
		downloader.Download,
		updateOnDownload,
		authTokens,
		log.New(nil),
		temp,
	)

	var mc gitcollector.MetricsCollector
	if c.MetricsDBURI != "" {
		mc, err = setupMetrics(
			c.MetricsDBURI,
			c.MetricsDBTable,
			orgs,
			c.MetricsSync,
		)
		if err != nil {
			log.Errorf(err, "failed to setup metrics")
			return err
		}

		log.Debugf("metrics collection activated: sync timeout %d",
			c.MetricsSync)
	}

	wp := gitcollector.NewWorkerPool(
		schedule,
		&gitcollector.WorkerPoolOpts{
			Metrics: mc,
		},
	)

	wp.SetWorkers(workers)
	log.Debugf("number of workers in the pool %d", wp.Size())

	wp.Run()
	log.Debugf("worker pool is running")

	go runGHOrgProviders(log.New(nil), orgs, excludedRepos, c.Token, download, c.NoForks)

	wp.Wait()
	log.Debugf("worker pool stopped successfully")

	elapsed := time.Since(start).String()
	log.Infof("collection finished in %s", elapsed)
	return nil
}

func setupMetrics(
	uri, table string,
	orgs []string,
	metricSync int64,
) (gitcollector.MetricsCollector, error) {
	db, err := metrics.PrepareDB(uri, table, orgs)
	if err != nil {
		log.Errorf(err, "metrics database")
		return nil, err
	}

	mcs := make(map[string]*metrics.Collector, len(orgs))
	for _, org := range orgs {
		mc := metrics.NewCollector(&metrics.CollectorOpts{
			Log:      log.New(log.Fields{"org": org}),
			Send:     metrics.SendToDB(db, table, org),
			SyncTime: time.Duration(metricSync) * time.Second,
		})

		mcs[org] = mc
	}

	return metrics.NewCollectorByOrg(mcs), nil
}

func runGHOrgProviders(
	logger log.Logger,
	orgs []string,
	excludedRepos []string,
	token string,
	download chan gitcollector.Job,
	skipForks bool,
) {
	var wg sync.WaitGroup
	wg.Add(len(orgs))
	for _, o := range orgs {
		org := o
		p := provider.NewGitHubOrg(
			org,
			excludedRepos,
			token,
			download,
			&discovery.GitHubOpts{
				SkipForks: skipForks,
			},
		)

		go func() {
			err := p.Start()
			if err != nil &&
				!discovery.ErrNewRepositoriesNotFound.Is(err) {
				logger.Warningf(err.Error())
			}

			logger.Debugf("%s organization provider stopped", org)
			wg.Done()
		}()

		logger.Debugf("%s organization provider started", org)
	}

	wg.Wait()
	close(download)
}
