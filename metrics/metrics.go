package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/src-d/gitcollector"
	"github.com/src-d/gitcollector/library"
	"gopkg.in/src-d/go-log.v1"
)

// SendFn is the function a Collector will use to export metrics.
type SendFn func(context.Context, *Collector, *library.Job) error

// CollectorOpts represenst configuration options for a Collector.
type CollectorOpts struct {
	BatchSize int
	SyncTime  time.Duration
	Log       log.Logger
	Send      SendFn
}

// Collector is an implementation of gitcollector.MetricsCollector
type Collector struct {
	logger log.Logger
	opts   *CollectorOpts

	success              chan gitcollector.Job
	successDownloadCount uint64
	successUpdateCount   uint64

	fail      chan gitcollector.Job
	failCount uint64

	discover      chan gitcollector.Job
	discoverCount uint64

	wg     sync.WaitGroup
	cancel chan bool
}

var _ gitcollector.MetricsCollector = (*Collector)(nil)

const (
	batchSize   = 10
	syncTime    = 30 * time.Second
	waitTimeout = 5 * time.Second
)

// NewCollector builds a new Collector.
func NewCollector(opts *CollectorOpts) *Collector {
	if opts.BatchSize <= 0 {
		opts.BatchSize = batchSize
	}

	if opts.SyncTime <= 0 {
		opts.SyncTime = syncTime
	}

	if opts.Log == nil {
		opts.Log = log.New(nil)
	}

	if opts.Send == nil {
		opts.Send = func(
			_ context.Context,
			_ *Collector,
			_ *library.Job,
		) error {
			return nil
		}
	}

	opts.Log = opts.Log.New(log.Fields{"metrics": "library"})
	capacity := 5 * opts.BatchSize
	return &Collector{
		logger:   opts.Log,
		opts:     opts,
		success:  make(chan gitcollector.Job, capacity),
		fail:     make(chan gitcollector.Job, capacity),
		discover: make(chan gitcollector.Job, capacity),
		cancel:   make(chan bool),
	}
}

const (
	successKind = iota
	failKind
	discoverKind
)

// Start implements the gitcollector.MetricsCollector interface.
func (c *Collector) Start() {
	c.wg.Add(1)
	defer c.wg.Done()

	var (
		stop     bool
		batch    int
		lastSent = time.Now()
		job      *library.Job
		waiting  bool
	)

	ctx := context.Background()
	for !(c.isClosed() || stop) {
		var (
			j    gitcollector.Job
			kind int
		)

		select {
		case job, ok := <-c.success:
			if !ok {
				c.success = nil
				continue
			}

			j, kind = job, successKind
		case job, ok := <-c.fail:
			if !ok {
				c.fail = nil
				continue
			}

			j, kind = job, failKind
		case job, ok := <-c.discover:
			if !ok {
				c.discover = nil
				continue
			}

			j, kind = job, discoverKind
		case stop = <-c.cancel:
			c.close()
			continue
		case <-time.After(waitTimeout):
			if !waiting {
				c.logger.Debugf("waiting new metrics")
				waiting = true
			}
		}

		if j != nil {
			var ok bool
			job, ok = j.(*library.Job)
			if !ok {
				c.logger.Warningf("wrong job found: %T", j)
				continue
			}

			if err := c.modifyMetrics(job, kind); err != nil {
				log.Warningf(err.Error())
				continue
			}

			batch++
			waiting = false
		}

		if c.sendMetric(batch, lastSent) {
			if err := c.opts.Send(ctx, c, job); err != nil {
				c.logger.Warningf(
					"couldn't send metrics: %s",
					err.Error(),
				)

				continue
			}

			c.logMetrics(true)
			lastSent = time.Now()
			batch = 0
			waiting = false
		}
	}

	if batch > 0 && !stop {
		if err := c.opts.Send(ctx, c, job); err != nil {
			c.logger.Warningf(
				"couldn't send metrics: %s",
				err.Error(),
			)
		}
	}

	c.logMetrics(false)
}

func (c *Collector) logMetrics(debug bool) {
	logger := c.logger.New(log.Fields{
		"discover": c.discoverCount,
		"download": c.successDownloadCount,
		"update":   c.successUpdateCount,
		"fail":     c.failCount,
	})

	msg := "metrics updated"
	if debug {
		logger.Debugf(msg)
	} else {
		logger.Infof(msg)
	}
}

func (c *Collector) isClosed() bool {
	return c.success == nil && c.fail == nil && c.discover == nil
}

func (c *Collector) close() {
	close(c.success)
	close(c.fail)
	close(c.discover)
	close(c.cancel)
	c.cancel = nil
}

func (c *Collector) modifyMetrics(job *library.Job, kind int) error {
	switch kind {
	case successKind:
		if job.Type == library.JobDownload {
			c.successDownloadCount++
			break
		}

		for range job.Endpoints {
			c.successUpdateCount++
		}
	case failKind:
		for range job.Endpoints {
			c.failCount++
		}
	case discoverKind:
		if job.Type == library.JobDownload {
			c.discoverCount++
		}
	default:
		return fmt.Errorf("wrong metric type found: %d", kind)
	}

	return nil
}

func (c *Collector) sendMetric(batch int, lastSent time.Time) bool {
	fullBatch := batch >= c.opts.BatchSize
	syncTimeout := time.Since(lastSent) >= c.opts.SyncTime
	if syncTimeout {
		msg := "sync timeout"
		if batch == 0 {
			msg += ": nothing to update"
		}

		c.logger.Debugf(msg)
	}

	return fullBatch || (syncTimeout && batch > 0)
}

// Stop implements the gitcollector.MetricsCollector interface.
func (c *Collector) Stop(immediate bool) {
	if c.cancel == nil {
		return
	}

	c.cancel <- immediate
	c.wg.Wait()
}

// Success implements the gitcollector.MetricsCollector interface.
func (c *Collector) Success(job gitcollector.Job) {
	c.success <- job
}

// Fail implements the gitcollector.MetricsCollector interface.
func (c *Collector) Fail(job gitcollector.Job) {
	c.fail <- job
}

// Discover implements the gitcollector.MetricsCollector interface.
func (c *Collector) Discover(job gitcollector.Job) {
	c.discover <- job
}

// CollectorByOrg plays as a reverse proxy Collector for several organizations.
type CollectorByOrg struct {
	orgMetrics map[string]*Collector
}

// NewCollectorByOrg builds a new CollectorByOrg.
func NewCollectorByOrg(orgsMetrics map[string]*Collector) *CollectorByOrg {
	return &CollectorByOrg{
		orgMetrics: orgsMetrics,
	}
}

// Start implements the gitcollector.MetricsCollector interface.
func (c *CollectorByOrg) Start() {
	for _, m := range c.orgMetrics {
		go m.Start()
	}
}

// Stop implements the gitcollector.MetricsCollector interface.
func (c *CollectorByOrg) Stop(immediate bool) {
	for _, m := range c.orgMetrics {
		m.Stop(immediate)
	}
}

// Success implements the gitcollector.MetricsCollector interface.
func (c *CollectorByOrg) Success(job gitcollector.Job) {
	orgs := triageJob(job)
	for org, job := range orgs {
		m, ok := c.orgMetrics[org]
		if !ok {
			continue
		}

		m.Success(job)
	}
}

// Fail implements the gitcollector.MetricsCollector interface.
func (c *CollectorByOrg) Fail(job gitcollector.Job) {
	orgs := triageJob(job)
	for org, job := range orgs {
		m, ok := c.orgMetrics[org]
		if !ok {
			continue
		}

		m.Fail(job)
	}
}

// Discover implements the gitcollector.MetricsCollector interface.
func (c *CollectorByOrg) Discover(job gitcollector.Job) {
	orgs := triageJob(job)
	for org, job := range orgs {
		m, ok := c.orgMetrics[org]
		if !ok {
			continue
		}

		m.Discover(job)
	}
}

func triageJob(job gitcollector.Job) map[string]*library.Job {
	organizations := map[string]*library.Job{}
	lj, _ := job.(*library.Job)
	for _, ep := range lj.Endpoints {
		org := library.GetOrgFromEndpoint(ep)
		j, ok := organizations[org]
		if !ok {
			j = &(*lj)
			j.Endpoints = []string{}
			organizations[org] = j
		}

		j.Endpoints = append(j.Endpoints, ep)
	}

	return organizations
}
