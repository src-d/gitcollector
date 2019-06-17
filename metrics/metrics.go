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
func (mc *Collector) Start() {
	mc.wg.Add(1)
	defer mc.wg.Done()

	var (
		stop     bool
		batch    int
		lastSent = time.Now()
		job      *library.Job
	)

	for !(mc.isClosed() || stop) {
		var (
			j    gitcollector.Job
			kind int
		)

		select {
		case job, ok := <-mc.success:
			if !ok {
				mc.success = nil
				continue
			}

			j, kind = job, successKind
		case job, ok := <-mc.fail:
			if !ok {
				mc.fail = nil
				continue
			}

			j, kind = job, failKind
		case job, ok := <-mc.discover:
			if !ok {
				mc.discover = nil
				continue
			}

			j, kind = job, discoverKind
		case stop = <-mc.cancel:
			mc.close()
			continue
		case <-time.After(waitTimeout):
			mc.logger.Debugf("waiting new metrics")
			continue
		}

		var ok bool
		job, ok = j.(*library.Job)
		if !ok {
			mc.logger.Warningf("wrong job found: %T", j)
			continue
		}

		if err := mc.modifyMetrics(job, kind); err != nil {
			log.Warningf(err.Error())
			continue
		}

		batch++
		if mc.sendMetric(batch, lastSent) {
			lastSent = time.Now()
			if err := mc.opts.Send(
				context.TODO(),
				mc,
				job,
			); err != nil {
				mc.logger.Warningf(
					"couldn't send metrics: %s",
					err.Error(),
				)

				continue
			}

			batch = 0
		}
	}

	if batch > 0 && !stop {
		if err := mc.opts.Send(context.TODO(), mc, job); err != nil {
			mc.logger.Warningf(
				"couldn't send metrics: %s",
				err.Error(),
			)
		}
	}

	mc.logger.Infof(
		"discover: %d, download: %d, update: %d, fail: %d",
		mc.discoverCount,
		mc.successDownloadCount,
		mc.successUpdateCount,
		mc.failCount,
	)
}

func (mc *Collector) isClosed() bool {
	return mc.success == nil && mc.fail == nil && mc.discover == nil
}

func (mc *Collector) close() {
	close(mc.success)
	close(mc.fail)
	close(mc.discover)
	close(mc.cancel)
	mc.cancel = nil
}

func (mc *Collector) modifyMetrics(job *library.Job, kind int) error {
	switch kind {
	case successKind:
		if job.Type == library.JobDownload {
			mc.successDownloadCount++
			break
		}

		for range job.Endpoints {
			mc.successUpdateCount++
		}
	case failKind:
		for range job.Endpoints {
			mc.failCount++
		}
	case discoverKind:
		if job.Type == library.JobDownload {
			mc.discoverCount++
		}
	default:
		return fmt.Errorf("wrong metric type found: %d", kind)
	}

	return nil
}

func (mc *Collector) sendMetric(batch int, lastSent time.Time) bool {
	fullBatch := batch >= mc.opts.BatchSize
	syncTimeout := time.Since(lastSent) >= mc.opts.SyncTime && batch > 0
	return fullBatch || syncTimeout
}

// Stop implements the gitcollector.MetricsCollector interface.
func (mc *Collector) Stop(immediate bool) {
	if mc.cancel == nil {
		return
	}

	mc.cancel <- immediate
	mc.wg.Wait()
}

// Success implements the gitcollector.MetricsCollector interface.
func (mc *Collector) Success(job gitcollector.Job) {
	mc.success <- job
}

// Fail implements the gitcollector.MetricsCollector interface.
func (mc *Collector) Fail(job gitcollector.Job) {
	mc.fail <- job
}

// Discover implements the gitcollector.MetricsCollector interface.
func (mc *Collector) Discover(job gitcollector.Job) {
	mc.discover <- job
}
