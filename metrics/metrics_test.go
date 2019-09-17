package metrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/src-d/gitcollector/library"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollectorBatch(t *testing.T) {
	var discover, download, update, fail, total uint64
	mc := NewCollector(&CollectorOpts{
		BatchSize: 10,
		SyncTime:  1 * time.Hour,
		Send: func(
			ctx context.Context,
			mc *Collector,
			_ *library.Job,
		) error {
			discover = mc.discoverCount
			download = mc.successDownloadCount
			update = mc.successUpdateCount
			fail = mc.failCount

			next := discover + download + update + fail
			require.True(t, next > total)
			require.True(t, next-total >= 10)
			total = next
			return nil
		},
	})

	go mc.Start()

	var countOne, countThree int
	for i := 0; i < 1000; i++ {
		job := &library.Job{
			Endpoints: []string{
				fmt.Sprintf("ep-%d-1", i),
				fmt.Sprintf("ep-%d-2", i),
				fmt.Sprintf("ep-%d-3", i),
			},
		}

		switch i % 5 {
		case 0:
			job.Type = library.JobDownload
			mc.Success(job)
			countOne++
		case 1:
			job.Type = library.JobDownload
			mc.Discover(job)
			countOne++
		case 2:
			job.Type = library.JobDownload
			mc.Fail(job)
			countThree++
		case 3:
			job.Type = library.JobUpdate
			mc.Success(job)
			countThree++
		case 4:
			job.Type = library.JobUpdate
			mc.Fail(job)
			countThree++
		}
	}

	mc.Stop(false)
	expected := uint64(countOne + countThree*3)
	require.Equal(t, expected, total)
}

func TestMetricsCollectorTime(t *testing.T) {
	var count int
	mc := NewCollector(&CollectorOpts{
		BatchSize: 1000000,
		SyncTime:  1 * time.Second,
		Send: func(
			ctx context.Context,
			mc *Collector,
			_ *library.Job,
		) error {
			count++
			return nil
		},
	})

	go mc.Start()

	func() {
		done := time.After(3 * time.Second)
		for {
			select {
			case <-done:
				return
			default:
				job := &library.Job{
					Type:      library.JobDownload,
					Endpoints: []string{"foo-ep"},
				}

				mc.Success(job)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	mc.Stop(false)
	require.Equal(t, 3, count)
}

func TestMetricsCollectorByOrg(t *testing.T) {
	mc := NewCollectorByOrg(map[string]*Collector{
		"org1": NewCollector(&CollectorOpts{}),
		"org2": NewCollector(&CollectorOpts{}),
		"org3": NewCollector(&CollectorOpts{}),
	})

	go mc.Start()

	orgs := []string{"org1", "org2", "org3"}
	const url = "https://github.com/%s/foo-%d"
	for i := 0; i < 999; i++ {
		ep := fmt.Sprintf(url, orgs[i%len(orgs)], i)
		job := &library.Job{
			Endpoints: []string{ep},
		}

		switch i % 5 {
		case 0:
			job.Type = library.JobDownload
			mc.Success(job)
		case 1:
			job.Type = library.JobDownload
			mc.Discover(job)
		case 2:
			job.Type = library.JobDownload
			mc.Fail(job)
		case 3:
			job.Type = library.JobUpdate
			mc.Success(job)
		case 4:
			job.Type = library.JobUpdate
			mc.Fail(job)
		}
	}

	mc.Stop(false)
	var total uint64
	for _, m := range mc.orgMetrics {
		subTotal := m.discoverCount + m.successDownloadCount +
			m.successUpdateCount + m.failCount

		require.Equal(t, uint64(333), subTotal)
		total += subTotal
	}

	require.Equal(t, uint64(999), total)
}

type closeDelayCase struct {
	name       string
	immediate  bool
	syncTime   time.Duration
	delay      time.Duration
	expCounter int
}

func TestClosesWithDelay(t *testing.T) {
	for _, c := range []closeDelayCase{
		{"ImmediateWithoutDelay", true, time.Second, 0, 0},
		{"ImmediateWithDelay", true, 500 * time.Millisecond, time.Second, 1},
		{"NonImmediateWithoutDelay", false, time.Second, 0, 1},
		{"NonImmediateWithDelay", false, 500 * time.Millisecond, time.Second, 2},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			testCloseDelayCollector(t, c)
		})
	}
}

func testCloseDelayCollector(t *testing.T, c closeDelayCase) {
	var counter int
	mc := NewCollector(&CollectorOpts{
		SyncTime: c.syncTime,
		Send: func(
			ctx context.Context,
			mc *Collector,
			_ *library.Job,
		) error {
			counter++
			return nil
		},
	})

	go mc.Start()

	job := &library.Job{
		Type:      library.JobDownload,
		Endpoints: []string{"ep"},
	}

	time.Sleep(c.delay)

	mc.Success(job)
	job.Type = library.JobUpdate
	mc.Success(job)
	mc.Stop(c.immediate)

	require.Equal(t, c.expCounter, counter)
}

func TestFailedSend(t *testing.T) {
	for _, immediate := range []bool{false, true} {
		t.Run("TestFailedSendImmediate"+strings.Title(strconv.FormatBool(immediate)),
			func(t *testing.T) {
				testFailedSend(t, immediate)
			})
	}
}

func testFailedSend(t *testing.T, stopImmediate bool) {
	mc := NewCollector(&CollectorOpts{
		SyncTime: time.Second,
		Send: func(
			ctx context.Context,
			mc *Collector,
			_ *library.Job,
		) error {
			return fmt.Errorf("mocked")
		},
	})

	go mc.Start()

	job := &library.Job{
		Type:      library.JobDownload,
		Endpoints: []string{"ep"},
	}

	mc.Success(job)
	job.Type = library.JobUpdate
	mc.Success(job)
	mc.Stop(stopImmediate)

	// TODO maybe we can stabilize it?
	if stopImmediate {
		require.True(t, mc.successUpdateCount <= 2, "expected: <= 2, got: %v", mc.successUpdateCount)
	} else {
		require.Equal(t, uint64(2), mc.successUpdateCount)
	}
}
