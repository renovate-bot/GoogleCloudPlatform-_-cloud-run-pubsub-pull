// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"testing"
	"time"

	"cloud_run_pubsub_pull/api"
	"cloud_run_pubsub_pull/metrics/activemessages"
	"google3/third_party/golang/cmp/cmp"
)

type fakeTicker struct {
	c chan time.Time
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.c
}

const (
	instanceID                = "instance-1"
	maxActiveMessagesLimit    = 10
	defaultMetricWindowLength = 1 * time.Minute
)

func createTracker(c *clock) *Tracker {
	return &Tracker{
		nowFunc: func() time.Time {
			return c.now
		},
		instanceID:             instanceID,
		maxActiveMessagesLimit: maxActiveMessagesLimit,
		metricWindowLength:     defaultMetricWindowLength,
		pullLatencyMetric: newWindowedMetric(func() time.Time {
			return c.now
		}, defaultMetricWindowLength, 1*time.Second),
		processingRateMetric: newWindowedMetric(func() time.Time {
			return c.now
		}, defaultMetricWindowLength, 1*time.Second),
		activeMessages: activemessages.NewWithNowFunc(func() time.Time {
			return c.now
		}, defaultMetricWindowLength, 1*time.Second),
	}
}

func TestMaxPullLatency(t *testing.T) {
	clock := &clock{now: time.UnixMilli(100)}
	tracker := createTracker(clock)

	// Expect no latency before any messages are processed.
	expected := time.Duration(0)
	if tracker.MaxPullLatency() != expected {
		t.Errorf("tracker.MaxPullLatency() = %v, want %v", tracker.MaxPullLatency(), expected)
	}
	tracker.RecordMessageStart(time.UnixMilli(90))
	expected = 10 * time.Millisecond
	if tracker.MaxPullLatency() != expected {
		t.Errorf("tracker.MaxPullLatency() = %v, want %v", tracker.MaxPullLatency(), expected)
	}
	clock.now = time.UnixMilli(120)
	tracker.RecordMessageStart(time.UnixMilli(100))
	expected = 20 * time.Millisecond
	if tracker.MaxPullLatency() != expected {
		t.Errorf("tracker.MaxPullLatency() = %v, want %v", tracker.MaxPullLatency(), expected)
	}
}

func TestAveragePullLatency(t *testing.T) {
	clock := &clock{now: time.UnixMilli(100)}
	tracker := createTracker(clock)

	// Expect no pull latency before any messages are processed.
	expected := time.Duration(0)
	if tracker.AveragePullLatency() != expected {
		t.Errorf("tracker.AveragePullLatency() = %v, want %v", tracker.AveragePullLatency(), expected)
	}
	tracker.RecordMessageStart(time.UnixMilli(90))
	expected = 10 * time.Millisecond
	if tracker.AveragePullLatency() != expected {
		t.Errorf("tracker.AveragePullLatency() = %v, want %v", tracker.AveragePullLatency(), expected)
	}
	clock.now = time.UnixMilli(1100)
	tracker.RecordMessageStart(time.UnixMilli(1000))
	expected = 55 * time.Millisecond
	if tracker.AveragePullLatency() != expected {
		t.Errorf("tracker.AveragePullLatency() = %v, want %v", tracker.AveragePullLatency(), expected)
	}
	clock.now = clock.now.Add(1 * time.Minute)
	expected = 0
	if tracker.AveragePullLatency() != expected {
		t.Errorf("tracker.AveragePullLatency() = %v, want %v", tracker.AveragePullLatency(), expected)
	}
}

func TestProcessingRate(t *testing.T) {
	clock := &clock{now: time.UnixMilli(100)}
	tracker := createTracker(clock)

	expected := float64(0.0)
	if tracker.ProcessingRate() != expected {
		t.Errorf("tracker.ProcessingRate() = %v, want %v", tracker.ProcessingRate(), expected)
	}

	tracker.RecordMessageStart(time.UnixMilli(90))
	tracker.RecordMessageEnd()
	clock.now = clock.now.Add(3 * time.Second)
	tracker.RecordMessageStart(time.UnixMilli(90))
	tracker.RecordMessageEnd()

	expected = float64(2.0 / 60.0)
	if tracker.ProcessingRate() != expected {
		t.Errorf("tracker.ProcessingRate() = %v, want %v", tracker.ProcessingRate(), expected)
	}

	clock.now = clock.now.Add(1 * time.Minute)
	expected = float64(0.0)
	if tracker.ProcessingRate() != expected {
		t.Errorf("tracker.ProcessingRate() = %v, want %v", tracker.ProcessingRate(), expected)
	}
}

func TestActiveMessages(t *testing.T) {
	clock := &clock{now: time.UnixMilli(100)}
	tracker := createTracker(clock)

	if tracker.AverageActiveMessages() != 0 {
		t.Errorf("tracker.AverageActiveMessages() = %v, want 0", tracker.AverageActiveMessages())
	}

	// 1 message active for 15s
	tracker.RecordMessageStart(time.Time{})
	clock.now = clock.now.Add(15 * time.Second)
	// 2 messages active for 30s
	tracker.RecordMessageStart(time.Time{})
	clock.now = clock.now.Add(30 * time.Second)
	// 1 message active for 15s
	tracker.RecordMessageEnd()
	clock.now = clock.now.Add(15 * time.Second)

	if tracker.AverageActiveMessages() != 1.5 {
		t.Errorf("tracker.AverageActiveMessages() = %v, want 1.5", tracker.AverageActiveMessages())
	}
}

func TestCustomMetricWindowLength(t *testing.T) {
	clock := &clock{now: time.UnixMilli(100)}
	reporter := NewFakeReporter(0)
	tracker, err := NewTrackerWithDependencies(
		reporter,
		func() time.Time {
			return clock.now
		},
		&TrackerOptions{
			ReportingInterval:      10 * time.Second,
			InstanceID:             instanceID,
			MaxActiveMessagesLimit: maxActiveMessagesLimit,
			MetricWindowLength:     10 * time.Second,
		})
	if err != nil {
		t.Fatalf("NewTrackerWithReporter: %v", err)
	}
	tracker.reportingTicker = nil

	// 1 message active for 10s
	tracker.RecordMessageStart(clock.now.Add(-2 * time.Second))
	tracker.RecordMessageEnd()
	tracker.RecordMessageStart(clock.now.Add(-2 * time.Second))
	clock.now = clock.now.Add(10 * time.Second)
	// 2 messages active for 5s
	tracker.RecordMessageStart(clock.now.Add(-1 * time.Second))
	clock.now = clock.now.Add(5 * time.Second)
	// 3 messages active for 5s
	tracker.RecordMessageStart(clock.now.Add(-500 * time.Millisecond))
	clock.now = clock.now.Add(5 * time.Second)
	tracker.RecordMessageEnd()
	tracker.RecordMessageEnd()

	if tracker.AveragePullLatency() != 500*time.Millisecond {
		t.Errorf("tracker.AveragePullLatency() = %v, want 500ms", tracker.AveragePullLatency())
	}
	if tracker.ProcessingRate() != 0.2 {
		t.Errorf("tracker.ProcessingRate() = %v, want 0.2", tracker.ProcessingRate())
	}
	if tracker.AverageActiveMessages() != 2.5 {
		t.Errorf("tracker.AverageActiveMessages() = %v, want 2.5", tracker.AverageActiveMessages())
	}
}

func TestReportLoadPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	clock := &clock{now: time.UnixMilli(100)}
	ticker := &fakeTicker{c: make(chan time.Time)}
	loadReporter := NewFakeReporter(0)
	tracker := createTracker(clock)
	tracker.reportingTicker = ticker
	tracker.loadReporter = loadReporter

	done := make(chan bool)
	go func() {
		tracker.ReportLoadPeriodically(ctx)
		done <- true
	}()

	ticker.c <- clock.now
	loadReport := <-loadReporter.Reports
	if diff := cmp.Diff(api.LoadReport{
		ReportTime:             clock.now,
		InstanceID:             instanceID,
		MaxPullLatency:         0 * time.Millisecond,
		AveragePullLatency:     0 * time.Millisecond,
		ProcessingRate:         0.0,
		AverageActiveMessages:  0.0,
		MaxActiveMessagesLimit: maxActiveMessagesLimit,
		MetricWindowLength:     defaultMetricWindowLength,
	}, loadReport); diff != "" {
		t.Errorf("tracker.ReportLoadPeriodically reported unexpected load report (-want +got):\n%s", diff)
	}

	clock.now = time.UnixMilli(120)
	// Start with 0 active messages for 1s.
	// Then, 1 active message for 5s
	tracker.RecordMessageStart(clock.now.Add(-20 * time.Millisecond))
	clock.now = clock.now.Add(5 * time.Second)
	// 2 active messages for 20s
	tracker.RecordMessageStart(clock.now.Add(-10 * time.Millisecond))
	clock.now = clock.now.Add(20 * time.Second)
	// 3 active messages for 30s
	tracker.RecordMessageStart(clock.now.Add(-6 * time.Millisecond))
	clock.now = clock.now.Add(30 * time.Second)
	// 0 active messages for 4s.
	tracker.RecordMessageEnd()
	tracker.RecordMessageEnd()
	tracker.RecordMessageEnd()
	clock.now = clock.now.Add(4 * time.Second)
	ticker.c <- clock.now

	loadReport = <-loadReporter.Reports
	if diff := cmp.Diff(api.LoadReport{
		ReportTime:             clock.now,
		InstanceID:             instanceID,
		MaxPullLatency:         20 * time.Millisecond,
		AveragePullLatency:     12 * time.Millisecond,
		ProcessingRate:         0.05, // 3 / 60 = 0.05
		AverageActiveMessages:  2.25, // (1*5 + 2*20 + 3*30) / 60 = 2.25
		MaxActiveMessagesLimit: maxActiveMessagesLimit,
		MetricWindowLength:     defaultMetricWindowLength,
	}, loadReport); diff != "" {
		t.Errorf("tracker.ReportLoadPeriodically reported unexpected load report (-want +got):\n%s", diff)
	}

	cancel()
	complete := <-done
	if !complete {
		t.Errorf("tracker.ReportLoadPeriodically did not complete")
	}
}
