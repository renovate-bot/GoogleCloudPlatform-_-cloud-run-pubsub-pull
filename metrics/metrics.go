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

// Package metrics implements tracking load metrics for the puller and reporting to the scaler.
package metrics

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud_run_pubsub_pull/api"
	"cloud_run_pubsub_pull/metrics/activemessages"
)

const (
	metricGranularity = 1 * time.Second
)

type ticker interface {
	C() <-chan time.Time
}

type tickerImpl struct {
	ticker *time.Ticker
}

func (t *tickerImpl) C() <-chan time.Time {
	return t.ticker.C
}

// Reporter reports load metrics to the scaler.
type Reporter interface {
	reportMetrics(ctx context.Context, load api.LoadReport) error
}

// TrackerOptions contains options for creating a Tracker.
type TrackerOptions struct {
	ReportingURL           string
	ReportingAudience      string
	ReportingInterval      time.Duration
	InstanceID             string
	MaxActiveMessagesLimit int
	MetricWindowLength     time.Duration
}

// Tracker tracks load metrics for the puller and reports to the scaler.
//
// Tracker is thread-safe.
type Tracker struct {
	nowFunc                func() time.Time
	reportingTicker        ticker
	loadReporter           Reporter
	instanceID             string
	maxActiveMessagesLimit int
	metricWindowLength     time.Duration

	mutex                sync.Mutex
	maxPullLatency       time.Duration
	pullLatencyMetric    *windowedMetric
	processingRateMetric *windowedMetric
	activeMessages       *activemessages.ActiveMessages
}

// NewTracker creates a new Tracker.
func NewTracker(ctx context.Context, opts *TrackerOptions) (*Tracker, error) {
	r, err := newReporter(ctx, opts.ReportingURL, opts.ReportingAudience)
	if err != nil {
		return nil, err
	}
	return NewTrackerWithDependencies(r, time.Now, opts)
}

// NewTrackerWithDependencies creates a new Tracker with the given dependencies.
func NewTrackerWithDependencies(reporter Reporter, nowFunc func() time.Time, opts *TrackerOptions) (*Tracker, error) {
	if opts.MetricWindowLength < metricGranularity {
		return nil, fmt.Errorf("metric window length %v must be >= the metric granularity %v", opts.MetricWindowLength, metricGranularity)
	}
	if float64(opts.MetricWindowLength/metricGranularity) != opts.MetricWindowLength.Seconds()/metricGranularity.Seconds() {
		return nil, fmt.Errorf("metric window length %v must be a multiple of the metric granularity %v", opts.MetricWindowLength, metricGranularity)
	}
	return &Tracker{
		nowFunc: nowFunc,
		reportingTicker: &tickerImpl{
			ticker: time.NewTicker(opts.ReportingInterval),
		},
		loadReporter:           reporter,
		instanceID:             opts.InstanceID,
		maxActiveMessagesLimit: opts.MaxActiveMessagesLimit,
		metricWindowLength:     opts.MetricWindowLength,
		pullLatencyMetric:      newWindowedMetric(nowFunc, opts.MetricWindowLength, metricGranularity),
		processingRateMetric:   newWindowedMetric(nowFunc, opts.MetricWindowLength, metricGranularity),
		activeMessages:         activemessages.NewWithNowFunc(nowFunc, opts.MetricWindowLength, metricGranularity),
	}, nil
}

// RecordMessageStart records the start of processing a message.
func (t *Tracker) RecordMessageStart(publishTime time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	pullLatency := t.nowFunc().Sub(publishTime)
	if pullLatency > t.maxPullLatency {
		t.maxPullLatency = pullLatency
	}
	t.pullLatencyMetric.incBy(int64(pullLatency))
	t.activeMessages.Increment()
}

// RecordMessageEnd records the end of processing a message.
func (t *Tracker) RecordMessageEnd() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.processingRateMetric.incBy(1)
	t.activeMessages.Decrement()
}

// MaxPullLatency returns the maximum latency between message publish time and puller start time.
func (t *Tracker) MaxPullLatency() time.Duration {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.maxPullLatency
}

func (t *Tracker) averagePullLatencyLocked() time.Duration {
	sum := t.pullLatencyMetric.sum()
	count := t.pullLatencyMetric.count()
	if count == 0 {
		return 0
	}
	return time.Duration(sum / count)
}

// AveragePullLatency returns the average latency between message publish time and puller start
// time over the past configured metric window.
func (t *Tracker) AveragePullLatency() time.Duration {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.averagePullLatencyLocked()
}

func (t *Tracker) processingRateLocked() float64 {
	return float64(t.processingRateMetric.sum()) / t.metricWindowLength.Seconds()
}

// ProcessingRate returns the average number of messages processed per second over the past
// configured metric window.
func (t *Tracker) ProcessingRate() float64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.processingRateLocked()
}

func (t *Tracker) averageActiveMessagesLocked() float32 {
	return t.activeMessages.AverageValue()
}

// AverageActiveMessages returns the average number of messages being processed concurrently
// over the past configured metric window.
func (t *Tracker) AverageActiveMessages() float32 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.averageActiveMessagesLocked()
}

// ReportLoadPeriodically reports load metrics periodically.
//
// ReportLoadPeriodically blocks until the context is cancelled.
func (t *Tracker) ReportLoadPeriodically(ctx context.Context) {
	tickerC := t.reportingTicker.C()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerC:
			t.reportLoad(ctx)
		}
	}
}

func (t *Tracker) createLoadReport() api.LoadReport {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return api.LoadReport{
		ReportTime:             t.nowFunc(),
		InstanceID:             t.instanceID,
		MaxPullLatency:         t.maxPullLatency,
		AveragePullLatency:     t.averagePullLatencyLocked(),
		ProcessingRate:         t.processingRateLocked(),
		AverageActiveMessages:  t.averageActiveMessagesLocked(),
		MaxActiveMessagesLimit: int32(t.maxActiveMessagesLimit),
		MetricWindowLength:     t.metricWindowLength,
	}
}

func (t *Tracker) reportLoad(ctx context.Context) {
	if err := t.loadReporter.reportMetrics(ctx, t.createLoadReport()); err != nil {
		log.Printf("Failed to report load: %v", err)
	}
}
