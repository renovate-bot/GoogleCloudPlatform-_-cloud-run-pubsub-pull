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

package aggregator

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud_run_pubsub_pull/api"
	"google3/third_party/golang/cmp/cmp"
)

type clock struct {
	now time.Time
}

func newAggregator(clock *clock) *Aggregator {
	return &Aggregator{
		nowFunc: func() time.Time {
			return clock.now
		},
		loadByInstanceID: make(map[string]api.LoadReport),
	}
}

func createLoadReport(t *testing.T, load *api.LoadReport) *http.Request {
	t.Helper()
	encoded, err := json.Marshal(load)
	if err != nil {
		t.Fatalf("Failed to marshal load report: %v", err)
	}
	return httptest.NewRequest("POST", "/reportLoad", bytes.NewBuffer(encoded))
}

func sendLoadReport(t *testing.T, aggregator *Aggregator, load *api.LoadReport) {
	t.Helper()
	req := createLoadReport(t, load)
	resp := httptest.NewRecorder()
	aggregator.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("Failed to send load report: %v", resp.Code)
	}
}

func TestAggregatesLoad(t *testing.T) {
	clock := &clock{now: time.Time{}}
	aggregator := newAggregator(clock)

	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:             "instance-1",
		MaxPullLatency:         10,
		AveragePullLatency:     10 * time.Millisecond,
		ProcessingRate:         1.0,
		AverageActiveMessages:  1.0,
		MaxActiveMessagesLimit: 10,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:             "instance-2",
		MaxPullLatency:         20,
		AveragePullLatency:     20 * time.Millisecond,
		ProcessingRate:         2.0,
		AverageActiveMessages:  2.0,
		MaxActiveMessagesLimit: 10,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:             "instance-3",
		MaxPullLatency:         30,
		AveragePullLatency:     45 * time.Millisecond,
		ProcessingRate:         6.0,
		AverageActiveMessages:  3.0,
		MaxActiveMessagesLimit: 10,
	})

	expectedLoad := AggregatedLoad{
		MaxPullLatency:                    30,
		AveragePullLatency:                25 * time.Millisecond,
		AverageProcessingRatePerInstance:  3.0,
		AverageActiveMessages:             6.0,
		MaxActiveMessagesLimitPerInstance: 10,
	}
	aggregatedLoad := aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestAggregatesLoadNoInstances(t *testing.T) {
	clock := &clock{now: time.Time{}}
	aggregator := newAggregator(clock)

	expectedLoad := AggregatedLoad{
		MaxActiveMessagesLimitPerInstance: math.MaxInt32,
	}
	aggregatedLoad := aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestAggregateFractionalValues(t *testing.T) {
	clock := &clock{now: time.Time{}}
	aggregator := newAggregator(clock)

	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:         "instance-1",
		AveragePullLatency: 10 * time.Millisecond,
		ProcessingRate:     0.5,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:         "instance-2",
		AveragePullLatency: 13 * time.Millisecond,
		ProcessingRate:     0.25,
	})

	expectedLoad := AggregatedLoad{
		MaxPullLatency:                   0,
		AveragePullLatency:               11 * time.Millisecond,
		AverageProcessingRatePerInstance: 0.375,
	}
	aggregatedLoad := aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestMaxActiveMessagesLimitMismatch(t *testing.T) {
	clock := &clock{now: time.Time{}}
	aggregator := newAggregator(clock)

	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:             "instance-1",
		MaxActiveMessagesLimit: 10,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		InstanceID:             "instance-2",
		MaxActiveMessagesLimit: 20,
	})

	expectedLoad := AggregatedLoad{
		MaxActiveMessagesLimitPerInstance: 10,
	}
	aggregatedLoad := aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestExpiresOldLoad(t *testing.T) {
	clock := &clock{now: time.UnixMilli(0).Add(time.Hour)}
	aggregator := newAggregator(clock)

	sendLoadReport(t, aggregator, &api.LoadReport{
		ReportTime:         clock.now.Add(-29 * time.Second),
		InstanceID:         "instance-1",
		MaxPullLatency:     10,
		AveragePullLatency: 10 * time.Millisecond,
		ProcessingRate:     1.0,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		ReportTime:         clock.now.Add(-29 * time.Second),
		InstanceID:         "instance-2",
		MaxPullLatency:     20,
		AveragePullLatency: 20 * time.Millisecond,
		ProcessingRate:     2.0,
	})
	sendLoadReport(t, aggregator, &api.LoadReport{
		ReportTime:         clock.now.Add(-30 * time.Second),
		InstanceID:         "instance-3",
		MaxPullLatency:     30,
		AveragePullLatency: 45 * time.Millisecond,
		ProcessingRate:     6.0,
	})

	expectedLoad := AggregatedLoad{
		MaxPullLatency:                   30,
		AveragePullLatency:               25 * time.Millisecond,
		AverageProcessingRatePerInstance: 3.0,
	}
	aggregatedLoad := aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
	if len(aggregator.loadByInstanceID) != 3 {
		t.Errorf("len(aggregator.loadByInstanceID) = %d, want 3", len(aggregator.loadByInstanceID))
	}

	clock.now = clock.now.Add(time.Second)

	expectedLoad = AggregatedLoad{
		MaxPullLatency:                   20,
		AveragePullLatency:               15 * time.Millisecond,
		AverageProcessingRatePerInstance: 1.5,
	}
	aggregatedLoad = aggregator.Load()
	if diff := cmp.Diff(expectedLoad, aggregatedLoad); diff != "" {
		t.Errorf("Load() returned unexpected diff (-want +got):\n%s", diff)
	}
	if len(aggregator.loadByInstanceID) != 2 {
		t.Errorf("len(aggregator.loadByInstanceID) = %d, want 2", len(aggregator.loadByInstanceID))
	}
}
