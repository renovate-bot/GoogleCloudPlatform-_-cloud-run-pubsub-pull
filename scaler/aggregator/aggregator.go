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

// Package aggregator aggregates load metrics from Pub/Sub pull worker instances.
package aggregator

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"cloud_run_pubsub_pull/api"
)

// loadReportExpirationDuration is the amount of time to keep a load report before expiring it.
const loadReportExpirationDuration = 30 * time.Second

// Aggregator aggregates load metrics from Pub/Sub pull worker instances.
//
// Aggregator is thread-safe.
type Aggregator struct {
	nowFunc func() time.Time

	mutex            sync.Mutex
	loadByInstanceID map[string]api.LoadReport
}

// AggregatedLoad contains the aggregated load metrics.
type AggregatedLoad struct {
	// MaxPullLatency is the maximum latency between message publish time and puller start time.
	MaxPullLatency time.Duration

	// AveragePullLatency is the average latency between message publish time and puller start
	// time over the past configured metric window.
	AveragePullLatency time.Duration

	// AverageProcessingRatePerInstance is the average number of messages processed per second
	// over the past configured metric window by the average instance.
	AverageProcessingRatePerInstance float64

	// AverageActiveMessages is the average number of messages being processed concurrently over
	// the past configured metric window across all instances.
	AverageActiveMessages float32

	// MaxActiveMessagesLimitPerInstance is the maximum number of messages that can be processed
	// concurrently by a single instance.
	MaxActiveMessagesLimitPerInstance int
}

// NewAggregator creates a new Aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{
		nowFunc:          time.Now,
		loadByInstanceID: make(map[string]api.LoadReport),
	}
}

func (a *Aggregator) aggregateLoad(load api.LoadReport) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.loadByInstanceID[load.InstanceID] = load
}

// ServeHTTP handles load reporting requests from the worker instances.
//
// Implements the http.Handler interface.
func (a *Aggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	load := api.LoadReport{}
	if err := json.NewDecoder(r.Body).Decode(&load); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("Received load report: %+v", load)
	a.aggregateLoad(load)
	w.WriteHeader(http.StatusOK)
}

// Load returns the aggregated load metrics.
func (a *Aggregator) Load() AggregatedLoad {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	count := 0
	maxLatency := 0 * time.Millisecond
	averagePullLatencyMsSum := int64(0)
	processingRateSum := 0.0
	averageActiveMessagesSum := float32(0.0)
	maxActiveMessagesLimit := int32(math.MaxInt32)
	now := a.nowFunc()
	for key, load := range a.loadByInstanceID {
		if now.Sub(load.ReportTime) > loadReportExpirationDuration {
			delete(a.loadByInstanceID, key)
			continue
		}
		count++
		maxLatency = max(maxLatency, load.MaxPullLatency)
		averagePullLatencyMsSum += load.AveragePullLatency.Milliseconds()
		processingRateSum += load.ProcessingRate
		averageActiveMessagesSum += load.AverageActiveMessages
		if maxActiveMessagesLimit != math.MaxInt32 && load.MaxActiveMessagesLimit != maxActiveMessagesLimit {
			log.Printf("MaxActiveMessagesLimit mismatch: %d != %d for instance %s", maxActiveMessagesLimit, load.MaxActiveMessagesLimit, load.InstanceID)
		}
		maxActiveMessagesLimit = min(maxActiveMessagesLimit, load.MaxActiveMessagesLimit)
	}
	averageLatency := time.Duration(0)
	averageProcessingRatePerInstance := 0.0
	if count > 0 {
		// This rounds down to the nearest millisecond.
		averageLatency = time.Duration(averagePullLatencyMsSum/int64(count)) * time.Millisecond
		averageProcessingRatePerInstance = processingRateSum / float64(count)
	}
	return AggregatedLoad{
		MaxPullLatency:                    maxLatency,
		AveragePullLatency:                averageLatency,
		AverageProcessingRatePerInstance:  averageProcessingRatePerInstance,
		AverageActiveMessages:             averageActiveMessagesSum,
		MaxActiveMessagesLimitPerInstance: int(maxActiveMessagesLimit),
	}
}
