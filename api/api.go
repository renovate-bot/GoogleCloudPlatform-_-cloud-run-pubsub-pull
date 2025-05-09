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

// Package api provides the API between the worker pool and the scaler service.
package api

import "time"

// LoadReport contains the metrics reported by each worker pool instance to the scaler service.
type LoadReport struct {
	// ReportTime is the time the load report was created.
	ReportTime time.Time

	// InstanceID is the ID of the worker pool instance.
	InstanceID string

	// MaxPullLatency is the maximum latency between message publish time and puller start time.
	MaxPullLatency time.Duration

	// AveragePullLatency is the average latency between message publish time and puller start
	// time over the past configured metric window.
	AveragePullLatency time.Duration

	// ProcessingRate is the average number of messages processed per second over the past
	// configured metric window.
	ProcessingRate float64

	// AverageActiveMessages is the average number of messages being processed concurrently over
	// the past configured metric window.
	AverageActiveMessages float32

	// MaxActiveMessagesLimit is the maximum number of messages that can be processed concurrently.
	MaxActiveMessagesLimit int32

	// MetricWindowLength is the length of the window over which the load metrics are tracked.
	MetricWindowLength time.Duration
}
