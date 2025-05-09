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

// Package algorithms contains the algorithms for scaling the number of Pub/Sub pull workers.
package algorithms

import (
	"fmt"
	"math"
	"time"

	"cloud_run_pubsub_pull/scaler/aggregator"
)

// Algorithm is the algorithm to use for scaling.
//
// The available algorithms are defined as constants in this package below.
type Algorithm int

const (
	// PullLatency is the algorithm to scale based on average pull latency.
	PullLatency Algorithm = iota
	// ActiveMessagesUtilization is the algorithm to scale based on average active message
	// utilization.
	ActiveMessagesUtilization
)

// SupportedAlgorithmsHelpList is the list of supported algorithms for exposing in help text.
const SupportedAlgorithmsHelpList = "pull_latency, active_messages_utilization"

func (a Algorithm) String() string {
	switch a {
	case PullLatency:
		return "pull_latency"
	case ActiveMessagesUtilization:
		return "active_messages_utilization"
	default:
		return "unknown"
	}
}

// AlgorithmFromString returns the Algorithm enum value for the given string.
//
// Returns an error if the string is not a valid algorithm.
func AlgorithmFromString(s string) (Algorithm, error) {
	switch s {
	case "pull_latency":
		return PullLatency, nil
	case "active_messages_utilization":
		return ActiveMessagesUtilization, nil
	default:
		return 0, fmt.Errorf("unsupported algorithm: %s", s)
	}
}

// ComputePullLatencyRecommendation computes the number of pull workers to run based on the average
// pull latency over the past minute. The algorithm tries to keep the average pull latency equal to
// the target latency.
func ComputePullLatencyRecommendation(currentInstances int, targetLatency time.Duration, metrics aggregator.AggregatedLoad) int {
	avgLatencyMs := float64(metrics.AveragePullLatency.Milliseconds())
	targetLatencyMs := float64(targetLatency.Milliseconds())
	fractionalInstances := float64(max(currentInstances, 1)) * (avgLatencyMs / targetLatencyMs)
	return int(math.Ceil(fractionalInstances))
}

// ComputeActiveMessageUtilizationRecommendation computes the number of pull workers to run based on
// the average active message utilization over the past minute. The algorithm tries to keep the
// average active message utilization equal to the target utilization.
//
// Average active message utilization is defined as the average number of messages being processed
// concurrently over the past minute across all instances divided by the maximum number of messages
// that can be processed concurrently by all instances.
func ComputeActiveMessageUtilizationRecommendation(targetUtilization float32,
	metrics aggregator.AggregatedLoad) int {
	rawInstances := metrics.AverageActiveMessages / float32(metrics.MaxActiveMessagesLimitPerInstance)
	targetInstances := rawInstances / targetUtilization
	return int(math.Ceil(float64(targetInstances)))
}
