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

package algorithms

import (
	"testing"
	"time"

	"cloud_run_pubsub_pull/scaler/aggregator"
)

func TestAlgorithmFromString(t *testing.T) {
	tests := []struct {
		s    string
		want Algorithm
	}{
		{
			s:    "pull_latency",
			want: PullLatency,
		},
		{
			s:    "active_messages_utilization",
			want: ActiveMessagesUtilization,
		},
	}
	for _, test := range tests {
		got, err := AlgorithmFromString(test.s)
		if err != nil {
			t.Errorf("AlgorithmFromString(%q) failed: %v", test.s, err)
		}
		if got != test.want {
			t.Errorf("AlgorithmFromString(%q) = %v, want %v", test.s, got, test.want)
		}
	}
}

func TestAlgorithmFromStringInvalid(t *testing.T) {
	s := "invalid"
	if _, err := AlgorithmFromString(s); err == nil {
		t.Errorf("AlgorithmFromString(%q) succeeded, want error", s)
	}
}

func TestComputePullLatencyRecommendationNoInstances(t *testing.T) {
	metrics := aggregator.AggregatedLoad{AveragePullLatency: 10 * time.Millisecond}
	currentInstances := 0
	targetLatency := 10 * time.Millisecond

	expected := 1
	if got := ComputePullLatencyRecommendation(currentInstances, targetLatency, metrics); got != expected {
		t.Errorf("ComputePullLatencyRecommendation(%v, %v, %v) = %v, want %v", currentInstances, targetLatency, metrics, got, expected)
	}
}

func TestComputePullLatencyRecommendationScaleUp(t *testing.T) {
	metrics := aggregator.AggregatedLoad{AveragePullLatency: 15 * time.Millisecond}
	currentInstances := 4
	targetLatency := 10 * time.Millisecond

	expected := 6
	if got := ComputePullLatencyRecommendation(currentInstances, targetLatency, metrics); got != expected {
		t.Errorf("ComputePullLatencyRecommendation(%v, %v, %v) = %v, want %v", currentInstances, targetLatency, metrics, got, expected)
	}
}

func TestComputePullLatencyRecommendationScaleDown(t *testing.T) {
	metrics := aggregator.AggregatedLoad{AveragePullLatency: 5 * time.Millisecond}
	currentInstances := 4
	targetLatency := 10 * time.Millisecond

	expected := 2
	if got := ComputePullLatencyRecommendation(currentInstances, targetLatency, metrics); got != expected {
		t.Errorf("ComputePullLatencyRecommendation(%v, %v, %v) = %v, want %v", currentInstances, targetLatency, metrics, got, expected)
	}
}

func TestComputePullLatencyRecommendationNoChange(t *testing.T) {
	metrics := aggregator.AggregatedLoad{AveragePullLatency: 9 * time.Millisecond}
	currentInstances := 4
	targetLatency := 10 * time.Millisecond

	expected := 4
	if got := ComputePullLatencyRecommendation(currentInstances, targetLatency, metrics); got != expected {
		t.Errorf("ComputePullLatencyRecommendation(%v, %v, %v) = %v, want %v", currentInstances, targetLatency, metrics, got, expected)
	}
}

func TestComputeActiveMessageUtilizationRecommendation(t *testing.T) {
	tests := []struct {
		targetUtilization      float32
		averageActiveMessages  float32
		maxActiveMessagesLimit int
		expected               int
	}{
		{
			targetUtilization:      0.8,
			averageActiveMessages:  40,
			maxActiveMessagesLimit: 10,
			expected:               5,
		},
		{
			targetUtilization:      0.8,
			averageActiveMessages:  35,
			maxActiveMessagesLimit: 10,
			expected:               5,
		},
		{
			targetUtilization:      0.5,
			averageActiveMessages:  40,
			maxActiveMessagesLimit: 10,
			expected:               8,
		},
		{
			targetUtilization:      1.0,
			averageActiveMessages:  40,
			maxActiveMessagesLimit: 10,
			expected:               4,
		},
	}
	for _, test := range tests {
		got := ComputeActiveMessageUtilizationRecommendation(test.targetUtilization, aggregator.AggregatedLoad{
			AverageActiveMessages:         test.averageActiveMessages,
			MaxActiveMessagesLimitPerInstance: test.maxActiveMessagesLimit,
		})
		if got != test.expected {
			t.Errorf("ComputeActiveMessageUtilizationRecommendation(%v, {AverageActiveMessages: %v, MaxActiveMessagesLimitPerInstance: %v}) = %v, want %v", test.targetUtilization, test.averageActiveMessages, test.maxActiveMessagesLimit, got, test.expected)
		}
	}
}
