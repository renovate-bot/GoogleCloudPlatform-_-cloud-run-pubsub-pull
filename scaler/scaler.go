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

// Package scaler implements the Pub/Sub pull scaler.
package scaler

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud_run_pubsub_pull/scaler/aggregator"
	"cloud_run_pubsub_pull/scaler/algorithms"
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

// LoadProvider is an interface for providing load metrics.
type LoadProvider interface {
	Load() aggregator.AggregatedLoad
}

// AdminAPI is an interface for the Cloud Run Admin API to manage the worker pool.
type AdminAPI interface {
	// GetInstanceCount returns the current number of instances in the worker pool.
	GetInstanceCount(ctx context.Context) (int, error)

	// SetInstanceCount sets the number of instances in the worker pool.
	SetInstanceCount(ctx context.Context, count int) error
}

// Options contains the options for the scaler.
type Options struct {
	// TargetLatency is the target average pull latency. Only used for the `PullLatency` algorithm.
	TargetLatency time.Duration

	// TargetUtilization is the target average active message utilization. Only used for the
	// `ActiveMessagesUtilization` algorithm.
	TargetUtilization float32

	// Algorithm is the algorithm to use for scaling.
	Algorithm algorithms.Algorithm

	// MinInstances is the minimum number of worker instances to run.
	MinInstances int

	// CycleFrequency is how often the scaling loop runs.
	CycleFrequency time.Duration
}

// Scaler implements the Pub/Sub pull scaler.
//
// Scaler is thread-safe.
type Scaler struct {
	ticker       ticker
	loadProvider LoadProvider
	adminAPI     AdminAPI
	options      Options
}

// New creates a new Scaler.
func New(loadProvider LoadProvider, adminAPI AdminAPI, options Options) *Scaler {
	return &Scaler{
		ticker:       &tickerImpl{ticker: time.NewTicker(options.CycleFrequency)},
		loadProvider: loadProvider,
		adminAPI:     adminAPI,
		options:      options,
	}
}

// RunScalingCycle runs a single scaling cycle.
//
// Gets the latest load metrics, computes a number of workers using the configured algorithm, and
// updates the number of workers.
func (s *Scaler) RunScalingCycle(ctx context.Context) error {
	currentInstances, err := s.adminAPI.GetInstanceCount(ctx)
	if err != nil {
		return err
	}
	metrics := s.loadProvider.Load()

	var recommendation int
	switch s.options.Algorithm {
	case algorithms.PullLatency:
		recommendation = algorithms.ComputePullLatencyRecommendation(currentInstances, s.options.TargetLatency, metrics)
	case algorithms.ActiveMessagesUtilization:
		if s.options.TargetUtilization <= 0 || s.options.TargetUtilization > 1 {
			return fmt.Errorf("target utilization must be in the range (0, 1]: %f", s.options.TargetUtilization)
		}
		recommendation = algorithms.ComputeActiveMessageUtilizationRecommendation(s.options.TargetUtilization, metrics)
	default:
		return fmt.Errorf("unsupported algorithm: %s", s.options.Algorithm)
	}

	adjustedRecommendation := max(recommendation, s.options.MinInstances)

	log.Printf("Current Instances: %d, Metrics: %+v, Algorithm: %s, Recommendation: %d, Adjusted Recommendation: %d", currentInstances, metrics, s.options.Algorithm, recommendation, adjustedRecommendation)
	err = s.adminAPI.SetInstanceCount(ctx, adjustedRecommendation)
	if err != nil {
		return err
	}
	return nil
}

// Run starts the Pub/Sub pull scaler running in a continuous loop.
//
// Run blocks until the context is cancelled.
func (s *Scaler) Run(ctx context.Context) error {
	tickerC := s.ticker.C()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tickerC:
			if err := s.RunScalingCycle(ctx); err != nil {
				log.Printf("Failed to run scaling cycle: %v", err)
			}
		}
	}
}
