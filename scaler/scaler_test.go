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

package scaler

import (
	"context"
	"testing"
	"time"

	"cloud_run_pubsub_pull/scaler/aggregator"
	"cloud_run_pubsub_pull/scaler/algorithms"
)

type fakeAdminAPI struct {
	currentInstanceCount int
	setInstanceCount     int
}

func (f *fakeAdminAPI) GetInstanceCount(ctx context.Context) (int, error) {
	return f.currentInstanceCount, nil
}

func (f *fakeAdminAPI) SetInstanceCount(ctx context.Context, count int) error {
	f.setInstanceCount = count
	return nil
}

type fakeLoadProvider struct {
	load aggregator.AggregatedLoad
}

func (l *fakeLoadProvider) Load() aggregator.AggregatedLoad {
	return l.load
}

type fakeTicker struct {
	c chan time.Time
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.c
}

func TestRunPullLatencyCycle(t *testing.T) {
	adminAPI := &fakeAdminAPI{
		currentInstanceCount: 10,
		setInstanceCount:     0,
	}
	loadProvider := &fakeLoadProvider{
		load: aggregator.AggregatedLoad{
			AveragePullLatency: 20 * time.Millisecond,
		},
	}
	scaler := New(loadProvider, adminAPI, Options{
		TargetLatency:  10 * time.Millisecond,
		Algorithm:      algorithms.PullLatency,
		CycleFrequency: 1 * time.Minute,
	})

	if err := scaler.RunScalingCycle(context.Background()); err != nil {
		t.Fatalf("RunScalingCycle() failed: %v", err)
	}
	if adminAPI.setInstanceCount != 20 {
		t.Errorf("RunScalingCycle() set instance count to %d, want 20", adminAPI.setInstanceCount)
	}
}

func TestRunActiveMessagesUtilizationCycle(t *testing.T) {
	adminAPI := &fakeAdminAPI{
		currentInstanceCount: 10,
		setInstanceCount:     0,
	}
	loadProvider := &fakeLoadProvider{
		load: aggregator.AggregatedLoad{
			AverageActiveMessages:             160,
			MaxActiveMessagesLimitPerInstance: 10,
		},
	}
	scaler := New(loadProvider, adminAPI, Options{
		TargetUtilization: 0.8,
		Algorithm:         algorithms.ActiveMessagesUtilization,
		CycleFrequency:    1 * time.Minute,
	})

	if err := scaler.RunScalingCycle(context.Background()); err != nil {
		t.Fatalf("RunScalingCycle() failed: %v", err)
	}
	if adminAPI.setInstanceCount != 20 {
		t.Errorf("RunScalingCycle() set instance count to %d, want 20", adminAPI.setInstanceCount)
	}
}

func TestEnforceMinInstances(t *testing.T) {
	adminAPI := &fakeAdminAPI{
		currentInstanceCount: 10,
		setInstanceCount:     0,
	}
	loadProvider := &fakeLoadProvider{
		load: aggregator.AggregatedLoad{
			AverageActiveMessages:             0.0,
			MaxActiveMessagesLimitPerInstance: 10,
		},
	}
	scaler := New(loadProvider, adminAPI, Options{
		TargetUtilization: 0.8,
		Algorithm:         algorithms.ActiveMessagesUtilization,
		MinInstances:      1,
		CycleFrequency:    1 * time.Minute,
	})

	if err := scaler.RunScalingCycle(context.Background()); err != nil {
		t.Fatalf("RunScalingCycle() failed: %v", err)
	}
	if adminAPI.setInstanceCount != 1 {
		t.Errorf("RunScalingCycle() set instance count to %d, want 1", adminAPI.setInstanceCount)
	}
}

func TestRunActiveMessagesUtilizationCycleInvalidTargetUtilization(t *testing.T) {
	targets := []float32{-0.1, 0, 1.1}
	for _, target := range targets {
		adminAPI := &fakeAdminAPI{}
		loadProvider := &fakeLoadProvider{
			load: aggregator.AggregatedLoad{
				AverageActiveMessages:             160,
				MaxActiveMessagesLimitPerInstance: 10,
			},
		}
		scaler := New(loadProvider, adminAPI, Options{
			TargetUtilization: target,
			Algorithm:         algorithms.ActiveMessagesUtilization,
			CycleFrequency:    1 * time.Minute,
		})

		if err := scaler.RunScalingCycle(context.Background()); err == nil {
			t.Fatal("Expected RunScalingCycle() to fail. Got nil error.")
		}
	}
}

func TestRun(t *testing.T) {
	adminAPI := &fakeAdminAPI{
		currentInstanceCount: 10,
		setInstanceCount:     0,
	}
	loadProvider := &fakeLoadProvider{
		load: aggregator.AggregatedLoad{
			AveragePullLatency: 20 * time.Millisecond,
		},
	}
	ticker := &fakeTicker{
		c: make(chan time.Time),
	}
	scaler := &Scaler{
		ticker:       ticker,
		loadProvider: loadProvider,
		adminAPI:     adminAPI,
		options: Options{
			TargetLatency:  10 * time.Millisecond,
			Algorithm:      algorithms.PullLatency,
			CycleFrequency: 1 * time.Minute,
		},
	}
	err := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err <- scaler.Run(ctx)
	}()
	ticker.c <- time.Now()
	// The channel will block until the receiver has removed the first item from it, so by sending a
	// second tick we ensure that the loop has run at least once. Since we don't update the current
	// instance count returned by the admin API, the recommendations are idempotent.
	ticker.c <- time.Now()
	cancel()
	if err := <-err; err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if adminAPI.setInstanceCount != 20 {
		t.Errorf("Run() set instance count to %d, want 20", adminAPI.setInstanceCount)
	}
}
