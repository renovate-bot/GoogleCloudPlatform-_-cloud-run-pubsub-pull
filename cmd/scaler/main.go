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

// Package main implements a Pub/Sub pull based autoscaler server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"cloud_run_pubsub_pull/scaler/adminapi"
	"cloud_run_pubsub_pull/scaler/aggregator"
	"cloud_run_pubsub_pull/scaler/algorithms"
	"cloud_run_pubsub_pull/scaler"
)

const (
	loadReportPath = "/reportLoad"
	getLoadPath    = "/getLoad"
)

var (
	resourceName      = flag.String("resource_name", "", "The fully-qualified name of the scaled resource. Format 'projects/<project_id>/locations/<region>/<services|workerPools>/<resource_name>")
	algorithm         = flag.String("algorithm", "active_messages_utilization", "The algorithm to use for scaling. Supported algorithms: "+algorithms.SupportedAlgorithmsHelpList)
	targetLatency     = flag.Duration("target_latency", 0, "The target average pull latency.")
	targetUtilization = flag.Float64("target_utilization", 0.8, "The target average active message utilization.")
	minInstances      = flag.Int("min_instances", 0, "The minimum number of worker instances to run.")
	maxInstances      = flag.Int("max_instances", 0, "The maximum number of worker instances to run.")
	cycleFrequency    = flag.Duration("cycle_frequency", 1*time.Minute,
		"The frequency at which to run the scaling cycle.")
)

// Basic getter to expose the current load metrics for testing/demos.
func getLoad(w http.ResponseWriter, r *http.Request, aggregator *aggregator.Aggregator) {
	encoded, err := json.Marshal(aggregator.Load())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(encoded)
}

func main() {
	flag.Parse()

	if *resourceName == "" {
		log.Fatal("resource_name must be set")
	}
	algorithm, err := algorithms.AlgorithmFromString(*algorithm)
	if err != nil {
		log.Fatalf("Failed to parse algorithm: %v", err)
	}
	if algorithm == algorithms.PullLatency && *targetLatency == 0 {
		log.Fatal("target_latency must be set if algorithm is pull_latency")
	}
	if algorithm == algorithms.ActiveMessagesUtilization && (*targetUtilization <= 0 || *targetUtilization > 1) {
		log.Fatal("target_utilization must be set to a value in the range (0, 1] if algorithm is active_messages_utilization")
	}

	aggregator := aggregator.NewAggregator()
	http.Handle(loadReportPath, aggregator)
	http.HandleFunc(getLoadPath, func(w http.ResponseWriter, r *http.Request) {
		getLoad(w, r, aggregator)
	})

	ctx := context.Background()

	adminAPI, err := adminapi.NewCloudRunAdminAPI(ctx, *resourceName)
	if err != nil {
		log.Fatalf("Failed to create admin API: %v", err)
	}

	options := scaler.Options{
		TargetLatency:     *targetLatency,
		TargetUtilization: float32(*targetUtilization),
		Algorithm:         algorithm,
		MinInstances:      *minInstances,
		CycleFrequency:    *cycleFrequency,
	}
	if *maxInstances > 0 {
		options.MaxInstances = maxInstances
	}
	scaler, err := scaler.New(aggregator, adminAPI, options)
	if err != nil {
		log.Fatalf("Failed to create scaler: %v", err)
	}

	go func() {
		if err := scaler.Run(ctx); err != nil {
			log.Printf("Failed to run scaler: %v", err)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Listening on port %s", port)
	log.Print(http.ListenAndServe(":"+port, nil))
}
