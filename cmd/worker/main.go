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

// Package main implements "sidecar mode" for the Pub/Sub pull worker.
//
// In "sidecar mode", this binary initializes the Pub/Sub client library and owns the connection to
// Pub/Sub. This binary pulls messages from Pub/Sub and forwards them to the consumer over a local
// HTTP connection. This binary acks/nacks the messages based on a 2xx or non-2xx response from
// the consumer.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cloud_run_pubsub_pull/consumer"
	"cloud_run_pubsub_pull/puller"
)

var (
	subscriptionProjectID  = flag.String("subscription_project_id", "", "The project ID containing the Pub/Sub subscription.")
	subscriptionID         = flag.String("subscription_id", "", "The Pub/Sub subscription ID.")
	maxOutstandingMessages = flag.Int("max_outstanding_messages", 1000, "The maximum number of messages to pull from Pub/Sub at a time.")
	metricWindowLength     = flag.Duration("metric_window_length", 1*time.Minute, "The length of the window over which load metrics are tracked.")
	reportingURL           = flag.String("reporting_url", "", "The URL to report load metrics to.")
	reportingAudience      = flag.String("reporting_audience", "", "The audience to use for authenticating the load reporting request.")
	reportingInterval      = flag.Duration("reporting_interval", 1*time.Second, "The interval to report load metrics.")

	localPushURL = flag.String("local_push_url", "", "The URL (a localhost path) for the consumer to send push messages to.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	log.Printf("Starting worker")

	instanceID, err := puller.FetchInstanceID()
	if err != nil {
		log.Fatalf("Failed to fetch instance ID: %v", err)
	}

	log.Printf("Creating consumer pushing to %s", *localPushURL)
	consumer, err := consumer.NewHTTPConsumer(*localPushURL, fmt.Sprintf("projects/%s/subscriptions/%s", *subscriptionProjectID, *subscriptionID))
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	options := &puller.Options{
		ProjectID:              *subscriptionProjectID,
		SubscriptionID:         *subscriptionID,
		MaxOutstandingMessages: *maxOutstandingMessages,
		MetricWindowLength:     *metricWindowLength,
		ReportingURL:           *reportingURL,
		ReportingAudience:      *reportingAudience,
		ReportingInterval:      *reportingInterval,
		InstanceID:             instanceID,
	}
	log.Printf("Starting puller with options: %+v", options)
	err = puller.Run(ctx, consumer, options)
	if err != nil {
		log.Printf("Failed to run puller: %v", err)
	}
	log.Print("Puller exiting")
}
