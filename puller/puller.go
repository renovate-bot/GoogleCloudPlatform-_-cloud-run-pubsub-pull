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

// Package puller implements pulling messages from a Cloud Pub/Sub topic.
package puller

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud_run_pubsub_pull/metrics"
	"cloud_run_pubsub_pull/pubsubmessage"
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

// Options configures the puller.
type Options struct {
	// ProjectID is the project ID to use.
	ProjectID string

	// SubscriptionID is the Pub/Sub subscription ID to use.
	SubscriptionID string

	// ClientOptions to pass through when creating the Pub/Sub client.
	ClientOptions []option.ClientOption

	// MaxOutstandingMessages is the maximum number of messages to pull from Pub/Sub at a time.
	MaxOutstandingMessages int

	// MetricWindowLength is the length of the window over which load metrics are tracked.
	MetricWindowLength time.Duration

	// ReportingURL is the URL to report load metrics to.
	ReportingURL string

	// ReportingAudience is the audience to use when generating an ID token to authenticate the load
	// reporting request.
	ReportingAudience string

	// ReportingInterval is the interval to report load metrics.
	ReportingInterval time.Duration

	// InstanceID is a unique identifier for the puller instance. Use `FetchInstanceID` to get the
	// current instance ID when running on Cloud Run.
	InstanceID string
}

// Consumer consumes messages pulled from Pub/Sub.
type Consumer interface {
	// Consume consumes a message.
	//
	// The consumer is responsible for acking the message. If the consumer returns an error,
	// the caller must nack the message.
	Consume(ctx context.Context, message *pubsubmessage.PubSubMessage) error
}

// FetchInstanceID returns the Cloud Run instance ID of the puller.
func FetchInstanceID() (string, error) {
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/id", nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// Run pulls messages from a Cloud Pub/Sub subscription and invokes the consumer.
//
// The consumer is responsible for acking the message. If the consumer returns an error,
// Run will nack the message.
//
// Run blocks until the context is cancelled.
func Run(ctx context.Context, consumer Consumer, opts *Options) error {
	t, err := metrics.NewTracker(ctx, &metrics.TrackerOptions{
		ReportingURL:           opts.ReportingURL,
		ReportingAudience:      opts.ReportingAudience,
		ReportingInterval:      opts.ReportingInterval,
		InstanceID:             opts.InstanceID,
		MaxActiveMessagesLimit: opts.MaxOutstandingMessages,
		MetricWindowLength:     opts.MetricWindowLength,
	})
	if err != nil {
		return err
	}
	return runWithMetricsTracker(ctx, consumer, opts, t)
}

func runWithMetricsTracker(ctx context.Context, consumer Consumer, opts *Options, tracker *metrics.Tracker) error {
	client, err := pubsub.NewClient(ctx, opts.ProjectID, opts.ClientOptions...)
	if err != nil {
		return err
	}
	defer client.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tracker.ReportLoadPeriodically(ctx)
		wg.Done()
	}()

	sub := client.Subscription(opts.SubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = opts.MaxOutstandingMessages
	err = sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		tracker.RecordMessageStart(message.PublishTime)
		if err := consumer.Consume(ctx, &pubsubmessage.PubSubMessage{Message: message}); err != nil {
			log.Printf("Failed to consume message: %v", err)
			message.Nack()
		}
		tracker.RecordMessageEnd()
	})
	if err != nil {
		return fmt.Errorf("sub.Receive: %w", err)
	}

	wg.Wait()
	return nil
}
