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

package puller

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	pb "google3/google/pubsub/v1/pubsub_go_proto"
	"cloud_run_pubsub_pull/api"
	"cloud_run_pubsub_pull/consumer"
	"cloud_run_pubsub_pull/metrics"
	"google3/third_party/golang/cloud_google_com/go/pubsub/v/v1/pstest/pstest"
	"cloud.google.com/go/pubsub"
	"google3/third_party/golang/cmp/cmp"
	"google3/third_party/golang/go_sync/errgroup/errgroup"
	"google.golang.org/api/option"
	"google3/third_party/golang/grpc/credentials/insecure/insecure"
	"google3/third_party/golang/grpc/grpc"
)

const (
	projectID = "project-id"
	topicID   = "topic-id"
	subID     = "subscription-id"
)

func getTopicName() string {
	return fmt.Sprintf("projects/%s/topics/%s", projectID, topicID)
}

func getSubscriptionName() string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID)
}

func createTracker(t *testing.T) *metrics.Tracker {
	t.Helper()
	reporter := metrics.NewFakeReporter(10)
	return createTrackerWithReporter(t, reporter)
}

func createTrackerWithReporter(t *testing.T, reporter metrics.Reporter) *metrics.Tracker {
	t.Helper()
	tracker, err := metrics.NewTrackerWithDependencies(reporter, time.Now, &metrics.TrackerOptions{
		ReportingInterval:      1 * time.Second,
		InstanceID:             "instance-1",
		MaxActiveMessagesLimit: 10,
		MetricWindowLength:     1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("metrics.NewTrackerWithDependencies: %v", err)
	}
	return tracker
}

// waitForAck waits for the message with the given ID to be acked.
func waitForAck(ctx context.Context, messageID string, srv *pstest.Server) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		for _, msg := range srv.Messages() {
			if msg.ID == messageID && msg.Acks == 1 {
				return nil
			}
		}
	}
}

func setupPubSub(ctx context.Context, t *testing.T) *pstest.Server {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { srv.Close() })
	if _, err := srv.GServer.CreateTopic(ctx, &pb.Topic{Name: getTopicName()}); err != nil {
		t.Fatalf("srv.GServer.CreateTopic: %v", err)
	}
	if _, err := srv.GServer.CreateSubscription(ctx, &pb.Subscription{
		Name:  getSubscriptionName(),
		Topic: getTopicName(),
	}); err != nil {
		t.Fatalf("srv.GServer.CreateSubscription: %v", err)
	}
	return srv
}

func createConn(t *testing.T, srv *pstest.Server) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func createAckConsumer() Consumer {
	return &consumer.CallbackConsumer{
		Callback: func(ctx context.Context, message *pubsub.Message) error {
			log.Printf("Consumed message: %v", message)
			message.Ack()
			return nil
		}}
}

func TestConsumeMessage(t *testing.T) {
	ctx := context.Background()
	srv := setupPubSub(ctx, t)
	consumer := createAckConsumer()
	tracker := createTracker(t)
	group, childCtx := errgroup.WithContext(ctx)
	childCtx, cancel := context.WithCancel(childCtx)

	group.Go(func() error {
		return runWithMetricsTracker(childCtx, consumer, &Options{
			ProjectID:      projectID,
			SubscriptionID: subID,
			ClientOptions: []option.ClientOption{
				option.WithGRPCConn(createConn(t, srv)),
			},
		}, tracker)
	})
	messageID := srv.Publish(getTopicName(), []byte{1}, map[string]string{})

	err := waitForAck(ctx, messageID, srv)
	if err != nil {
		t.Error("Message was not acked")
	}
	cancel()
	err = group.Wait()
	if err != nil {
		t.Fatalf("runWithMetricsTracker: %v", err)
	}
}

func TestRecordLag(t *testing.T) {
	ctx := context.Background()
	srv := setupPubSub(ctx, t)
	consumer := createAckConsumer()
	group, childCtx := errgroup.WithContext(ctx)
	childCtx, cancel := context.WithCancel(childCtx)
	tracker := createTracker(t)

	messageID := srv.Publish(getTopicName(), []byte{1}, map[string]string{})
	time.Sleep(2 * time.Second)

	group.Go(func() error {
		return runWithMetricsTracker(childCtx, consumer, &Options{
			ProjectID:      projectID,
			SubscriptionID: subID,
			ClientOptions: []option.ClientOption{
				option.WithGRPCConn(createConn(t, srv)),
			},
		}, tracker)
	})
	err := waitForAck(ctx, messageID, srv)
	if err != nil {
		t.Error("Message was not acked")
	}
	cancel()
	err = group.Wait()
	if err != nil {
		t.Fatalf("runWithMetricsTracker: %v", err)
	}
	if tracker.MaxPullLatency() < 1*time.Second {
		t.Errorf("tracker.MaxPullLatency() = %v, want >= 1s", tracker.MaxPullLatency())
	}
}

func TestRecordProcessingRate(t *testing.T) {
	ctx := context.Background()
	srv := setupPubSub(ctx, t)
	consumer := createAckConsumer()
	group, childCtx := errgroup.WithContext(ctx)
	childCtx, cancel := context.WithCancel(childCtx)
	tracker := createTracker(t)

	srv.Publish(getTopicName(), []byte{1}, map[string]string{})
	srv.Publish(getTopicName(), []byte{1}, map[string]string{})
	messageID := srv.Publish(getTopicName(), []byte{1}, map[string]string{})

	group.Go(func() error {
		return runWithMetricsTracker(childCtx, consumer, &Options{
			ProjectID:      projectID,
			SubscriptionID: subID,
			ClientOptions: []option.ClientOption{
				option.WithGRPCConn(createConn(t, srv)),
			},
		}, tracker)
	})
	err := waitForAck(ctx, messageID, srv)
	if err != nil {
		t.Error("Message was not acked")
	}
	cancel()
	err = group.Wait()
	if err != nil {
		t.Fatalf("runWithMetricsTracker: %v", err)
	}
	if tracker.ProcessingRate() != 3.0/60.0 {
		t.Errorf("tracker.ProcessingRate() = %v, want 3/60 = 0.05", tracker.ProcessingRate())
	}
}

func TestReportLoad(t *testing.T) {
	ctx := context.Background()
	srv := setupPubSub(ctx, t)
	consumer := createAckConsumer()
	reporter := metrics.NewFakeReporter(10)
	tracker := createTrackerWithReporter(t, reporter)

	group, childCtx := errgroup.WithContext(ctx)
	childCtx, cancel := context.WithCancel(childCtx)

	group.Go(func() error {
		return runWithMetricsTracker(childCtx, consumer, &Options{
			ProjectID:      projectID,
			SubscriptionID: subID,
			ClientOptions: []option.ClientOption{
				option.WithGRPCConn(createConn(t, srv)),
			},
			InstanceID: "instance-1",
		}, tracker)
	})

	loadReport := <-reporter.Reports
	if diff := cmp.Diff(api.LoadReport{
		InstanceID:             "instance-1",
		MaxPullLatency:         0 * time.Millisecond,
		AveragePullLatency:     0 * time.Millisecond,
		ProcessingRate:         0.0,
		AverageActiveMessages:  0.0,
		MaxActiveMessagesLimit: 10,
		MetricWindowLength:     1 * time.Minute,
	}, loadReport, cmp.FilterPath(func(p cmp.Path) bool {
		return p.String() == "ReportTime"
	}, cmp.Ignore())); diff != "" {
		t.Errorf("tracker.ReportLoadPeriodically reported unexpected load report (-want +got):\n%s", diff)
	}

	cancel()
	err := group.Wait()
	if err != nil {
		t.Fatalf("runWithMetricsTracker: %v", err)
	}
}
