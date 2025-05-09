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

package consumer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud_run_pubsub_pull/pubsubmessage"
	"cloud.google.com/go/pubsub"
	"google3/third_party/golang/cmp/cmp"
)

func createFakeHandler(t *testing.T, c chan body, responseStatus int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Unexpected HTTP method: %s", r.Method)
		}
		if r.URL.Path != "/" {
			t.Errorf("Unexpected URL path: %s", r.URL.Path)
		}
		message := body{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			t.Fatalf("Failed to decode message: %v", err)
		}
		c <- message
		w.WriteHeader(responseStatus)
	}
}

func TestHTTPConsumerAck(t *testing.T) {
	c := make(chan body, 1)
	ts := httptest.NewServer(createFakeHandler(t, c, http.StatusOK))
	t.Cleanup(func() { ts.Close() })
	consumer := &HTTPConsumer{
		url:              ts.URL,
		subscriptionName: "projects/abc/subscriptions/def",
	}

	now := time.Now()
	deliveryAttempt := 1
	deliveredMessage := &pubsubmessage.PubSubMessage{
		Message: &pubsub.Message{
			ID:   "123",
			Data: []byte{1, 2, 3},
			Attributes: map[string]string{
				"foo": "bar",
			},
			DeliveryAttempt: &deliveryAttempt,
			PublishTime:     now,
			OrderingKey:     "abc",
		},
	}

	err := consumer.Consume(context.Background(), deliveredMessage)
	if err != nil {
		t.Fatalf("Failed to consume message: %v", err)
	}
	consumedMessage := <-c
	expectedMessage := body{
		DeliveryAttempt: &deliveryAttempt,
		Message: message{
			Attributes: map[string]string{
				"foo": "bar",
			},
			Data:                 []byte{1, 2, 3},
			MessageID:            "123",
			MessageIDCamelCase:   "123",
			OrderingKey:          "abc",
			PublishTime:          now,
			PublishTimeCamelCase: now,
		},
		SubscriptionName: "projects/abc/subscriptions/def",
	}
	if diff := cmp.Diff(expectedMessage, consumedMessage); diff != "" {
		t.Errorf("Unexpected message (-want +got):\n%s", diff)
	}
	if !deliveredMessage.WasAcked() {
		t.Error("Message was not acked")
	}
}

func TestHTTPConsumerAckMinimalFields(t *testing.T) {
	c := make(chan body, 1)
	ts := httptest.NewServer(createFakeHandler(t, c, http.StatusOK))
	t.Cleanup(func() { ts.Close() })
	consumer := &HTTPConsumer{
		url:              ts.URL,
		subscriptionName: "projects/abc/subscriptions/def",
	}
	now := time.Now()
	deliveredMessage := &pubsubmessage.PubSubMessage{
		Message: &pubsub.Message{
			ID:          "123",
			Data:        []byte{1, 2, 3},
			PublishTime: now,
		},
	}

	err := consumer.Consume(context.Background(), deliveredMessage)
	if err != nil {
		t.Fatalf("Failed to consume message: %v", err)
	}
	consumedMessage := <-c
	expectedMessage := body{
		Message: message{
			Data:                 []byte{1, 2, 3},
			MessageID:            "123",
			MessageIDCamelCase:   "123",
			PublishTime:          now,
			PublishTimeCamelCase: now,
		},
		SubscriptionName: "projects/abc/subscriptions/def",
	}
	if diff := cmp.Diff(expectedMessage, consumedMessage); diff != "" {
		t.Errorf("Unexpected message (-want +got):\n%s", diff)
	}
	if !deliveredMessage.WasAcked() {
		t.Error("Message was not acked")
	}
}

func TestHTTPConsumerNack(t *testing.T) {
	for _, responseStatus := range []int{http.StatusMultipleChoices, http.StatusBadRequest, http.StatusInternalServerError} {
		c := make(chan body, 1)
		ts := httptest.NewServer(createFakeHandler(t, c, responseStatus))
		t.Cleanup(func() { ts.Close() })
		consumer := &HTTPConsumer{
			url:              ts.URL,
			subscriptionName: "projects/abc/subscriptions/def",
		}
		deliveredMessage := &pubsubmessage.PubSubMessage{
			Message: &pubsub.Message{
				ID:          "123",
				Data:        []byte{1, 2, 3},
				PublishTime: time.Now(),
			},
		}

		err := consumer.Consume(context.Background(), deliveredMessage)
		if err == nil {
			t.Errorf("Unexpected success for response status: %v", responseStatus)
		}
		if deliveredMessage.WasAcked() {
			t.Error("Message was unexpectedly acked")
		}
	}
}
