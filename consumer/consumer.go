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

// Package consumer provides implementations for consuming messages pulled from Pub/Sub.
//
// Options include:
//   - Callback consumer that invokes a callback for each message.
//   - HTTP consumer that delivers each message to an HTTP endpoint.
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud_run_pubsub_pull/pubsubmessage"
	"cloud.google.com/go/pubsub"
)

// CallbackConsumer consumes messages by invoking a callback.
type CallbackConsumer struct {
	Callback func(context.Context, *pubsub.Message) error
}

// Consume invokes the callback.
func (c *CallbackConsumer) Consume(ctx context.Context, message *pubsubmessage.PubSubMessage) error {
	return c.Callback(ctx, message.Message)
}

// HTTPConsumer consumes messages by delivering them to an HTTP endpoint.
type HTTPConsumer struct {
	url              string
	subscriptionName string
}

// NewHTTPConsumer creates a new HTTPConsumer.
//
// The consumer will deliver messages to the given URL.
func NewHTTPConsumer(url, subscriptionName string) (*HTTPConsumer, error) {
	if url == "" {
		return nil, fmt.Errorf("url is required")
	}
	if subscriptionName == "" {
		return nil, fmt.Errorf("subscriptionName is required")
	}
	return &HTTPConsumer{url: url, subscriptionName: subscriptionName}, nil
}

// The `message` and `body` structs re-create the documented Pub/Sub push JSON format:
// https://cloud.google.com/pubsub/docs/push#receive_push.
type message struct {
	Attributes           map[string]string `json:"attributes,omitempty"`
	Data                 []byte            `json:"data"`
	MessageID            string            `json:"message_id"`
	MessageIDCamelCase   string            `json:"messageId"`
	OrderingKey          string            `json:"orderingKey,omitempty"`
	PublishTime          time.Time         `json:"publish_time"`
	PublishTimeCamelCase time.Time         `json:"publishTime"`
}

type body struct {
	DeliveryAttempt  *int    `json:"deliveryAttempt,omitempty"`
	Message          message `json:"message"`
	SubscriptionName string  `json:"subscription"`
}

func toJSON(msg *pubsubmessage.PubSubMessage, subscriptionName string) ([]byte, error) {
	value := body{
		DeliveryAttempt: msg.DeliveryAttempt,
		Message: message{
			Attributes:           msg.Attributes,
			Data:                 msg.Data,
			MessageID:            msg.ID,
			MessageIDCamelCase:   msg.ID,
			OrderingKey:          msg.OrderingKey,
			PublishTime:          msg.PublishTime,
			PublishTimeCamelCase: msg.PublishTime,
		},
		SubscriptionName: subscriptionName,
	}
	return json.Marshal(value)
}

// Consume delivers the message to the HTTP endpoint via a POST request.
//
// If the HTTP endpoint responds with a 2xx, returns nil and acks the message. If the HTTP endpoint
// responds with a non-2xx, returns an error and the caller must nack the message.
func (c *HTTPConsumer) Consume(ctx context.Context, message *pubsubmessage.PubSubMessage) error {
	payload, err := toJSON(message, c.subscriptionName)
	if err != nil {
		return err
	}
	resp, err := http.Post(c.url, "text/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http: %s", resp.Status)
	}
	message.Ack()
	return nil
}
