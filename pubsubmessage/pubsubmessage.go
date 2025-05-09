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

// Package pubsubmessage provides a wrapper around `pubsub.Message` that exposes whether the
// message has been acked or not.
package pubsubmessage

import "cloud.google.com/go/pubsub"

// PubSubMessage is a wrapper around `pubsub.Message` that exposes whether the message has been
// acked or not.
type PubSubMessage struct {
	*pubsub.Message
	wasAcked bool
}

// Ack acks the message and records that the message has been acked.
func (m *PubSubMessage) Ack() {
	m.Message.Ack()
	m.wasAcked = true
}

// WasAcked returns true if the message has been acked.
func (m *PubSubMessage) WasAcked() bool {
	return m.wasAcked
}
