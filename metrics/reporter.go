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

package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"cloud_run_pubsub_pull/api"
	"google.golang.org/api/idtoken"
)

type reporterImpl struct {
	client    *http.Client
	targetURL string
}

func (r *reporterImpl) reportMetrics(ctx context.Context, load api.LoadReport) error {
	b, err := json.Marshal(load)
	if err != nil {
		return err
	}
	resp, err := r.client.Post(r.targetURL, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}
	defer resp.Body.Close()
	return nil
}

func newReporter(ctx context.Context, targetURL, audience string) (Reporter, error) {
	client, err := idtoken.NewClient(ctx, audience)
	if err != nil {
		return nil, err
	}
	return &reporterImpl{
		client:    client,
		targetURL: targetURL}, nil
}

// FakeReporter is a fake implementation of Reporter for testing.
type FakeReporter struct {
	Reports chan api.LoadReport
}

// NewFakeReporter creates a new FakeReporter.
func NewFakeReporter(bufferSize int) *FakeReporter {
	return &FakeReporter{
		Reports: make(chan api.LoadReport, bufferSize),
	}
}

func (r *FakeReporter) reportMetrics(ctx context.Context, load api.LoadReport) error {
	r.Reports <- load
	return nil
}
