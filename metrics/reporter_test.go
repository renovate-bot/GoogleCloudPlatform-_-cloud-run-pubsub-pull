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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud_run_pubsub_pull/api"
	"google3/third_party/golang/cmp/cmp"
)

func createFakeHandler(t *testing.T, c chan api.LoadReport) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		report := api.LoadReport{}
		err := json.NewDecoder(r.Body).Decode(&report)
		if err != nil {
			t.Fatalf("Failed to decode load report: %v", err)
		}
		c <- report
		w.WriteHeader(http.StatusOK)
	}
}

func TestReportMetrics(t *testing.T) {
	c := make(chan api.LoadReport, 1)
	ts := httptest.NewServer(createFakeHandler(t, c))
	t.Cleanup(func() { ts.Close() })
	r := &reporterImpl{
		client:    ts.Client(),
		targetURL: ts.URL,
	}
	load := api.LoadReport{
		InstanceID:     "abc",
		MaxPullLatency: 10 * time.Second,
	}

	if err := r.reportMetrics(context.Background(), load); err != nil {
		t.Fatalf("reportMetrics: %v", err)
	}

	got := <-c
	if diff := cmp.Diff(load, got); diff != "" {
		t.Errorf("reportMetrics: unexpected diff (-want +got):\n%s", diff)
	}
}
