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

package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"google3/third_party/golang/cmp/cmp"
)

func handleGetResource(resource map[string]any, w http.ResponseWriter, r *http.Request) error {
	marshalled, err := json.Marshal(resource)
	if err != nil {
		return err
	}
	w.Write(marshalled)
	return nil
}

func handlePatchResource(w http.ResponseWriter, r *http.Request, done bool) (map[string]any, url.Values, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, err
	}
	resource := map[string]any{}
	if err := json.Unmarshal(body, &resource); err != nil {
		return nil, nil, err
	}
	err = respondWithOperation(w, r, done, nil /*error*/)
	if err != nil {
		return nil, nil, err
	}
	return resource, r.URL.Query(), nil
}

func validateWaitOperationRequest(r *http.Request) error {
	req := map[string]any{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return err
	}
	timeout, ok := req["timeout"]
	if !ok {
		return fmt.Errorf("timeout field is not present. got: %v", req)
	}
	timeoutStr, ok := timeout.(string)
	if !ok {
		return fmt.Errorf("timeout field is not a string. got %v", req)
	}
	if timeoutStr != "60s" {
		return fmt.Errorf("timeout got %q, want '60s'", timeoutStr)
	}
	return nil
}

func respondWithOperation(w http.ResponseWriter, r *http.Request, done bool, error any) error {
	op := map[string]any{
		"name": "projects/test-project/locations/test-region/operations/test-operation",
	}
	if done {
		op["done"] = true
	}
	if error != nil {
		op["error"] = error
	}
	marshalled, err := json.Marshal(op)
	if err != nil {
		return err
	}
	w.Write(marshalled)
	return nil
}

func TestGetInstanceCount(t *testing.T) {
	const expectedInstanceCount = 10
	worker := map[string]any{
		"name": "projects/test-project/locations/test-region/workerPools/test-worker",
		"scaling": map[string]any{
			"manualInstanceCount": expectedInstanceCount,
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Method: %s, want GET", r.Method)
		}
		if r.URL.Path != "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			t.Errorf("Path: %s, want /v2/projects/test-project/locations/test-region/workerPools/test-worker", r.URL.Path)
		}
		if err := handleGetResource(worker, w, r); err != nil {
			t.Fatalf("Failed to handle get worker pool: %v", err)
		}
	}))
	defer ts.Close()

	api := newCloudRunAdminAPIWithClient(ts.Client(), "test-region", "projects/test-project/locations/test-region/workerPools/test-worker")
	api.baseURL = ts.URL
	instanceCount, err := api.GetInstanceCount(context.Background())

	if err != nil {
		t.Fatalf("Failed to get worker pool: %v", err)
	}
	if instanceCount != expectedInstanceCount {
		t.Errorf("Worker pool instance count: %d, want %d", instanceCount, expectedInstanceCount)
	}
}

func TestSetInstanceCount(t *testing.T) {
	worker := map[string]any{
		"name": "projects/test-project/locations/test-region/workerPools/test-worker",
		"scaling": map[string]any{
			"manualInstanceCount": 10,
		},
	}
	var gotWorker map[string]any
	var query url.Values
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			if err := handleGetResource(worker, w, r); err != nil {
				t.Fatalf("Failed to handle get worker pool: %v", err)
			}
			return
		}
		if r.Method == http.MethodPatch && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			var err error
			gotWorker, query, err = handlePatchResource(w, r, true /*done*/)
			if err != nil {
				t.Fatalf("Failed to handle patch worker pool: %v", err)
			}
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	api := newCloudRunAdminAPIWithClient(ts.Client(), "test-region", "projects/test-project/locations/test-region/workerPools/test-worker")
	api.baseURL = ts.URL

	err := api.SetInstanceCount(context.Background(), 20)

	if err != nil {
		t.Fatalf("Failed to set instance count: %v", err)
	}
	if gotInstanceCount := gotWorker["scaling"].(map[string]any)["manualInstanceCount"].(float64); gotInstanceCount != 20.0 {
		t.Errorf("Worker pool instance count: %f, want 20", gotInstanceCount)
	}
	updateMask, ok := query["updateMask"]
	if !ok {
		t.Errorf("Update mask not found in query")
	}
	if diff := cmp.Diff([]string{"scaling.manualInstanceCount"}, updateMask); diff != "" {
		t.Errorf("Update mask diff (-want +got):\n%s", diff)
	}
}

func TestSetInstanceCountService(t *testing.T) {
	service := map[string]any{
		"name": "projects/test-project/locations/test-region/services/test-service",
		"scaling": map[string]any{
			"manualInstanceCount": 10,
		},
	}
	var gotService map[string]any
	var query url.Values
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v2/projects/test-project/locations/test-region/services/test-service" {
			if err := handleGetResource(service, w, r); err != nil {
				t.Fatalf("Failed to handle get service: %v", err)
			}
			return
		}
		if r.Method == http.MethodPatch && r.URL.Path == "/v2/projects/test-project/locations/test-region/services/test-service" {
			var err error
			gotService, query, err = handlePatchResource(w, r, true /*done*/)
			if err != nil {
				t.Fatalf("Failed to handle patch service: %v", err)
			}
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	api := newCloudRunAdminAPIWithClient(ts.Client(), "test-region", "projects/test-project/locations/test-region/services/test-service")
	api.baseURL = ts.URL

	err := api.SetInstanceCount(context.Background(), 20)

	if err != nil {
		t.Fatalf("Failed to set instance count: %v", err)
	}
	if gotInstanceCount := gotService["scaling"].(map[string]any)["manualInstanceCount"].(float64); gotInstanceCount != 20.0 {
		t.Errorf("Service instance count: %f, want 20", gotInstanceCount)
	}
	updateMask, ok := query["updateMask"]
	if !ok {
		t.Errorf("Update mask not found in query")
	}
	if diff := cmp.Diff([]string{"scaling.manualInstanceCount"}, updateMask); diff != "" {
		t.Errorf("Update mask diff (-want +got):\n%s", diff)
	}
}

func TestSetInstanceCountPollOnOperation(t *testing.T) {
	worker := map[string]any{
		"name": "projects/test-project/locations/test-region/workerPools/test-worker",
		"scaling": map[string]any{
			"manualInstanceCount": 10,
		},
	}
	waitOperationRequestCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			if err := handleGetResource(worker, w, r); err != nil {
				t.Fatalf("Failed to handle get worker pool: %v", err)
			}
			return
		}
		if r.Method == http.MethodPatch && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			_, _, err := handlePatchResource(w, r, false /*done*/)
			if err != nil {
				t.Fatalf("Failed to handle patch worker pool: %v", err)
			}
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v2/projects/test-project/locations/test-region/operations/test-operation:wait" {
			err := validateWaitOperationRequest(r)
			if err != nil {
				t.Errorf("Failed to validate wait operation request: %v", err)
			}
			done := waitOperationRequestCount > 0
			err = respondWithOperation(w, r, done, nil /*error*/)
			if err != nil {
				t.Fatalf("Failed to handle operation wait: %v", err)
			}
			waitOperationRequestCount++
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	api := newCloudRunAdminAPIWithClient(ts.Client(), "test-region", "projects/test-project/locations/test-region/workerPools/test-worker")
	api.baseURL = ts.URL

	err := api.SetInstanceCount(context.Background(), 20)

	if err != nil {
		t.Fatalf("Failed to set instance count: %v", err)
	}
	if waitOperationRequestCount != 2 {
		t.Errorf("Wait operation request count: %d, want 2", waitOperationRequestCount)
	}
}

func TestSetInstanceCountOperationError(t *testing.T) {
	worker := map[string]any{
		"name": "projects/test-project/locations/test-region/workerPools/test-worker",
		"scaling": map[string]any{
			"manualInstanceCount": 10,
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			if err := handleGetResource(worker, w, r); err != nil {
				t.Fatalf("Failed to handle get worker pool: %v", err)
			}
			return
		}
		if r.Method == http.MethodPatch && r.URL.Path == "/v2/projects/test-project/locations/test-region/workerPools/test-worker" {
			_, _, err := handlePatchResource(w, r, false /*done*/)
			if err != nil {
				t.Fatalf("Failed to handle patch worker pool: %v", err)
			}
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/v2/projects/test-project/locations/test-region/operations/test-operation:wait" {
			err := respondWithOperation(w, r, true /*done*/, "Operation failed")
			if err != nil {
				t.Fatalf("Failed to handle operation wait: %v", err)
			}
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer ts.Close()

	api := newCloudRunAdminAPIWithClient(ts.Client(), "test-region", "projects/test-project/locations/test-region/workerPools/test-worker")
	api.baseURL = ts.URL

	err := api.SetInstanceCount(context.Background(), 20)

	if err == nil {
		t.Error("SetInstanceCount() = nil, want error")
	}
}

func TestGetRegion(t *testing.T) {
	for _, resourceName := range []string{
		"projects/test-project/locations/test-region/services/test-service",
		"projects/test-project/locations/test-region/workerPools/test-worker",
	} {
		region, err := getRegion(resourceName)
		if err != nil {
			t.Errorf("getRegion(%q) = %v, want nil", resourceName, err)
			continue
		}
		if region != "test-region" {
			t.Errorf("getRegion(%q) = %q, want 'test-region'", resourceName, region)
		}
	}
}
