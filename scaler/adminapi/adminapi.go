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

// Package adminapi provides a client for the Cloud Run Admin API.
package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"

	"cloud.google.com/go/auth/httptransport"
)

const regionRegexStr = `projects/.+/locations/(.+)/(?:services|workerPools)/.+`

// CloudRunAdminAPI is a client for the Cloud Run Admin API.
type CloudRunAdminAPI struct {
	client       *http.Client
	baseURL      string
	resourceName string
}

// NewCloudRunAdminAPI creates a new CloudRunAdminAPI.
//
// The client is configured to use Application Default Credentials.
func NewCloudRunAdminAPI(ctx context.Context, resourceName string) (*CloudRunAdminAPI, error) {
	region, err := getRegion(resourceName)
	if err != nil {
		return nil, err
	}
	client, err := httptransport.NewClient(&httptransport.Options{})
	if err != nil {
		return nil, err
	}
	return newCloudRunAdminAPIWithClient(client, region, resourceName), nil
}

func newCloudRunAdminAPIWithClient(client *http.Client, region, resourceName string) *CloudRunAdminAPI {
	return &CloudRunAdminAPI{
		client:       client,
		baseURL:      fmt.Sprintf("https://%s-run.googleapis.com", region),
		resourceName: resourceName,
	}
}

func getRegion(resourceName string) (string, error) {
	regionRegex, err := regexp.Compile(regionRegexStr)
	if err != nil {
		return "", err
	}
	matches := regionRegex.FindStringSubmatch(resourceName)
	if len(matches) < 2 {
		return "", fmt.Errorf("resource name does not match regex %q. Resource name: %q", regionRegexStr, resourceName)
	}
	return matches[1], nil
}

func (a *CloudRunAdminAPI) buildResourceURL() string {
	return fmt.Sprintf("%s/v2/%s", a.baseURL, a.resourceName)
}

func (a *CloudRunAdminAPI) getResource(ctx context.Context) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.buildResourceURL(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	err = checkHTTPResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("get resource failed: %w", err)
	}
	resource := map[string]any{}
	err = json.NewDecoder(resp.Body).Decode(&resource)
	if err != nil {
		return nil, err
	}
	err = validateResource(resource)
	if err != nil {
		return nil, err
	}
	return resource, nil
}

func validateResource(resource map[string]any) error {
	scaling, ok := resource["scaling"]
	if !ok {
		return fmt.Errorf("resource does not have a 'scaling' field. Resource: %v", resource)
	}
	scalingMap, ok := scaling.(map[string]any)
	if !ok {
		return fmt.Errorf("scaling field is not a map. Resource: %v", resource)
	}
	manualInstanceCount, ok := scalingMap["manualInstanceCount"]
	if !ok {
		return fmt.Errorf("scaling field does not have manualInstanceCount field. Scaling: %v", scalingMap)
	}
	_, ok = manualInstanceCount.(float64)
	if !ok {
		return fmt.Errorf("manualInstanceCount field is not a float64. manualInstanceCount: %v", manualInstanceCount)
	}
	return nil
}

// GetInstanceCount returns the current number of instances in the resource (worker pool or
// service).
func (a *CloudRunAdminAPI) GetInstanceCount(ctx context.Context) (int, error) {
	resource, err := a.getResource(ctx)
	if err != nil {
		return 0, err
	}
	manualInstanceCount := resource["scaling"].(map[string]any)["manualInstanceCount"].(float64)
	return int(manualInstanceCount), nil
}

// SetInstanceCount sets the number of instances in the resource (worker pool or service).
func (a *CloudRunAdminAPI) SetInstanceCount(ctx context.Context, count int) error {
	resource, err := a.getResource(ctx)
	if err != nil {
		return err
	}
	resource["scaling"].(map[string]any)["manualInstanceCount"] = count

	log.Printf("Updating instance count to %d", count)

	body, err := json.Marshal(resource)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, a.buildResourceURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Add("updateMask", "scaling.manualInstanceCount")
	req.URL.RawQuery = query.Encode()

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	err = checkHTTPResponse(resp)
	if err != nil {
		return fmt.Errorf("update instance count failed: %w", err)
	}
	op := map[string]any{}
	err = json.NewDecoder(resp.Body).Decode(&op)
	if err != nil {
		return err
	}
	err = a.waitForOperation(ctx, op)
	if err != nil {
		return err
	}
	log.Printf("Updated instance count to %d", count)
	return nil
}

type waitOperationRequest struct {
	Timeout string `json:"timeout"`
}

func (a *CloudRunAdminAPI) waitForOperation(ctx context.Context, op map[string]any) error {
	requestBody := &waitOperationRequest{Timeout: "60s"}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		done, err := getDone(op)
		if err != nil {
			return err
		}
		if done {
			error, ok := op["error"]
			if ok {
				return fmt.Errorf("operation failed: %v", error)
			}
			return nil
		}
		body, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		name, err := getName(op)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v2/%s:wait", a.baseURL, name), bytes.NewReader(body))
		if err != nil {
			return err
		}
		resp, err := a.client.Do(req)
		if err != nil {
			return err
		}
		err = checkHTTPResponse(resp)
		if err != nil {
			return fmt.Errorf("wait operation failed: %w", err)
		}
		err = json.NewDecoder(resp.Body).Decode(&op)
		if err != nil {
			return err
		}
	}
}

func getDone(op map[string]any) (bool, error) {
	done, ok := op["done"]
	if !ok {
		return false, nil
	}
	doneBool, ok := done.(bool)
	if !ok {
		return false, fmt.Errorf("operation.done is not a bool. op: %v", op)
	}
	return doneBool, nil
}

func getName(op map[string]any) (string, error) {
	name, ok := op["name"]
	if !ok {
		return "", fmt.Errorf("operation does not have a 'name' field. op: %v", op)
	}
	nameString, ok := name.(string)
	if !ok {
		return "", fmt.Errorf("operation.name is not a string. op: %v", op)
	}
	return nameString, nil
}

func checkHTTPResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("http call failed with status %v. Failed to parse error response body: %w", resp.Status, err)
		}
		return fmt.Errorf("http call failed. Status: %v, Body: %v", resp.Status, string(body))
	}
	return nil
}
