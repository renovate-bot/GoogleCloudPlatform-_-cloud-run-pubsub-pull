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

// Package main implements an example consumer demonstrating the Pub/Sub pull worker in sidecar
// mode.
//
// This consumer runs an HTTP server that accepts a JSON payload containing a message from the
// Pub/Sub pull worker sidecar, sleeps for a specified amount of time, and then acks/nacks the
// message at a configured rate.
package main

import (
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"flag"
	
	"github.com/golang/glog"
)

const defaultPort = "8081"

var (
	sleepDuration = flag.Duration("sleep_duration", 1*time.Second, "The duration to sleep for when consuming messages.")
	nackPercent   = flag.Int("nack_percent", 0, "The percentage of messages to nack. Integer between 0 and 100.")
)

func handlePush(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Failed to read body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	glog.V(3).Infof("Push request body: %v", string(body))

	glog.V(2).Infof("Received push request. Sleeping for %v", *sleepDuration)
	time.Sleep(*sleepDuration)

	if *nackPercent > 0 && rand.Intn(100) < *nackPercent {
		glog.V(2).Info("Nacking message")
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		glog.V(2).Info("Acking message")
		w.WriteHeader(http.StatusOK)
	}
}

func main() {
	flag.Parse()
	http.HandleFunc("/", handlePush)
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	glog.Infof("Starting HTTP server listening on port %s", port)
	glog.Info(http.ListenAndServe(":"+port, nil))
	glog.Info("Consumer exiting")
}
