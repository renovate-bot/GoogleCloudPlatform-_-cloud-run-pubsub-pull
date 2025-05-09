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

// Package activemessages tracks the number of messages being processed concurrently over time.
package activemessages

import (
	"container/ring"
	"time"
)

// timeBucket tracks the number of concurrent messages over a small time period of size
// `granularity`.
//
// The period of time is [endTime - granularity, endTime) -- Inclusive start, exclusive end.
type timeBucket struct {
	// The bucket's start time. Inclusive.
	startTime time.Time
	// The bucket's end time. Exclusive.
	endTime time.Time
	// The time the bucket's value was last updated.
	lastUpdateTime time.Time
	// The time-weighted (in microseconds) sum of the bucket's values up to the last update.
	microWeightedSum float32
	// The current value of the bucket.
	currentValue int
}

// incBy increments the number of concurrent messages by `delta`.
//
// Precondition: `t.startTime` <= `t.lastUpdateTime` <= `now` < `t.endTime`
func (t *timeBucket) incBy(delta int, now time.Time) {
	microsAtCurrentValue := now.Sub(t.lastUpdateTime).Microseconds()
	t.microWeightedSum += float32(t.currentValue) * float32(microsAtCurrentValue)
	t.currentValue += delta
	t.lastUpdateTime = now
}

// ActiveMessages tracks the number of messages being processed concurrently over time.
type ActiveMessages struct {
	nowFunc func() time.Time

	// Buckets of time tracking the number of active messages.
	//
	// Ordered by endTime. Buckets are *not* aligned or contiguous.
	//
	// Always points to the most recent bucket.
	buckets *ring.Ring

	// The length of the window over which the metric is tracked.
	windowLength time.Duration

	// The size of each time bucket.
	granularity time.Duration
}

// New creates a new ActiveMessages.
//
// `windowLength` is the length of the window over which the metric is tracked.
//
// `granularity` is the size of each time bucket.
func New(windowLength, granularity time.Duration) *ActiveMessages {
	return NewWithNowFunc(time.Now, windowLength, granularity)
}

// NewWithNowFunc creates a new ActiveMessages tracker with a custom now function.
//
// `nowFunc` is the function to get the current time.
//
// `windowLength` is the length of the window over which the metric is tracked.
//
// `granularity` is the size of each time bucket.
func NewWithNowFunc(nowFunc func() time.Time, windowLength, granularity time.Duration) *ActiveMessages {
	return &ActiveMessages{
		nowFunc:      nowFunc,
		buckets:      initializedRing(int(windowLength / granularity)),
		windowLength: windowLength,
		granularity:  granularity,
	}
}

func initializedRing(length int) *ring.Ring {
	r := ring.New(length)
	for ; r.Value == nil; r = r.Next() {
		r.Value = &timeBucket{}
	}
	return r
}

// Increment increments the number of active messages by 1.
func (a *ActiveMessages) Increment() {
	a.incBy(1)
}

// Decrement decrements the number of active messages by 1.
func (a *ActiveMessages) Decrement() {
	a.incBy(-1)
}

func (a *ActiveMessages) incBy(delta int) {
	now := a.nowFunc()

	lastBucket := a.buckets.Value.(*timeBucket)
	if now.Before(lastBucket.endTime) {
		lastBucket.incBy(delta, now)
		return
	}

	b := a.buckets.Next()
	b.Value = &timeBucket{
		startTime:        now,
		endTime:          now.Add(a.granularity),
		lastUpdateTime:   now,
		microWeightedSum: 0.0,
		currentValue:     lastBucket.currentValue + delta,
	}
	a.buckets = b
}

// AverageValue returns the average number of active messages over the tracked window.
func (a *ActiveMessages) AverageValue() float32 {
	now := a.nowFunc()
	windowStart := now.Add(-a.windowLength)
	microWeightedSum := float32(0)
	totalMicroseconds := int64(0)

	b := a.buckets
	var nextBucket *timeBucket = nil
	for i := 0; i < a.buckets.Len(); i++ {
		bucket := b.Value.(*timeBucket)
		if bucket.endTime.Before(windowStart) || bucket.endTime.Equal(windowStart) {
			break
		}

		// Count the bucket's accumulated time-weighted sum.
		microWeightedSum += bucket.microWeightedSum
		totalMicroseconds += bucket.lastUpdateTime.Sub(bucket.startTime).Microseconds()

		// Count time between the bucket's last update time and the next bucket's start time (or now).
		nextBucketStart := now
		if nextBucket != nil {
			nextBucketStart = nextBucket.startTime
		}

		microsAtCurrentValue := nextBucketStart.Sub(bucket.lastUpdateTime).Microseconds()
		microWeightedSum += float32(bucket.currentValue) * float32(microsAtCurrentValue)
		totalMicroseconds += microsAtCurrentValue

		nextBucket = bucket
		b = b.Prev()
	}
	totalMicroseconds = max(totalMicroseconds, a.windowLength.Microseconds())
	return microWeightedSum / float32(totalMicroseconds)
}
