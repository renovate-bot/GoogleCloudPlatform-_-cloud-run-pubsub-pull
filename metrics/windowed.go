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
	"container/ring"
	"log"
	"time"
)

// timeBucket represents a time period and the sum/count of a metric over that time period.
type timeBucket struct {
	endTime time.Time
	sum     int64
	count   int64
}

func (t *timeBucket) incBy(value int64) {
	t.sum += value
	t.count++
}

// windowedMetric tracks a metric over a window of time.
//
// windowedMetric is thread-compatible.
type windowedMetric struct {
	nowFunc func() time.Time

	// Buckets of time tracking the sum/count over the interval.
	//
	// Ordered by endTime. Buckets are aligned to the granularity and contiguous.
	//
	// Always points to the most recent bucket.
	buckets *ring.Ring

	// The length of the window over which the metric is tracked.
	windowLength time.Duration
	// The size of each time bucket.
	granularity time.Duration
}

// newWindowedMetric creates a new windowedMetric with the given options.
//
// `nowFunc` is used to get the current time.
//
// `windowLength` is the length of the window over which the metric is tracked.
//
// `granularity` is the size of each time bucket.
func newWindowedMetric(nowFunc func() time.Time, windowLength, granularity time.Duration) *windowedMetric {
	return &windowedMetric{
		nowFunc:      nowFunc,
		buckets:      initializedRing(int(windowLength / granularity)),
		windowLength: windowLength,
		granularity:  granularity,
	}
}

// initializedRing creates a zero-initialized ring with the given length.
func initializedRing(length int) *ring.Ring {
	r := ring.New(length)
	for ; r.Value == nil; r = r.Next() {
		r.Value = &timeBucket{}
	}
	return r
}

// incBy adds the given value to the metric at the current time.
func (w *windowedMetric) incBy(value int64) {
	now := w.nowFunc()
	currentBucketEnd := now.Truncate(w.granularity).Add(w.granularity)
	lastBucket := w.buckets.Value.(*timeBucket)

	if lastBucket.endTime.Equal(time.Time{}) {
		// This is the first update. Initialize the last bucket to the current time.
		w.buckets.Value = &timeBucket{
			endTime: currentBucketEnd,
			sum:     value,
			count:   1,
		}
		return
	}

	// Check to make sure time didn't go backwards. If it did, discard the update.
	if lastTrackedTime := lastBucket.endTime.Add(-w.windowLength); currentBucketEnd.Before(lastTrackedTime) {
		log.Printf("Current time %v is before last tracked time %v", currentBucketEnd, lastTrackedTime)
		return
	}

	// Insert new buckets if needed.
	if currentBucketEnd.After(lastBucket.endTime) {
		bucketsToInsert := int((currentBucketEnd.Sub(lastBucket.endTime) / w.granularity))
		b := w.buckets
		for i := 1; i <= bucketsToInsert; i++ {
			b = b.Next()
			b.Value = &timeBucket{
				endTime: lastBucket.endTime.Add(w.granularity * time.Duration(i)),
				sum:     0,
				count:   0,
			}
		}
		b.Value.(*timeBucket).incBy(value)
		w.buckets = b
		return
	}

	// Iterate through the existing buckets backwards until we find the current bucket.
	for i, b := 0, w.buckets; i < w.buckets.Len(); i++ {
		bucket := b.Value.(*timeBucket)
		expectedEndTime := lastBucket.endTime.Add(-w.granularity * time.Duration(i))
		if !bucket.endTime.Equal(expectedEndTime) {
			// Re-initialize buckets that are expired or were never used.
			b.Value = &timeBucket{
				endTime: expectedEndTime,
				sum:     0,
				count:   0,
			}
			bucket = b.Value.(*timeBucket)
		}
		if bucket.endTime.Equal(currentBucketEnd) {
			bucket.incBy(value)
			return
		}
		b = b.Prev()
	}

	// This should never happen. `now` should always fall into one of the above three cases.
	log.Printf("Current time %v is not in any bucket.", currentBucketEnd)
}

// iterateWindow iterates over the buckets in the tracked time window backwards, applying `f`.
func (w *windowedMetric) iterateWindow(f func(*timeBucket)) {
	nowBucketEnd := w.nowFunc().Truncate(w.granularity).Add(w.granularity)
	windowStart := nowBucketEnd.Add(-w.windowLength)
	b := w.buckets
	for i := 0; i < w.buckets.Len(); i++ {
		bucket := b.Value.(*timeBucket)
		if bucket.endTime.Before(windowStart) || bucket.endTime.Equal(windowStart) {
			break
		}
		f(bucket)
		b = b.Prev()
	}
}

// sum returns the sum of the metric over the window.
func (w *windowedMetric) sum() int64 {
	sum := int64(0)
	w.iterateWindow(func(bucket *timeBucket) {
		sum += bucket.sum
	})
	return sum
}

// count returns the count of the metric over the window.
func (w *windowedMetric) count() int64 {
	count := int64(0)
	w.iterateWindow(func(bucket *timeBucket) {
		count += bucket.count
	})
	return count
}
