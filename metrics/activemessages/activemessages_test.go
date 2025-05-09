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

package activemessages

import (
	"testing"
	"time"
)

type clock struct {
	now time.Time
}

func (c *clock) advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestEmpty(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 1*time.Second, 1*time.Second)

	avg := tracker.AverageValue()
	if avg != 0 {
		t.Errorf("AverageValue() = %v, want 0", avg)
	}
}

func TestSingleBucket(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 1*time.Second, 1*time.Second)

	// value=1 for 250ms
	tracker.Increment()
	clock.advance(250 * time.Millisecond)
	// value=2 for 100ms
	tracker.Increment()
	clock.advance(100 * time.Millisecond)
	// value=3 for 150ms
	tracker.Increment()
	clock.advance(150 * time.Millisecond)
	// value=2 for 100ms
	tracker.Decrement()
	clock.advance(100 * time.Millisecond)
	// value=1 for 100ms
	tracker.Decrement()
	clock.advance(100 * time.Millisecond)
	// value=2 for 300ms
	tracker.Increment()
	clock.advance(300 * time.Millisecond)

	avg := tracker.AverageValue()
	if avg != 1.8 {
		t.Errorf("AverageValue() = %v, want 1.8", avg)
	}
}

func TestMultipleConsecutiveBuckets(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 2*time.Second, 1*time.Second)

	// value=1 for 500ms
	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	// value=2 for 500ms
	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	// value=1 for 250ms
	tracker.Decrement()
	clock.advance(250 * time.Millisecond)
	// value=2 for 750ms
	tracker.Increment()
	clock.advance(750 * time.Millisecond)

	avg := tracker.AverageValue()
	if avg != 1.625 {
		t.Errorf("AverageValue() = %v, want 1.625", avg)
	}
}

func TestMultipleNonConsecutiveBuckets(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 4*time.Second, 1*time.Second)

	// value=1 for 500ms
	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	// value=2 for 3s
	tracker.Increment()
	clock.advance(3 * time.Second)
	// value=1 for 500ms
	tracker.Decrement()
	clock.advance(500 * time.Millisecond)

	avg := tracker.AverageValue()
	if avg != 1.75 {
		t.Errorf("AverageValue() = %v, want 1.75", avg)
	}
}

func TestNowInMiddleOfEndBucket(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 1*time.Second, 1*time.Second)

	tracker.Increment()
	clock.advance(500 * time.Millisecond)

	avg := tracker.AverageValue()
	if avg != 0.5 {
		t.Errorf("AverageValue() = %v, want 0.5", avg)
	}
}

func TestWindowStartInMiddleOfBucket(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 2*time.Second, 1*time.Second)

	// value=1 for 1s
	tracker.Increment()
	clock.advance(1 * time.Second)
	// value=2 for 1.5s
	tracker.Increment()
	clock.advance(1500 * time.Millisecond)

	// We still count the entire first bucket, even though the window starts in the middle.
	avg := tracker.AverageValue()
	if avg != 1.6 {
		t.Errorf("AverageValue() = %v, want 1.6", avg)
	}
}

func TestCountsBucketsWithNoDataAsZero(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 2*time.Second, 1*time.Second)

	tracker.Increment()
	clock.advance(1 * time.Second)

	avg := tracker.AverageValue()
	if avg != 0.5 {
		t.Errorf("AverageValue() = %v, want 0.5", avg)
	}
}

func TestCurrentValueIsZero(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 1*time.Second, 1*time.Second)

	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	tracker.Decrement()
	clock.advance(500 * time.Millisecond)

	avg := tracker.AverageValue()
	if avg != 0.5 {
		t.Errorf("AverageValue() = %v, want 0.5", avg)
	}
}

func TestExcludesBucketsBeforeWindowStart(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 2*time.Second, 1*time.Second)

	tracker.Increment()
	clock.advance(1 * time.Second)
	tracker.Increment()
	clock.advance(2 * time.Second)

	avg := tracker.AverageValue()
	if avg != 2 {
		t.Errorf("AverageValue() = %v, want 2", avg)
	}
}

func TestOverwritesOldBuckets(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	tracker := NewWithNowFunc(func() time.Time { return clock.now }, 1*time.Second, 1*time.Second)

	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	tracker.Increment()
	clock.advance(500 * time.Millisecond)
	tracker.Decrement()
	clock.advance(1 * time.Second)

	avg := tracker.AverageValue()
	if avg != 1 {
		t.Errorf("AverageValue() = %v, want 1", avg)
	}
}
