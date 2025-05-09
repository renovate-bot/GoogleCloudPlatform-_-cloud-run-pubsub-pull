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
	"testing"
	"time"
)

func TestContinuousValues(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(2)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(3)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(4)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)

	sum := metric.sum()
	if sum != 15 {
		t.Errorf("metric.sum() = %v, want %v", sum, 15)
	}
	count := metric.count()
	if count != 5 {
		t.Errorf("metric.count() = %v, want %v", count, 5)
	}
}

func TestContinuousValuesUnaligned(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	// t = 1000
	metric.incBy(1)
	// t = 2200
	clock.now = clock.now.Add(1200 * time.Millisecond)
	metric.incBy(2)
	// t = 3050
	clock.now = clock.now.Add(850 * time.Millisecond)
	metric.incBy(3)
	// t = 4010
	clock.now = clock.now.Add(960 * time.Millisecond)
	metric.incBy(4)
	// t = 5010
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)

	sum := metric.sum()
	if sum != 15 {
		t.Errorf("metric.sum() = %v, want %v", sum, 15)
	}
	count := metric.count()
	if count != 5 {
		t.Errorf("metric.count() = %v, want %v", count, 5)
	}
}

func TestContinuousValuesOverwritesAll(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(2)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(3)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(4)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(6)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(7)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(8)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(9)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(10)

	sum := metric.sum()
	if sum != 40 {
		t.Errorf("metric.sum() = %v, want %v", sum, 40)
	}
	count := metric.count()
	if count != 5 {
		t.Errorf("metric.count() = %v, want %v", count, 5)
	}
}

func TestTimeGapInMiddle(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(2)
	clock.now = clock.now.Add(2 * time.Second)
	metric.incBy(4)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)

	sum := metric.sum()
	if sum != 12 {
		t.Errorf("metric.sum() = %v, want %v", sum, 12)
	}
	count := metric.count()
	if count != 4 {
		t.Errorf("metric.count() = %v, want %v", count, 4)
	}
}

func TestWindowRollsOver(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(2)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(3)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(4)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(6)

	sum := metric.sum()
	if sum != 20 {
		t.Errorf("metric.sum() = %v, want %v", sum, 20)
	}
	count := metric.count()
	if count != 5 {
		t.Errorf("metric.count() = %v, want %v", count, 5)
	}
}

func TestWindowRollsOverCompletely(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(2)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(3)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(4)
	clock.now = clock.now.Add(1 * time.Second)
	metric.incBy(5)
	clock.now = clock.now.Add(5 * time.Second)

	sum := metric.sum()
	if sum != 0 {
		t.Errorf("metric.sum() = %v, want %v", sum, 0)
	}
	count := metric.count()
	if count != 0 {
		t.Errorf("metric.count() = %v, want %v", count, 0)
	}
}

func TestMultipleIncrementsSameBucket(t *testing.T) {
	clock := &clock{now: time.UnixMilli(1000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	clock.now = clock.now.Add(500 * time.Millisecond)
	metric.incBy(2)

	sum := metric.sum()
	if sum != 3 {
		t.Errorf("metric.sum() = %v, want %v", sum, 3)
	}
	count := metric.count()
	if count != 2 {
		t.Errorf("metric.count() = %v, want %v", count, 2)
	}
}

func TestTooOldUpdateIgnored(t *testing.T) {
	clock := &clock{now: time.UnixMilli(10000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	metric.incBy(1)
	// t = 4000
	clock.now = clock.now.Add(-1 * 6 * time.Second)
	metric.incBy(1)

	sum := metric.sum()
	if sum != 1 {
		t.Errorf("metric.sum() = %v, want %v", sum, 1)
	}
	count := metric.count()
	if count != 1 {
		t.Errorf("metric.count() = %v, want %v", count, 1)
	}
}

func TestStaleUpdateNoOldBucketsTracked(t *testing.T) {
	clock := &clock{now: time.UnixMilli(10000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	// t = 10000
	metric.incBy(1)
	// t = 8000
	clock.now = clock.now.Add(-1 * 2 * time.Second)
	metric.incBy(1)

	// t = 10000
	clock.now = clock.now.Add(2 * time.Second)
	sum := metric.sum()
	if sum != 2 {
		t.Errorf("metric.sum() = %v, want %v", sum, 2)
	}
	count := metric.count()
	if count != 2 {
		t.Errorf("metric.count() = %v, want %v", count, 2)
	}

	// t = 14000
	clock.now = clock.now.Add(4 * time.Second)
	sum = metric.sum()
	if sum != 1 {
		t.Errorf("metric.sum() = %v, want %v", sum, 1)
	}
	count = metric.count()
	if count != 1 {
		t.Errorf("metric.count() = %v, want %v", count, 1)
	}
}

func TestStaleUpdateWithOldBucketsTracked(t *testing.T) {
	clock := &clock{now: time.UnixMilli(10000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	// t = 10000
	metric.incBy(1)
	clock.now = time.UnixMilli(11000)
	metric.incBy(1)
	clock.now = time.UnixMilli(12000)
	metric.incBy(1)

	// Go back to t = 10000
	clock.now = time.UnixMilli(10000)
	metric.incBy(1)

	// And go back forward to t = 12000
	clock.now = time.UnixMilli(12000)
	sum := metric.sum()
	if sum != 4 {
		t.Errorf("metric.sum() = %v, want %v", sum, 4)
	}
	count := metric.count()
	if count != 4 {
		t.Errorf("metric.count() = %v, want %v", count, 4)
	}

	clock.now = time.UnixMilli(15000)
	sum = metric.sum()
	if sum != 2 {
		t.Errorf("metric.sum() = %v, want %v", sum, 2)
	}
	count = metric.count()
	if count != 2 {
		t.Errorf("metric.count() = %v, want %v", count, 2)
	}
}

func TestZeroMetric(t *testing.T) {
	clock := &clock{now: time.UnixMilli(10000)}
	metric := newWindowedMetric(func() time.Time {
		return clock.now
	}, 5*time.Second, 1*time.Second)

	sum := metric.sum()
	if sum != 0 {
		t.Errorf("metric.sum() = %v, want %v", sum, 0)
	}
	count := metric.count()
	if count != 0 {
		t.Errorf("metric.count() = %v, want %v", count, 0)
	}
}
