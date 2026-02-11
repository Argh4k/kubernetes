/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package benchmark

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/component-base/featuregate"
	ktesting "k8s.io/kubernetes/test/utils/ktesting"
)



func TestFeatureGatesMerge(t *testing.T) {
	const (
		FeatureA featuregate.Feature = "FeatureA"
		FeatureB featuregate.Feature = "FeatureB"
		FeatureC featuregate.Feature = "FeatureC"
	)

	tests := []struct {
		name      string
		src       map[featuregate.Feature]bool
		overrides map[featuregate.Feature]bool
		want      map[featuregate.Feature]bool
	}{
		{
			name:      "both nil, return empty map",
			src:       nil,
			overrides: nil,
			want:      map[featuregate.Feature]bool{},
		},
		{
			name:      "both empty, return empty map",
			src:       map[featuregate.Feature]bool{},
			overrides: map[featuregate.Feature]bool{},
			want:      map[featuregate.Feature]bool{},
		},
		{
			name:      "nil src, valid overrides",
			src:       nil,
			overrides: map[featuregate.Feature]bool{FeatureA: true},
			want:      map[featuregate.Feature]bool{FeatureA: true},
		},
		{
			name:      "valid src, nil overrides",
			src:       map[featuregate.Feature]bool{FeatureA: true},
			overrides: nil,
			want:      map[featuregate.Feature]bool{FeatureA: true},
		},
		{
			name:      "distinct features merged",
			src:       map[featuregate.Feature]bool{FeatureA: true},
			overrides: map[featuregate.Feature]bool{FeatureB: false},
			want:      map[featuregate.Feature]bool{FeatureA: true, FeatureB: false},
		},
		{
			name:      "overlap with the same value",
			src:       map[featuregate.Feature]bool{FeatureA: true, FeatureB: true},
			overrides: map[featuregate.Feature]bool{FeatureB: true},
			want:      map[featuregate.Feature]bool{FeatureA: true, FeatureB: true},
		},
		{
			name:      "overlap with override (true to false)",
			src:       map[featuregate.Feature]bool{FeatureA: true},
			overrides: map[featuregate.Feature]bool{FeatureA: false},
			want:      map[featuregate.Feature]bool{FeatureA: false},
		},
		{
			name:      "overlap with override (false to true)",
			src:       map[featuregate.Feature]bool{FeatureA: false},
			overrides: map[featuregate.Feature]bool{FeatureA: true},
			want:      map[featuregate.Feature]bool{FeatureA: true},
		},
		{
			name:      "mixed distinct and overlap",
			src:       map[featuregate.Feature]bool{FeatureA: true, FeatureB: true},
			overrides: map[featuregate.Feature]bool{FeatureB: false, FeatureC: true},
			want:      map[featuregate.Feature]bool{FeatureA: true, FeatureB: false, FeatureC: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := featureGatesMerge(tt.src, tt.overrides)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Unexpected featureGatesMerge result (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestApplyThreshold(t *testing.T) {
	tests := []struct {
		name      string
		items     []DataItem
		threshold float64
		selector  thresholdMetricSelector
		wantErr   bool
		errCount  int
	}{
		{
			name:      "no items, should pass",
			items:     []DataItem{},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "TargetMetric",
				DataBucket: "Average",
			},
			wantErr: false,
		},
		{
			name: "zero threshold, should always pass",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "TargetMetric"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 0,
			selector: thresholdMetricSelector{
				Name:        "TargetMetric",
				DataBucket:  "Average",
				ExpectLower: true,
			},
			wantErr: false,
		},
		{
			name: "metric not found in items, should pass (ignored)",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "OtherMetric"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "TargetMetric",
				DataBucket: "Average",
			},
			wantErr: false,
		},
		{
			name: "labels do not match, should pass (ignored)",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "TargetMetric", "plugin": "foo"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "TargetMetric",
				Labels:     map[string]string{"plugin": "bar"},
				DataBucket: "Average",
			},
			wantErr: false,
		},
		{
			name: "labels match, value lower than threshold (ExpectLower=false), should fail",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "TargetMetric", "plugin": "foo"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "TargetMetric",
				Labels:     map[string]string{"plugin": "foo"},
				DataBucket: "Average",
			},
			wantErr: true,
		},
		{
			name: "missing data bucket in item, should fail",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "TargetMetric"},
					Data:   map[string]float64{"Average": 100},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "TargetMetric",
				DataBucket: "99Perc",
			},
			wantErr: true,
		},
		{
			name: "value higher than threshold (ExpectLower=false), should pass",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Throughput"},
					Data:   map[string]float64{"Average": 100},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:        "Throughput",
				DataBucket:  "Average",
				ExpectLower: false,
			},
			wantErr: false,
		},
		{
			name: "value lower than threshold (ExpectLower=false), should fail",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Throughput"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:        "Throughput",
				DataBucket:  "Average",
				ExpectLower: false,
			},
			wantErr: true,
		},
		{
			name: "value lower than threshold (ExpectLower=true), should pass",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Latency"},
					Data:   map[string]float64{"Average": 10},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:        "Latency",
				DataBucket:  "Average",
				ExpectLower: true,
			},
			wantErr: false,
		},
		{
			name: "value higher than threshold (ExpectLower=true), should fail",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Latency"},
					Data:   map[string]float64{"Average": 100},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:        "Latency",
				DataBucket:  "Average",
				ExpectLower: true,
			},
			wantErr: true,
		},
		{
			name: "value exactly equals threshold, should fail",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Throughput"},
					Data:   map[string]float64{"Average": 50},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "Throughput",
				DataBucket: "Average",
			},
			wantErr: true,
		},
		{
			name: "multiple items failing threshold, should return joined error",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Throughput", "ID": "1"},
					Data:   map[string]float64{"Average": 10},
				},
				{
					Labels: map[string]string{"Metric": "Throughput", "ID": "2"},
					Data:   map[string]float64{"Average": 20},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:       "Throughput",
				DataBucket: "Average",
			},
			wantErr:  true,
			errCount: 2,
		},
		{
			name: "multiple items failing threshold (ExpectLower=true), should return joined error",
			items: []DataItem{
				{
					Labels: map[string]string{"Metric": "Throughput", "ID": "1"},
					Data:   map[string]float64{"Average": 65},
				},
				{
					Labels: map[string]string{"Metric": "Throughput", "ID": "2"},
					Data:   map[string]float64{"Average": 75},
				},
			},
			threshold: 50,
			selector: thresholdMetricSelector{
				Name:        "Throughput",
				DataBucket:  "Average",
				ExpectLower: true,
			},
			wantErr:  true,
			errCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyThreshold(tt.items, tt.threshold, tt.selector)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Expected no error in applyThreshold, but got: %v", err)
				}
				if tt.errCount > 0 {
					if u, ok := err.(interface{ Unwrap() []error }); ok {
						if len(u.Unwrap()) != tt.errCount {
							t.Errorf("Expected %d errors, got %d", tt.errCount, len(u.Unwrap()))
						}
					} else {
						t.Errorf("Expected joined error with %d errors, got type %T: %v", tt.errCount, err, err)
					}
				}
			} else {
				if tt.wantErr {
					t.Errorf("Expected error %v in applyThreshold, but got nil", tt.wantErr)
				}
			}
		})
	}
}

// mockDataCollector always returns the same data items, to be used for mocking data collector in unit tests.
type mockDataCollector struct {
	dataItems []DataItem
}

// init does nothing.
func (mc *mockDataCollector) init() error {
	return nil
}

// run does nothing.
func (mc *mockDataCollector) run(_ ktesting.TContext) {}

// collect always returns DataItems defined in the collector.
func (mc *mockDataCollector) collect() []DataItem {
	return mc.dataItems
}

func TestMetricThreshold(t *testing.T) {
	testCases := []struct {
		name                                  string
		thresholdValue                        float64
		dataItems                             []DataItem
		thresholdMetricSelector               *thresholdMetricSelector
		expectCollectionFailure               bool
		expectedDataItemsWithThresholdIndices []int
		expectedThresholdName                 string
	}{
		{
			name:           "value is above threshold, no error",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 150,
					},
					Labels: map[string]string{
						"Metric": "throughput",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Average",
			},
			expectedDataItemsWithThresholdIndices: []int{0},
			expectedThresholdName:                 "AverageThreshold",
		},
		{
			name:           "value is below threshold, expect error",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 70,
						"Max":     90,
					},
					Labels: map[string]string{
						"Metric": "throughput",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Max",
			},
			expectCollectionFailure:               true,
			expectedDataItemsWithThresholdIndices: []int{0},
			expectedThresholdName:                 "MaxThreshold",
		},
		{
			name:           "no error if the labels do not match",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 70,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Average",
				Labels: map[string]string{
					"label": "value2",
				},
			},
			expectedDataItemsWithThresholdIndices: []int{},
			expectedThresholdName:                 "AverageThreshold",
		},
		{
			name:           "out of multiple data items only matching are selected",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 70,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value",
					},
				},
				{
					Data: map[string]float64{
						"Average": 150,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value2",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Average",
				Labels: map[string]string{
					"label": "value2",
				},
			},
			expectedDataItemsWithThresholdIndices: []int{1},
			expectedThresholdName:                 "AverageThreshold",
		},
		{
			name:           "threshold value is added for all matching entries",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 130,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value",
					},
				},
				{
					Data: map[string]float64{
						"Average": 150,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value2",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Average",
			},
			expectedDataItemsWithThresholdIndices: []int{0, 1},
			expectedThresholdName:                 "AverageThreshold",
		},
		{
			name:           "threshold value is added for all matching entries even with error",
			thresholdValue: 100,
			dataItems: []DataItem{
				{
					Data: map[string]float64{
						"Average": 70,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value",
					},
				},
				{
					Data: map[string]float64{
						"Average": 80,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value2",
					},
				},
				{
					Data: map[string]float64{
						"Average": 130,
					},
					Labels: map[string]string{
						"Metric": "throughput",
						"label":  "value3",
					},
				},
			},
			thresholdMetricSelector: &thresholdMetricSelector{
				Name:       "throughput",
				DataBucket: "Average",
			},
			expectCollectionFailure:               true,
			expectedDataItemsWithThresholdIndices: []int{0, 1, 2},
			expectedThresholdName:                 "AverageThreshold",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, tCtx := ktesting.NewTestContext(t)
			var capturedErr error
			capturingCtx, finalize := tCtx.WithError(&capturedErr)
			defer finalize()

			originalGetTestDataCollectors := getTestDataCollectors
			defer func() { getTestDataCollectors = originalGetTestDataCollectors }()
			getTestDataCollectors = func(_ coreinformers.PodInformer, _ string, _ []string, _ map[string]string, _ *metricsCollectorConfig, _ float64) []testDataCollector {
				return []testDataCollector{&mockDataCollector{dataItems: tc.dataItems}}
			}

			workload := &workload{
				Name: "some/workload",
				Threshold: thresholds{
					valuesByTopic: map[string]float64{"example": tc.thresholdValue},
				},
				ThresholdMetricSelector: tc.thresholdMetricSelector,
			}
			exec := &WorkloadExecutor{
				topicName:                    "example",
				testCase:                     &testCase{},
				tCtx:                         capturingCtx,
				numPodsScheduledPerNamespace: make(map[string]int),
				workload:                     workload,
			}

			start := &startCollectingMetricsOp{
				Opcode:     startCollectingMetricsOpcode,
				Name:       "test-collection",
				Namespaces: []string{"test-namespaces"},
			}
			err := exec.runOp(start, 0)
			if err != nil {
				t.Fatalf("Failed to start metric collection")
			}
			stop := &stopCollectingMetricsOp{Opcode: stopCollectingMetricsOpcode}
			err = exec.runOp(stop, 0)
			if err != nil {
				t.Fatalf("Failed to stop metric collection")
			}

			if tc.expectCollectionFailure != capturingCtx.Failed() {
				t.Fatalf("expectCollectionFailure=%v but got %v", tc.expectCollectionFailure, capturingCtx.Failed())
			}
			for _, idx := range tc.expectedDataItemsWithThresholdIndices {
				if idx >= len(exec.dataItems) {
					t.Fatalf("expectedDataItemsWithThresholdIndex out of data items range")
				}
				if _, ok := exec.dataItems[idx].Data[tc.expectedThresholdName]; !ok {
					t.Fatalf("expected data item at index=%d to have %s field", idx, tc.expectedThresholdName)
				}
			}
		})
	}
}
