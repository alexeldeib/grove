/*
Copyright 2025 The Grove Authors.

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

package pod

import (
	"maps"
	"testing"
)

func TestWithKueuePodGroupMetadata(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		labels      map[string]string
		annotations map[string]string
		wantMapped  bool
	}{
		"queue and total count opt in": {
			labels: map[string]string{
				kueueQueueNameLabel: "test-queue",
				"existing":          "label",
			},
			annotations: map[string]string{
				kueuePodGroupTotalCountAnnotation: "2",
				"existing":                        "annotation",
			},
			wantMapped: true,
		},
		"queue alone preserves existing behavior": {
			labels: map[string]string{kueueQueueNameLabel: "test-queue"},
		},
		"total count alone preserves existing behavior": {
			annotations: map[string]string{kueuePodGroupTotalCountAnnotation: "2"},
		},
		"unrelated workload preserves existing behavior": {},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			const podGangName = "test-pcs-0-test-worker-0"
			originalLabels := maps.Clone(tc.labels)
			originalAnnotations := maps.Clone(tc.annotations)
			labels, annotations := withKueuePodGroupMetadata(tc.labels, tc.annotations, podGangName)

			if got := labels[kueuePodGroupNameLabel]; tc.wantMapped && got != podGangName {
				t.Errorf("PodGroup name = %q, want %q", got, podGangName)
			} else if !tc.wantMapped && got != "" {
				t.Errorf("PodGroup name = %q, want empty", got)
			}
			if got := annotations[kueuePodSetGroupNameAnnotation]; tc.wantMapped && got != podGangName {
				t.Errorf("PodSet group name = %q, want %q", got, podGangName)
			} else if !tc.wantMapped && got != "" {
				t.Errorf("PodSet group name = %q, want empty", got)
			}
			if !maps.Equal(tc.labels, originalLabels) {
				t.Errorf("input labels mutated: got %v, want %v", tc.labels, originalLabels)
			}
			if !maps.Equal(tc.annotations, originalAnnotations) {
				t.Errorf("input annotations mutated: got %v, want %v", tc.annotations, originalAnnotations)
			}
		})
	}
}
