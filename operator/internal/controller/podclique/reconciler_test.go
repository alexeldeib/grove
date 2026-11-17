// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package podclique

import (
	"context"
	"errors"
	"testing"
	"time"

	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGetStatusResyncInterval(t *testing.T) {
	const (
		pcsName   = "pcs"
		namespace = "default"
	)
	// expected values are literals, not constants.PodCliqueStatusResyncInterval, so the test does not
	// pass by construction. A change to the resync interval must consciously update these.
	tests := []struct {
		name             string
		terminationDelay *time.Duration
		breachStatus     metav1.ConditionStatus
		breachAge        time.Duration
		getPCSFails      bool
		expected         time.Duration
	}{
		{
			name:             "short delay leaves scheduling headroom even without a recorded breach",
			terminationDelay: ptr.To(2 * time.Minute),
			expected:         time.Minute,
		},
		{
			name:             "aged breach retries before its remaining deadline",
			terminationDelay: ptr.To(time.Minute),
			breachStatus:     metav1.ConditionTrue,
			breachAge:        55 * time.Second,
			expected:         2500 * time.Millisecond,
		},
		{
			name:             "long policy with an aged breach uses its remaining window",
			terminationDelay: ptr.To(4 * time.Hour),
			breachStatus:     metav1.ConditionTrue,
			breachAge:        4*time.Hour - 20*time.Second,
			expected:         10 * time.Second,
		},
		{
			name:             "elapsed breach retries promptly without hot-looping",
			terminationDelay: ptr.To(time.Minute),
			breachStatus:     metav1.ConditionTrue,
			breachAge:        61 * time.Second,
			expected:         time.Second,
		},
		{
			name:             "cleared breach does not retain its old deadline",
			terminationDelay: ptr.To(time.Minute),
			breachStatus:     metav1.ConditionFalse,
			breachAge:        time.Hour,
			expected:         30 * time.Second,
		},
		{
			name:             "TerminationDelay longer than the resync interval keeps the resync interval",
			terminationDelay: ptr.To(4 * time.Hour),
			expected:         3 * time.Minute,
		},
		{
			name:             "no TerminationDelay falls back to the resync interval",
			terminationDelay: nil,
			expected:         3 * time.Minute,
		},
		{
			name:        "PodCliqueSet fetch failure falls back to the resync interval",
			getPCSFails: true,
			expected:    3 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
			pcsBuilder := testutils.NewPodCliqueSetBuilder(pcsName, namespace, "uid")
			if tt.terminationDelay != nil {
				pcsBuilder.WithTerminationDelay(*tt.terminationDelay)
			}
			pcs := pcsBuilder.Build()
			pclq := testutils.NewPodCliqueBuilder(pcsName, "uid", "clq", namespace, 0).Build()
			if tt.breachStatus != "" {
				pclq.Status.Conditions = []metav1.Condition{{
					Type: apicommonconstants.ConditionTypeMinAvailableBreached, Status: tt.breachStatus,
					LastTransitionTime: metav1.NewTime(now.Add(-tt.breachAge)),
				}}
			}

			clientBuilder := testutils.NewTestClientBuilder().WithObjects(pcs, pclq)
			if tt.getPCSFails {
				clientBuilder.RecordErrorForObjects(testutils.ClientMethodGet,
					apierrors.NewInternalError(errors.New("boom")),
					client.ObjectKey{Name: pcsName, Namespace: namespace})
			}
			r := &Reconciler{client: clientBuilder.Build()}

			actual := r.getStatusResyncInterval(context.Background(), pclq, now)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
