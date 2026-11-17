// Copyright 2026 The Grove Authors.
// SPDX-License-Identifier: Apache-2.0

package podcliquescalinggroup

import (
	"context"
	"testing"
	"time"

	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestScalingGroupStatusResyncInterval(t *testing.T) {
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct{ age, want time.Duration }{
		"fresh":         {0, 30 * time.Second},
		"aged":          {55 * time.Second, 2500 * time.Millisecond},
		"near deadline": {59900 * time.Millisecond, time.Second},
		"expired":       {time.Minute, time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("pcs", "test", "uid").WithTerminationDelay(time.Minute).Build()
			pcsg := testutils.NewPodCliqueScalingGroupBuilder("pcs-0-workers", "test", "pcs", 0).Build()
			pcsg.Status.Conditions = []metav1.Condition{{Type: apiconstants.ConditionTypeMinAvailableBreached,
				Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-tc.age))}}
			r := Reconciler{client: testutils.NewTestClientBuilder().WithObjects(pcs, pcsg).Build()}
			if got := r.getStatusResyncInterval(context.Background(), pcsg, now); got != tc.want {
				t.Fatalf("interval = %s, want %s", got, tc.want)
			}
		})
	}
}
