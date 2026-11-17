// Copyright 2026 The Grove Authors.
// SPDX-License-Identifier: Apache-2.0

package kueue

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProtectedReplicaClassification(t *testing.T) {
	cases := map[string]struct {
		raw, gang, want string
		base            int32
		invalid         bool
	}{
		"base carries dependencies":               {raw: `{"worker":2}`, gang: "demo-0", want: "false", base: 1},
		"second replica remains protected":        {raw: `{"worker":2}`, gang: "demo-0-worker-0", want: "false", base: 1},
		"third replica is elastic":                {raw: `{"worker":2}`, gang: "demo-0-worker-1", want: "true", base: 1},
		"floor one has elastic first scaled gang": {raw: `{"worker":1}`, gang: "demo-0-worker-0", want: "true", base: 1},
		"multiple replicas in base":               {raw: `{"worker":4}`, gang: "demo-0-worker-0", want: "false", base: 3},
		"offset beyond larger base":               {raw: `{"worker":4}`, gang: "demo-0-worker-1", want: "true", base: 3},
		"independent prefill minimum":             {raw: `{"worker":2,"prefill":3}`, gang: "demo-0-prefill-1", want: "false", base: 1},
		"independent prefill elasticity":          {raw: `{"worker":2,"prefill":3}`, gang: "demo-0-prefill-2", want: "true", base: 1},
		"unknown floor stays protected":           {raw: `{"worker":2}`, gang: "demo-0-prefill-2", want: "false", base: 1},
		"hyphenated group":                        {raw: `{"worker-pool":2}`, gang: "demo-0-worker-pool-1", want: "true", base: 1},
		"later PCS replica":                       {raw: `{"worker":2}`, gang: "demo-12-worker-1", want: "true", base: 1},
		"invalid JSON":                            {raw: `{`, gang: "demo-0", invalid: true},
		"null map":                                {raw: `null`, gang: "demo-0", invalid: true},
		"null floor":                              {raw: `{"worker":null}`, gang: "demo-0", invalid: true},
		"negative floor":                          {raw: `{"worker":-1}`, gang: "demo-0", invalid: true},
		"unknown group":                           {raw: `{"missing":2}`, gang: "demo-0", invalid: true},
		"unknown gang identity":                   {raw: `{"worker":2}`, gang: "demo-0-missing-1", invalid: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pcs := &grovecorev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "ns", Annotations: map[string]string{protectedReplicasAnnotation: tc.raw}}}
			for _, name := range []string{"worker", "worker-pool", "prefill"} {
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = append(pcs.Spec.Template.PodCliqueScalingGroupConfigs, grovecorev1alpha1.PodCliqueScalingGroupConfig{Name: name, MinAvailable: &tc.base})
			}
			pg := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{Name: tc.gang, Namespace: "ns", Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
			wl := newKueueWorkload("ns", tc.gang)
			err := applyPreemptionProtection(wl, pcs, pg)
			if tc.invalid {
				require.Error(t, err)
				require.Empty(t, wl.GetLabels())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, wl.GetLabels()[preemptibleLabel])
		})
	}
}

func TestProtectionLabelUpdatePreservesAdmission(t *testing.T) {
	ctx := context.Background()
	existing := newKueueWorkload("ns", "demo-0-worker-0")
	existing.SetLabels(map[string]string{preemptibleLabel: "true", "unrelated": "preserved"})
	existing.Object["status"] = map[string]any{"admission": map[string]any{"clusterQueue": "org"}}
	desired := existing.DeepCopy()
	desired.SetLabels(map[string]string{preemptibleLabel: "false"})
	cl := testutils.NewTestClientBuilder().WithObjects(existing).Build()
	backend := &schedulerBackend{client: cl}
	require.NoError(t, backend.syncPreemptibleLabel(ctx, existing, desired))
	result := newKueueWorkload("ns", existing.GetName())
	require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(existing), result))
	require.Equal(t, "false", result.GetLabels()[preemptibleLabel])
	require.Equal(t, "preserved", result.GetLabels()["unrelated"])
	admission, _, err := unstructured.NestedString(result.Object, "status", "admission", "clusterQueue")
	require.NoError(t, err)
	require.Equal(t, "org", admission)
	// Removing the configuration must not leave a formerly elastic gang eligible.
	desired.SetLabels(nil)
	existing.SetLabels(map[string]string{preemptibleLabel: "true"})
	require.NoError(t, cl.Update(ctx, existing))
	require.NoError(t, backend.syncPreemptibleLabel(ctx, existing, desired))
	require.Equal(t, "false", existing.GetLabels()[preemptibleLabel])
}
