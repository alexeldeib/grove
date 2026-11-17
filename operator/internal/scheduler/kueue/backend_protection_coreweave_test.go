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
	"k8s.io/utils/ptr"
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

func TestProtectedReplicaFloorChangesPreserveLease(t *testing.T) {
	for name, tc := range map[string]struct {
		before, after, want string
	}{
		"increase": {before: `{"worker":1}`, after: `{"worker":2}`, want: "false"},
		"decrease": {before: `{"worker":2}`, after: `{"worker":1}`, want: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cl, backend, pcs, pg := newResyncFixture(t)
			validator, ok := backend.(*schedulerBackend)
			require.True(t, ok)
			pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
				Name: "worker", CliqueNames: []string{"worker"}, MinAvailable: ptr.To(int32(1)),
			}}
			pcs.Annotations = map[string]string{protectedReplicasAnnotation: tc.before}
			require.NoError(t, cl.Update(ctx, pcs))
			pg.Name = "demo-0-worker-0"
			pg.ResourceVersion = ""
			pg.UID = "scaled-gang-uid"
			// A new PodGang keeps its own name as the Workload identity, not the copied fixture's.
			pg.Annotations[kueueWorkloadNameAnnotation] = pg.Name
			require.NoError(t, cl.Create(ctx, pg))
			require.NoError(t, backend.SyncPodGang(ctx, pg))
			workload := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			workload.SetUID("lease-uid")
			workload.Object["status"] = map[string]any{
				"admission":  map[string]any{"clusterQueue": "queue-a"},
				"conditions": []any{map[string]any{"type": "QuotaReserved", "status": "True"}},
			}
			require.NoError(t, cl.Update(ctx, workload))
			before := workload.DeepCopy()
			previous := pcs.DeepCopy()
			pcs.Annotations[protectedReplicasAnnotation] = tc.after
			require.NoError(t, validator.ValidatePodCliqueSetUpdate(ctx, previous, pcs))
			require.NoError(t, cl.Update(ctx, pcs))
			require.NoError(t, backend.SyncPodGang(ctx, pg))
			after := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			require.Equal(t, tc.want, after.GetLabels()[preemptibleLabel])
			require.Equal(t, before.GetUID(), after.GetUID())
			require.Equal(t, before.Object["spec"], after.Object["spec"])
			require.Equal(t, before.Object["status"], after.Object["status"])
			require.Equal(t, before.GetOwnerReferences(), after.GetOwnerReferences())
			require.NoError(t, backend.SyncPodGang(ctx, pg))
			require.Equal(t, after.GetResourceVersion(), getTestWorkload(t, cl, pg.Namespace, pg.Name).GetResourceVersion())
		})
	}
}

func TestProtectedReplicaFloorUpdateValidation(t *testing.T) {
	for name, raw := range map[string]string{
		"negative": `{"worker":-1}`, "null": `{"worker":null}`, "unknown group": `{"missing":1}`, "malformed": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			_, backend, pcs, _ := newResyncFixture(t)
			validator, ok := backend.(*schedulerBackend)
			require.True(t, ok)
			pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{Name: "worker"}}
			next := pcs.DeepCopy()
			next.Annotations = map[string]string{protectedReplicasAnnotation: raw}
			require.Error(t, backend.ValidatePodCliqueSet(context.Background(), next))
			require.Error(t, validator.ValidatePodCliqueSetUpdate(context.Background(), pcs, next))
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
