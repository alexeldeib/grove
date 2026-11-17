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

package pod

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/kueue"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestServingRecoveryHoldsOldRanksAcrossControllerRestart(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodFailed, corev1.PodSucceeded} {
		t.Run(string(phase), func(t *testing.T) {
			now := metav1.Now()
			old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "default", UID: "old", DeletionTimestamp: &now,
				Finalizers:  []string{"kueue.x-k8s.io/managed", "example.com/hold"},
				Labels:      map[string]string{"kueue.x-k8s.io/prebuilt-workload-name": "gang"},
				Annotations: map[string]string{"kueue.x-k8s.io/pod-group-serving": "true"}},
				Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: phase}}
			cl := testutils.NewTestClientBuilder().WithObjects(old).Build()
			b := kueue.New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
			r := _resource{client: cl, expectationsStore: expect.NewExpectationsStore()}
			sc := &syncContext{ctx: context.Background(), pclq: &grovecorev1alpha1.PodClique{Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1}},
				existingPCLQPods: []*corev1.Pod{old}, finalizer: b.(scheduler.Finalizer), pclqExpectationsStoreKey: "clique"}
			diff := r.syncExpectationsAndComputeDifference(logr.Discard(), sc)
			require.Equal(t, -1, diff, "empty expectations are reconstructed after operator restart")
			adjusted, waiting := r.withholdUnfinalizedReplacements(sc, diff)
			if phase == corev1.PodRunning {
				assert.True(t, waiting)
				assert.Zero(t, adjusted)
			} else {
				assert.False(t, waiting)
				assert.Equal(t, -1, adjusted)
			}
			require.NoError(t, r.removeSchedulerFinalizersFromDeletingPods(sc))
			actual := &corev1.Pod{}
			require.NoError(t, cl.Get(sc.ctx, client.ObjectKeyFromObject(old), actual))
			assert.Equal(t, []string{"example.com/hold"}, actual.Finalizers)
			// Releasing a finalizer is not evidence that bound old processes stopped.
			adjusted, waiting = r.withholdUnfinalizedReplacements(sc, diff)
			assert.Equal(t, phase == corev1.PodRunning, waiting)
		})
	}
}

func TestServingRecoveryBlocksPartialMultiPodTeardown(t *testing.T) {
	for _, tc := range []struct {
		name        string
		predecessor bool
		deleting    bool
	}{
		{name: "current terminating pod", deleting: true},
		{name: "current pending deletion"},
		{name: "terminating predecessor", predecessor: true, deleting: true},
		{name: "running predecessor", predecessor: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := metav1.Now()
			old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-index-0", Namespace: "default", UID: "old-index-0",
				Labels: map[string]string{"kueue.x-k8s.io/prebuilt-workload-name": "gang"}},
				Spec: corev1.PodSpec{NodeName: "node", Hostname: "clique-0"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			if tc.deleting {
				old.DeletionTimestamp = &now
				old.Finalizers = []string{"example.com/hold"}
			}
			cl := testutils.NewTestClientBuilder().Build()
			b := kueue.New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
			r := _resource{client: cl, expectationsStore: expect.NewExpectationsStore()}
			sc := &syncContext{ctx: context.Background(), pclq: &grovecorev1alpha1.PodClique{Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 2}},
				finalizer: b.(scheduler.Finalizer), pclqExpectationsStoreKey: "clique"}
			if tc.predecessor {
				sc.predecessorPods = []*corev1.Pod{old}
			} else {
				sc.existingPCLQPods = []*corev1.Pod{old}
				if !tc.deleting {
					require.NoError(t, r.expectationsStore.ExpectDeletions(logr.Discard(), "clique", old.UID))
				}
			}
			// Index 1 is already gone, but index 0 still has bound processes.
			// Holding only one of two missing slots would let createPods reuse 0.
			diff := r.syncExpectationsAndComputeDifference(logr.Discard(), sc)
			require.Equal(t, -2, diff)
			adjusted, waiting := r.withholdUnfinalizedReplacements(sc, diff)
			assert.True(t, waiting)
			assert.Zero(t, adjusted, "no replacement may race the old hostname during partial teardown")

			// Once the last old process has stopped, both missing ranks can return.
			old.Status.Phase = corev1.PodFailed
			adjusted, waiting = r.withholdUnfinalizedReplacements(sc, diff)
			assert.False(t, waiting)
			assert.Equal(t, -2, adjusted)
		})
	}
}

func TestServingRecoveryFindsPredecessorWithoutTouchingHealthySibling(t *testing.T) {
	now := metav1.Now()
	newPod := func(name string, ownerUID types.UID, deleting bool) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name),
			Labels:          map[string]string{apicommon.LabelPodClique: "clique", "kueue.x-k8s.io/prebuilt-workload-name": "gang"},
			Finalizers:      []string{"kueue.x-k8s.io/managed", "example.com/hold"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "grove.io/v1alpha1", Kind: "PodClique", Name: "clique", UID: ownerUID, Controller: ptr.To(true)}}},
			Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
		if deleting {
			p.DeletionTimestamp = &now
		}
		return p
	}
	old := newPod("predecessor", "old-clique", true)
	healthy := newPod("healthy", "current-clique", false)
	cl := testutils.NewTestClientBuilder().WithObjects(old, healthy).
		WithIndex(&corev1.Pod{}, componentutils.PodCliqueNameIndexField, func(obj client.Object) []string {
			return []string{obj.GetLabels()[apicommon.LabelPodClique]}
		}).Build()
	b := kueue.New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	r := _resource{client: indexedPredecessorClient{Client: cl, t: t}, expectationsStore: expect.NewExpectationsStore(), schedRegistry: &testutils.FakeSchedulerRegistry{
		Backends: map[string]scheduler.Backend{"kueue": b}, DefaultBackend: "kueue"}}
	sc := &syncContext{ctx: context.Background(), pclq: &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: "clique", Namespace: "default", UID: "current-clique"},
		Spec:       grovecorev1alpha1.PodCliqueSpec{Replicas: 2, PodSpec: corev1.PodSpec{SchedulerName: "kueue"}}},
		existingPCLQPods: []*corev1.Pod{healthy}, pclqExpectationsStoreKey: "clique"}
	require.NoError(t, r.prepareServingRecovery(sc))
	require.Len(t, sc.predecessorPods, 1)
	adjusted, waiting := r.withholdUnfinalizedReplacements(sc, r.syncExpectationsAndComputeDifference(logr.Discard(), sc))
	assert.True(t, waiting)
	assert.Zero(t, adjusted)
	require.NoError(t, r.removeSchedulerFinalizersFromDeletingPods(sc))
	actual := &corev1.Pod{}
	require.NoError(t, cl.Get(sc.ctx, client.ObjectKeyFromObject(healthy), actual))
	assert.Contains(t, actual.Finalizers, "kueue.x-k8s.io/managed")
	require.NoError(t, cl.Get(sc.ctx, client.ObjectKeyFromObject(old), actual))
	assert.Equal(t, []string{"example.com/hold"}, actual.Finalizers)
}

// Reject a regression to a namespace-wide cached scan, even when its label filter
// would return the correct predecessors in a small fixture.
type indexedPredecessorClient struct {
	client.Client
	t *testing.T
}

func (c indexedPredecessorClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		options := (&client.ListOptions{}).ApplyOptions(opts)
		require.NotNil(c.t, options.FieldSelector)
		assert.Equal(c.t, componentutils.PodCliqueNameIndexField+"=clique", options.FieldSelector.String())
		assert.Equal(c.t, "default", options.Namespace)
	}
	return c.Client.List(ctx, list, opts...)
}
