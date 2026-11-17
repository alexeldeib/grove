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
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	"github.com/ai-dynamo/grove/operator/api/common/constants"
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
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCliqueDeletionReleasesServingPodFinalizersOnlyForOwnedPods(t *testing.T) {
	pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "clique", Namespace: "default", UID: "owner", Finalizers: []string{constants.FinalizerPodClique}},
		Spec: grovecorev1alpha1.PodCliqueSpec{PodSpec: corev1.PodSpec{SchedulerName: "kueue"}}}
	owned := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "owned", Namespace: "default",
		Labels:          map[string]string{apicommon.LabelPodClique: pclq.Name},
		Finalizers:      []string{"kueue.x-k8s.io/managed", "example.com/retain"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "PodClique", Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true)}}}}
	sibling := owned.DeepCopy()
	sibling.Name = "sibling"
	sibling.OwnerReferences[0].UID = "other-owner"
	cl := testutils.NewTestClientBuilder().WithObjects(pclq, owned, sibling).
		WithIndex(&corev1.Pod{}, ".metadata.controller.uid", func(obj client.Object) []string {
			owner := metav1.GetControllerOfNoCopy(obj)
			if owner == nil {
				return nil
			}
			return []string{string(owner.UID)}
		}).Build()
	b := kueue.New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	r := Reconciler{client: cl, apiReader: cl, expectationsStore: expect.NewExpectationsStore(), schedRegistry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{"kueue": b}}}
	result := r.triggerDeletionFlow(context.Background(), logr.Discard(), pclq)
	require.False(t, result.HasErrors())
	actual := &corev1.Pod{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(owned), actual))
	assert.Equal(t, []string{"example.com/retain"}, actual.Finalizers)
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(sibling), actual))
	assert.Contains(t, actual.Finalizers, "kueue.x-k8s.io/managed")
	actualClique := &grovecorev1alpha1.PodClique{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(pclq), actualClique))
	assert.Empty(t, actualClique.Finalizers)
}

// The API contains an owned pod (even without its clique label), but the cache
// and per-reconcile memoized lookup have never observed it.
func TestCliqueDeletionInventoriesUncachedServingPodsBeforeClearingExpectations(t *testing.T) {
	pclq, pod, live := servingDeletionFixture()
	cached := &servingDeletionCache{Client: live}
	store := expect.NewExpectationsStore()
	key, err := expect.ControlleeKeyFunc(pclq)
	require.NoError(t, err)
	require.NoError(t, store.ExpectCreations(logr.Discard(), key, pod.UID))
	ctx := componentutils.WithPCLQPodsCache(context.Background())
	cachedPods, err := componentutils.GetPCLQPods(ctx, cached, "", pclq)
	require.NoError(t, err)
	require.Empty(t, cachedPods)
	b := kueue.New(cached, live.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	r := Reconciler{client: cached, apiReader: live, expectationsStore: store,
		schedRegistry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{"kueue": b}}}
	require.False(t, r.triggerDeletionFlow(ctx, logr.Discard(), pclq).HasErrors())
	assertServingDeletionComplete(t, live, pclq, pod)
	assert.Empty(t, store.GetCreateExpectations(key))
	assert.Equal(t, 1, cached.podLists, "deletion must bypass both informer cache and memoized lookup")
}

func TestCliqueDeletionRetainsFinalizerAndExpectationsUntilCleanupSucceeds(t *testing.T) {
	for _, failure := range []string{"inventory", "pod-finalizer-patch"} {
		t.Run(failure, func(t *testing.T) {
			pclq, pod, live := servingDeletionFixture()
			store := expect.NewExpectationsStore()
			key, err := expect.ControlleeKeyFunc(pclq)
			require.NoError(t, err)
			require.NoError(t, store.ExpectCreations(logr.Discard(), key, pod.UID))
			patcher := &servingDeletionPatchClient{Client: live, fail: failure == "pod-finalizer-patch"}
			b := kueue.New(patcher, live.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
			r := Reconciler{client: live, apiReader: live, expectationsStore: store,
				schedRegistry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{"kueue": b}}}
			if failure == "inventory" {
				r.apiReader = servingDeletionReader{Reader: live, fail: true}
			}
			assert.True(t, r.triggerDeletionFlow(context.Background(), logr.Discard(), pclq).HasErrors())
			current := &grovecorev1alpha1.PodClique{}
			require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(pclq), current))
			assert.Contains(t, current.Finalizers, constants.FinalizerPodClique)
			assert.Contains(t, store.GetCreateExpectations(key), pod.UID)
			patcher.fail = false
			r.apiReader = live
			require.False(t, r.triggerDeletionFlow(context.Background(), logr.Discard(), current).HasErrors())
			assertServingDeletionComplete(t, live, pclq, pod)
			assert.Empty(t, store.GetCreateExpectations(key))
		})
	}
}

func TestCliqueDeletionRetriesCreationRecordedDuringInventory(t *testing.T) {
	pclq, pod, live := servingDeletionFixture()
	store := expect.NewExpectationsStore()
	key, err := expect.ControlleeKeyFunc(pclq)
	require.NoError(t, err)
	late := pod.DeepCopy()
	late.Name, late.UID, late.ResourceVersion = "late", "late", ""
	b := kueue.New(live, live.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	r := Reconciler{client: live, expectationsStore: store,
		schedRegistry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{"kueue": b}}}
	r.apiReader = servingDeletionReader{Reader: live, afterList: func() {
		require.NoError(t, live.Create(context.Background(), late))
		require.NoError(t, store.ExpectCreations(logr.Discard(), key, late.UID))
	}}
	result := r.triggerDeletionFlow(context.Background(), logr.Discard(), pclq)
	assert.True(t, result.NeedsRequeue())
	assert.Contains(t, store.GetCreateExpectations(key), late.UID)
	current := &grovecorev1alpha1.PodClique{}
	require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(pclq), current))
	assert.Contains(t, current.Finalizers, constants.FinalizerPodClique)
	r.apiReader = live
	require.False(t, r.triggerDeletionFlow(context.Background(), logr.Discard(), current).HasErrors())
	assertServingDeletionComplete(t, live, pclq, late)
}

func TestCliqueDeletionDoesNotWaitForeverForAlreadyDeletedCreation(t *testing.T) {
	pclq, pod, live := servingDeletionFixture()
	store := expect.NewExpectationsStore()
	key, err := expect.ControlleeKeyFunc(pclq)
	require.NoError(t, err)
	require.NoError(t, store.ExpectCreations(logr.Discard(), key, "already-gone"))
	b := kueue.New(live, live.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	r := Reconciler{client: live, apiReader: live, expectationsStore: store,
		schedRegistry: &testutils.FakeSchedulerRegistry{Backends: map[string]scheduler.Backend{"kueue": b}}}
	require.False(t, r.triggerDeletionFlow(context.Background(), logr.Discard(), pclq).NeedsRequeue())
	assertServingDeletionComplete(t, live, pclq, pod)
	assert.Empty(t, store.GetCreateExpectations(key))
}

func servingDeletionFixture() (*grovecorev1alpha1.PodClique, *corev1.Pod, client.Client) {
	pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "clique", Namespace: "default", UID: "owner", Finalizers: []string{constants.FinalizerPodClique}},
		Spec: grovecorev1alpha1.PodCliqueSpec{PodSpec: corev1.PodSpec{SchedulerName: "kueue"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unseen", Namespace: "default", UID: "unseen",
		Finalizers:      []string{"kueue.x-k8s.io/managed", "example.com/retain"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "PodClique", Name: pclq.Name, UID: pclq.UID, Controller: ptr.To(true)}}}}
	live := testutils.NewTestClientBuilder().WithObjects(pclq, pod).Build()
	return pclq, pod, live
}

func assertServingDeletionComplete(t *testing.T, cl client.Client, pclq *grovecorev1alpha1.PodClique, pod *corev1.Pod) {
	t.Helper()
	currentPod := &corev1.Pod{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(pod), currentPod))
	assert.Equal(t, []string{"example.com/retain"}, currentPod.Finalizers)
	current := &grovecorev1alpha1.PodClique{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(pclq), current))
	assert.NotContains(t, current.Finalizers, constants.FinalizerPodClique)
}

type servingDeletionCache struct {
	client.Client
	podLists int
}

func (c *servingDeletionCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok {
		c.podLists++
		pods.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

type servingDeletionReader struct {
	client.Reader
	fail      bool
	afterList func()
}

func (r servingDeletionReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.fail {
		return fmt.Errorf("inventory unavailable")
	}
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if r.afterList != nil {
		r.afterList()
	}
	return nil
}

type servingDeletionPatchClient struct {
	client.Client
	fail bool
}

func (c *servingDeletionPatchClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.fail {
		return fmt.Errorf("pod patch unavailable")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
