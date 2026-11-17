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

package kueue

import (
	"context"
	"fmt"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLifecycleUpgradeReplacesLegacyAndServingMembersConsistently(t *testing.T) {
	for _, mode := range []string{podGroupLifecycleLegacy, podGroupLifecycleServing} {
		t.Run(mode, func(t *testing.T) {
			cl, initial, pcs, pg := newResyncFixture(t)
			require.NoError(t, initial.SyncPodGang(t.Context(), pg))
			wl := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			assert.Equal(t, podGroupLifecycleServing, wl.GetAnnotations()[podGroupLifecycleAnnotation], "new empty groups activate serving semantics")
			annotations := wl.GetAnnotations()
			delete(annotations, podGroupLifecycleAnnotation) // Workload created before this upgrade.
			wl.SetAnnotations(annotations)
			require.NoError(t, cl.Update(t.Context(), wl))
			for _, name := range []string{"rank-0", "rank-1"} {
				pod := lifecycleMember(name, pg.Namespace, pg.Name, mode)
				require.NoError(t, cl.Create(t.Context(), pod))
			}
			// Ordinary replacement after an operator-only upgrade: rank 1 is gone,
			// while rank 0 still carries its original lifecycle annotations.
			require.NoError(t, cl.Delete(t.Context(), lifecycleMember("rank-1", pg.Namespace, pg.Name, mode)))
			backend := New(lifecycleCacheClient{Client: cl}, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
			require.NoError(t, backend.Init(cl))
			preparation := scheduler.PodPreparationContext{PodCliqueSet: pcs, PodClique: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "demo-0-worker", Namespace: pg.Namespace}}, PodGang: pg, PodGangName: pg.Name}
			replacement := lifecycleMember("replacement", pg.Namespace, pg.Name, "opposite-template-mode")
			replacement.Annotations[podGroupServingAnnotation] = "true"
			require.NoError(t, backend.PreparePod(replacement, preparation))
			assertLifecycle(t, replacement, mode)
			require.NoError(t, cl.Create(t.Context(), replacement))
			survivor := &corev1.Pod{}
			require.NoError(t, cl.Get(t.Context(), client.ObjectKey{Namespace: pg.Namespace, Name: "rank-0"}, survivor))
			assertLifecycle(t, survivor, mode)
			assert.Equal(t, mode, getTestWorkload(t, cl, pg.Namespace, pg.Name).GetAnnotations()[podGroupLifecycleAnnotation])

			// Losing every member and restarting the operator must not change a
			// lease's recorded mode, nor may ordinary Workload sync promote it.
			require.NoError(t, cl.Delete(t.Context(), survivor))
			require.NoError(t, cl.Delete(t.Context(), replacement))
			restarted := New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
			require.NoError(t, restarted.Init(cl))
			require.NoError(t, restarted.SyncPodGang(t.Context(), pg))
			require.NoError(t, restarted.PreparePod(replacement, preparation))
			assertLifecycle(t, replacement, mode)
		})
	}
}

func TestLifecycleMigrationEmptyLeaseAndMixedGroups(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint("mixed=", mixed), func(t *testing.T) {
			cl, backend, _, pg := newResyncFixture(t)
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))
			wl := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			annotations := wl.GetAnnotations()
			delete(annotations, podGroupLifecycleAnnotation)
			wl.SetAnnotations(annotations)
			require.NoError(t, cl.Update(t.Context(), wl))
			if mixed {
				for _, mode := range []string{podGroupLifecycleLegacy, podGroupLifecycleServing} {
					require.NoError(t, cl.Create(t.Context(), lifecycleMember(mode, pg.Namespace, pg.Name, mode)))
				}
			}
			b := backend.(*schedulerBackend)
			mode, err := b.podGroupLifecycle(t.Context(), pg)
			if mixed {
				require.ErrorContains(t, err, "mixed pod lifecycle modes")
				assert.Empty(t, getTestWorkload(t, cl, pg.Namespace, pg.Name).GetAnnotations()[podGroupLifecycleAnnotation])
			} else {
				require.NoError(t, err)
				assert.Equal(t, podGroupLifecycleLegacy, mode, "an empty existing lease is not a new activation")
			}
		})
	}
}

func TestNewLeasePreservesTerminalLegacyMembers(t *testing.T) {
	cl, backend, _, pg := newResyncFixture(t)
	pod := lifecycleMember("terminal", pg.Namespace, pg.Name, podGroupLifecycleLegacy)
	pod.Status.Phase = corev1.PodFailed
	require.NoError(t, cl.Create(t.Context(), pod))
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	assert.Equal(t, podGroupLifecycleLegacy, getTestWorkload(t, cl, pg.Namespace, pg.Name).GetAnnotations()[podGroupLifecycleAnnotation])
}

func TestLifecycleMigrationFailsClosed(t *testing.T) {
	for _, failure := range []string{"missing", "foreign", "deleting", "unknown", "inventory", "persist"} {
		t.Run(failure, func(t *testing.T) {
			cl, backend, _, pg := newResyncFixture(t)
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))
			wl := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			annotations := wl.GetAnnotations()
			delete(annotations, podGroupLifecycleAnnotation)
			if failure == "unknown" {
				annotations[podGroupLifecycleAnnotation] = "typo"
			}
			wl.SetAnnotations(annotations)
			if failure == "foreign" {
				owners := wl.GetOwnerReferences()
				owners[0].UID = "other-gang"
				wl.SetOwnerReferences(owners)
			}
			if failure == "deleting" {
				wl.SetFinalizers([]string{"example.com/hold"})
			}
			require.NoError(t, cl.Update(t.Context(), wl))
			if failure == "missing" || failure == "deleting" {
				require.NoError(t, cl.Delete(t.Context(), wl, &client.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))}))
			}
			failing := lifecycleFailureClient{Client: cl, failure: failure}
			b := New(failing, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue}).(*schedulerBackend)
			require.NoError(t, b.Init(failing))
			mode, err := b.podGroupLifecycle(t.Context(), pg)
			require.Error(t, err)
			assert.Empty(t, mode, "failed migration must not authorize pod creation")
		})
	}
}

func lifecycleMember(name, ns, gang, mode string) *corev1.Pod {
	annotations := map[string]string{retriableInGroupAnnotation: "false"}
	if mode == podGroupLifecycleServing {
		annotations = map[string]string{podGroupServingAnnotation: "true"}
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{podGroupNameLabel: gang, prebuiltWorkloadNameLabel: gang}, Annotations: annotations},
		Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
}

func assertLifecycle(t *testing.T, pod *corev1.Pod, mode string) {
	t.Helper()
	if mode == podGroupLifecycleServing {
		assert.Equal(t, "true", pod.Annotations[podGroupServingAnnotation])
		assert.NotContains(t, pod.Annotations, retriableInGroupAnnotation)
	} else {
		assert.NotContains(t, pod.Annotations, podGroupServingAnnotation)
		assert.Equal(t, "false", pod.Annotations[retriableInGroupAnnotation])
	}
}

// Model an informer that hasn't observed any members; writes still reach the API.
type lifecycleCacheClient struct{ client.Client }

func (c lifecycleCacheClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if pods, ok := list.(*corev1.PodList); ok {
		pods.Items = nil
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

// Match the deployed Workload role: update is allowed, patch is not.
func (c lifecycleCacheClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if obj.GetObjectKind().GroupVersionKind() == workloadGVK {
		return apierrors.NewForbidden(schema.GroupResource{Group: workloadGVK.Group, Resource: "workloads"}, obj.GetName(), fmt.Errorf("Workload patch is not allowed"))
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestLifecycleUpdateConflictRetriesWithoutLosingConcurrentMetadata(t *testing.T) {
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	wl := getTestWorkload(t, cl, pg.Namespace, pg.Name)
	annotations := wl.GetAnnotations()
	delete(annotations, podGroupLifecycleAnnotation)
	wl.SetAnnotations(annotations)
	require.NoError(t, cl.Update(t.Context(), wl))
	failing := &lifecycleConflictClient{Client: cl}
	b := backend.(*schedulerBackend)
	b.client = failing
	require.NoError(t, b.Init(failing))
	mode, err := b.podGroupLifecycle(t.Context(), pg)
	require.True(t, apierrors.IsConflict(err), "stale resource version must not authorize replacement")
	assert.Empty(t, mode)
	current := getTestWorkload(t, cl, pg.Namespace, pg.Name)
	assert.Empty(t, current.GetAnnotations()[podGroupLifecycleAnnotation])
	assert.Equal(t, "retained", current.GetAnnotations()["example.com/concurrent"])
	mode, err = b.podGroupLifecycle(t.Context(), pg)
	require.NoError(t, err)
	assert.Equal(t, podGroupLifecycleLegacy, mode)
	current = getTestWorkload(t, cl, pg.Namespace, pg.Name)
	assert.Equal(t, podGroupLifecycleLegacy, current.GetAnnotations()[podGroupLifecycleAnnotation])
	assert.Equal(t, "retained", current.GetAnnotations()["example.com/concurrent"])
}

type lifecycleConflictClient struct {
	client.Client
	changed bool
}

func (c *lifecycleConflictClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if !c.changed {
		c.changed = true
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(workloadGVK)
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			return err
		}
		annotations := current.GetAnnotations()
		annotations["example.com/concurrent"] = "retained"
		current.SetAnnotations(annotations)
		if err := c.Client.Update(ctx, current); err != nil {
			return err
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

type lifecycleFailureClient struct {
	client.Client
	failure string
}

func (c lifecycleFailureClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.failure == "inventory" {
		return fmt.Errorf("API inventory unavailable")
	}
	return c.Client.List(ctx, list, opts...)
}

func (c lifecycleFailureClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.failure == "persist" {
		return fmt.Errorf("lifecycle persistence unavailable")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c lifecycleFailureClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if c.failure == "persist" {
		return fmt.Errorf("lifecycle persistence unavailable")
	}
	return c.Client.Update(ctx, obj, opts...)
}
