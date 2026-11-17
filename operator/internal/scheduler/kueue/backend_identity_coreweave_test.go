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
	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reproduce the scale-down/up race: an admitted Workload and its old running member outlive
// their PodGang, while a new PodGang reuses the name. Pod preparation may precede SyncPodGang.
func TestRecreatedPodGangIsolatesLingeringWorkload(t *testing.T) {
	cl, backend, pcs, old := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), old))
	setWorkloadCondition(t, cl, old.Namespace, old.Name, "QuotaReserved")
	prior := getTestWorkload(t, cl, old.Namespace, old.Name)
	require.NoError(t, cl.Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "old-member", Namespace: old.Namespace,
			Labels: enrolledPodLabels(pcs.Name, old.Name)},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}))
	require.NoError(t, cl.Delete(t.Context(), old))
	replacement := old.DeepCopy()
	replacement.UID = "replacement-podgang-uid"
	replacement.ResourceVersion = ""
	replacement.Annotations = nil
	require.NoError(t, cl.Create(t.Context(), replacement))
	stale := replacement.DeepCopy()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: old.Namespace}}
	preparation := scheduler.PodPreparationContext{
		PodGang: stale, PodGangName: stale.Name, PodCliqueSet: pcs,
		PodClique: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "demo-0-worker"}},
	}
	name := "grove-" + string(replacement.UID)
	requirePreparationAwaitsWorkload(t, backend.PreparePod(pod, preparation), name)
	assert.Equal(t, name, pod.Labels[prebuiltWorkloadNameLabel])
	assert.Equal(t, name, pod.Labels[podGroupNameLabel])
	require.NoError(t, backend.SyncPodGang(t.Context(), stale))
	pod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: old.Namespace}}
	require.NoError(t, backend.PreparePod(pod, preparation))
	assert.Equal(t, name, pod.Labels[prebuiltWorkloadNameLabel])
	current := getTestWorkload(t, cl, old.Namespace, name)
	assert.True(t, workloadOwnedByPodGang(current, replacement))
	assert.Equal(t, prior.Object, getTestWorkload(t, cl, old.Namespace, old.Name).Object,
		"old quota and placement must remain intact while the old member runs")

	// The annotation makes the choice sticky even when the original Workload is garbage-collected
	// and the caller still holds an informer snapshot from before the annotation was written.
	require.NoError(t, cl.Delete(t.Context(), prior))
	got, err := backend.(*schedulerBackend).workloadName(t.Context(), stale)
	require.NoError(t, err)
	assert.Equal(t, name, got)
	require.NoError(t, backend.SyncPodGang(t.Context(), stale))
	getTestWorkload(t, cl, old.Namespace, name)
}

func TestWorkloadIdentityUsesLivePodGang(t *testing.T) {
	cl, backend, _, pg := newResyncFixture(t)
	live := pg.DeepCopy()
	live.Annotations = map[string]string{kueueWorkloadNameAnnotation: "grove-persisted-uid"}
	direct := testutils.NewTestClientBuilder().WithObjects(live).Build()
	concrete := backend.(*schedulerBackend)
	concrete.directClient = direct
	name, err := concrete.workloadName(t.Context(), pg)
	require.NoError(t, err)
	assert.Equal(t, "grove-persisted-uid", name)

	// A stale reconcile must not stamp an annotation onto a different live incarnation.
	stored := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stored))
	stored.UID = "different-uid"
	concrete.directClient = testutils.NewTestClientBuilder().WithObjects(stored).Build()
	_, err = concrete.workloadName(t.Context(), pg)
	require.ErrorContains(t, err, "no longer the live incarnation")
}

func TestReplacementWorkloadRetainsAdmissionSafetyChecks(t *testing.T) {
	cl, backend, pcs, pg := newResyncFixture(t)
	pg.Annotations = map[string]string{kueueWorkloadNameAnnotation: "grove-persisted-uid"}
	require.NoError(t, cl.Update(t.Context(), pg))
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	labels := enrolledPodLabels(pcs.Name, pg.Name)
	labels[podGroupNameLabel] = "grove-persisted-uid"
	labels[prebuiltWorkloadNameLabel] = "grove-persisted-uid"
	require.NoError(t, cl.Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "member", Namespace: pg.Namespace, Labels: labels},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}))
	setWorkloadCondition(t, cl, pg.Namespace, "grove-persisted-uid", "Finished")
	require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "non-terminal Pods")
	getTestWorkload(t, cl, pg.Namespace, "grove-persisted-uid")
}

func TestBaseReleasesReplacementSiblingWorkload(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	prior := getTestWorkload(t, cl, tail.Namespace, tail.Name)
	require.NoError(t, cl.Delete(t.Context(), tail))
	tail.UID = "replacement-tail-uid"
	tail.ResourceVersion = ""
	tail.Annotations = nil
	require.NoError(t, cl.Create(t.Context(), tail))
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	name := "grove-" + string(tail.UID)
	assert.Contains(t, workloadAdmissionGates(getTestWorkload(t, cl, tail.Namespace, name)), basePodGangAdmissionGate)
	setBaseScheduled(t, cl, base.Namespace, 1)
	reSync(t, cl, backend, base)
	assert.NotContains(t, workloadAdmissionGates(getTestWorkload(t, cl, tail.Namespace, name)), basePodGangAdmissionGate)
	assert.Equal(t, prior.Object, getTestWorkload(t, cl, tail.Namespace, tail.Name).Object)
}

// Hooks model informer lag and precise API interleavings without sleeps.
type identityTestClient struct {
	create func(context.Context, client.Object, ...client.CreateOption) error
	client.Client
	get   func(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error
	list  func(context.Context, client.ObjectList, ...client.ListOption) error
	patch func(context.Context, client.Object, client.Patch, ...client.PatchOption) error
}

func (c *identityTestClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.get != nil {
		return c.get(ctx, key, obj, opts...)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *identityTestClient) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	if c.list != nil {
		return c.list(ctx, obj, opts...)
	}
	return c.Client.List(ctx, obj, opts...)
}
func (c *identityTestClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.patch != nil {
		return c.patch(ctx, obj, patch, opts...)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func identityPreparation(pcs *grovecorev1alpha1.PodCliqueSet, pg *groveschedulerv1alpha1.PodGang) scheduler.PodPreparationContext {
	return scheduler.PodPreparationContext{PodCliqueSet: pcs, PodGang: pg, PodGangName: pg.Name,
		PodClique: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "demo-0-worker"}}}
}

func TestAnnotatedStalePodGangCannotPreparePods(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprintf("terminating=%v", terminating), func(t *testing.T) {
			cl, backend, pcs, pg := newResyncFixture(t)
			stale := pg.DeepCopy()
			stale.Annotations = map[string]string{kueueWorkloadNameAnnotation: "grove-stale"}
			live := pg.DeepCopy()
			if terminating {
				live.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				live.Finalizers = []string{"test/hold"}
			} else {
				live.UID = "new-incarnation"
			}
			backend.(*schedulerBackend).directClient = testutils.NewTestClientBuilder().WithObjects(live).Build()
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
			require.Error(t, backend.PreparePod(pod, identityPreparation(pcs, stale)))
			assert.Empty(t, pod.Labels[prebuiltWorkloadNameLabel])
			assert.Empty(t, pod.Labels[podGroupNameLabel])
			stored := &groveschedulerv1alpha1.PodGang{}
			require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stored))
			assert.Equal(t, pg.Annotations[kueueWorkloadNameAnnotation], stored.Annotations[kueueWorkloadNameAnnotation])
		})
	}
}

func TestConcurrentLegacySelectionConvergesOnPersistedWinner(t *testing.T) {
	cl, backend, pcs, pg := newResyncFixture(t)
	clearIdentity(t, cl, pg)
	legacy := newKueueWorkload(pg.Namespace, pg.Name)
	legacy.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: groveschedulerv1alpha1.SchemeGroupVersion.String(), Kind: "PodGang", Name: pg.Name, UID: "old-uid"}})
	require.NoError(t, cl.Create(t.Context(), legacy))
	winner := "grove-" + string(pg.UID)
	raced := false
	patches := 0
	wrapped := &identityTestClient{Client: cl}
	wrapped.get = func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*unstructured.Unstructured); ok && key.Name == pg.Name && !raced {
			raced = true
			live := &groveschedulerv1alpha1.PodGang{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pg), live))
			live.Annotations = map[string]string{kueueWorkloadNameAnnotation: winner}
			require.NoError(t, cl.Update(ctx, live))
			require.NoError(t, cl.Delete(ctx, legacy))
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	wrapped.patch = func(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		patches++
		return cl.Patch(ctx, obj, p, opts...)
	}
	concrete := backend.(*schedulerBackend)
	concrete.client = wrapped
	concrete.directClient = wrapped
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
	requirePreparationAwaitsWorkload(t, backend.PreparePod(pod, identityPreparation(pcs, pg)), winner)
	assert.True(t, raced)
	assert.Equal(t, 1, patches, "losing legacy-name write must conflict and retry against the winner")
	assert.Equal(t, winner, pod.Labels[prebuiltWorkloadNameLabel])
	assert.Equal(t, winner, pod.Labels[podGroupNameLabel])
}

func TestWorkloadIdentityPreservesOnlyOwnedLegacyNames(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			cl, backend, _, pg := newResyncFixture(t)
			clearIdentity(t, cl, pg)
			if existing {
				wl := newKueueWorkload(pg.Namespace, pg.Name)
				wl.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: groveschedulerv1alpha1.SchemeGroupVersion.String(), Kind: "PodGang", Name: pg.Name, UID: pg.UID}})
				require.NoError(t, cl.Create(t.Context(), wl))
			}
			name, err := backend.(*schedulerBackend).workloadName(t.Context(), pg)
			require.NoError(t, err)
			expected := "grove-" + string(pg.UID)
			if existing {
				expected = pg.Name
			}
			assert.Equal(t, expected, name)
			live := &groveschedulerv1alpha1.PodGang{}
			require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), live))
			assert.Equal(t, name, live.Annotations[kueueWorkloadNameAnnotation])
		})
	}
}

func TestRemovedPodGangOwnerReference(t *testing.T) {
	for _, mode := range []string{"surviving-pod", "provenance", "unknown", "wrong-pod-uid", "foreign-pod", "pod-read-error", "persisted-unknown"} {
		t.Run(mode, func(t *testing.T) {
			cl, backend, pcs, pg := newResyncFixture(t)
			clearIdentity(t, cl, pg)
			legacy := newKueueWorkload(pg.Namespace, pg.Name)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-member", Namespace: pg.Namespace, UID: "old-pod-uid", Labels: enrolledPodLabels(pcs.Name, pg.Name)}}
			if mode != "unknown" && mode != "persisted-unknown" && mode != "provenance" {
				require.NoError(t, cl.Create(t.Context(), pod))
				legacy.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}})
			}
			if mode == "wrong-pod-uid" {
				owners := legacy.GetOwnerReferences()
				owners[0].UID = "deleted-pod-uid"
				legacy.SetOwnerReferences(owners)
			}
			if mode == "foreign-pod" {
				pod.Labels[apicommon.LabelManagedByKey] = "another-controller"
				require.NoError(t, cl.Update(t.Context(), pod))
			}
			if mode == "provenance" {
				legacy.SetAnnotations(map[string]string{kueuePodGangNameAnnotation: pg.Name, kueuePodGangUIDAnnotation: "old-podgang-uid"})
			}
			require.NoError(t, unstructured.SetNestedSlice(legacy.Object, []any{map[string]any{"type": "QuotaReserved", "status": "True"}}, "status", "conditions"))
			require.NoError(t, cl.Create(t.Context(), legacy))
			before := getTestWorkload(t, cl, pg.Namespace, pg.Name)
			if mode == "persisted-unknown" {
				pg.Annotations = map[string]string{kueueWorkloadNameAnnotation: pg.Name}
				require.NoError(t, cl.Update(t.Context(), pg))
			}
			if mode == "pod-read-error" {
				backend.(*schedulerBackend).directClient = &identityTestClient{Client: cl, get: func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*corev1.Pod); ok {
						return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, key.Name, fmt.Errorf("denied"))
					}
					return cl.Get(ctx, key, obj, opts...)
				}}
			}
			replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
			err := backend.PreparePod(replacement, identityPreparation(pcs, pg))
			if mode == "surviving-pod" || mode == "provenance" {
				selected := "grove-" + string(pg.UID)
				requirePreparationAwaitsWorkload(t, err, selected)
				assert.Equal(t, selected, replacement.Labels[prebuiltWorkloadNameLabel])
				require.NoError(t, backend.SyncPodGang(t.Context(), pg))
				replacement = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
				require.NoError(t, backend.PreparePod(replacement, identityPreparation(pcs, pg)))
				assert.Equal(t, selected, replacement.Labels[prebuiltWorkloadNameLabel])
			} else {
				require.Error(t, err)
				assert.Empty(t, replacement.Labels[prebuiltWorkloadNameLabel])
				if mode == "pod-read-error" {
					assert.True(t, apierrors.IsForbidden(err))
				}
			}
			assert.Equal(t, before.Object, getTestWorkload(t, cl, pg.Namespace, pg.Name).Object)
		})
	}
}

func TestBaseSkipsGoneSiblingsAndPreservesRealErrors(t *testing.T) {
	for _, mode := range []string{"missing", "terminating", "read-error", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			cl, backend, base, tail := newOrderingFixture(t, 1)
			if mode == "terminating" {
				require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(tail), tail))
				tail.Finalizers = []string{"test/hold"}
				require.NoError(t, cl.Update(t.Context(), tail))
				require.NoError(t, cl.Delete(t.Context(), tail))
			}
			if mode == "missing" {
				require.NoError(t, cl.Delete(t.Context(), tail))
			}
			cached := &identityTestClient{Client: cl, list: func(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				if gangs, ok := list.(*groveschedulerv1alpha1.PodGangList); ok && mode == "missing" {
					gangs.Items = append(gangs.Items, *tail.DeepCopy())
				}
				return nil
			}}
			backend.client = cached
			backend.directClient = &identityTestClient{Client: cl, get: func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*groveschedulerv1alpha1.PodGang); ok && key.Name == tail.Name {
					if mode == "read-error" {
						return apierrors.NewForbidden(schema.GroupResource{Resource: "podgangs"}, key.Name, fmt.Errorf("denied"))
					}
					if mode == "replaced" {
						pg := obj.(*groveschedulerv1alpha1.PodGang)
						*pg = *tail.DeepCopy()
						pg.UID = "different-uid"
						return nil
					}
				}
				return cl.Get(ctx, key, obj, opts...)
			}}
			err := backend.SyncPodGang(t.Context(), base)
			if mode == "read-error" || mode == "replaced" {
				require.Error(t, err)
				if mode == "read-error" {
					assert.True(t, apierrors.IsForbidden(err))
				}
			} else {
				require.NoError(t, err)
				getTestWorkload(t, cl, base.Namespace, base.Name)
			}
		})
	}
}

func (c *identityTestClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.create != nil {
		return c.create(ctx, obj, opts...)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func clearIdentity(t *testing.T, cl client.Client, pg *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), pg))
	delete(pg.Annotations, kueueWorkloadNameAnnotation)
	require.NoError(t, cl.Update(t.Context(), pg))
}

func TestDelayedPredecessorCreationCannotCaptureReplacementPods(t *testing.T) {
	cl, backend, pcs, old := newResyncFixture(t)
	oldSnapshot := old.DeepCopy()
	concrete := backend.(*schedulerBackend)
	wrapped := &identityTestClient{Client: cl}
	concrete.client = wrapped
	concrete.directClient = cl
	var replacement *groveschedulerv1alpha1.PodGang
	var replacementPod *corev1.Pod
	wrapped.create = func(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
		wl, ok := obj.(*unstructured.Unstructured)
		if ok && workloadOwnedByPodGang(wl, old) {
			// Pause the predecessor at the actual Workload Create boundary. Its name was selected
			// under the previous behavior, but the API object does not exist when the replacement starts.
			require.NoError(t, cl.Delete(ctx, old))
			replacement = old.DeepCopy()
			replacement.UID = "new-incarnation-uid"
			replacement.ResourceVersion = ""
			replacement.Annotations = nil
			require.NoError(t, cl.Create(ctx, replacement))
			replacementPod = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: old.Namespace}}
			requirePreparationAwaitsWorkload(t, backend.PreparePod(replacementPod, identityPreparation(pcs, replacement)), "grove-"+string(replacement.UID))
			// The predecessor resumes before the replacement has ever synced its Workload.
		}
		return cl.Create(ctx, obj, opts...)
	}
	require.ErrorContains(t, backend.SyncPodGang(t.Context(), old), "no longer the live incarnation")
	require.NotNil(t, replacement)
	assert.Equal(t, "grove-"+string(replacement.UID), replacementPod.Labels[prebuiltWorkloadNameLabel])
	assert.Equal(t, "grove-"+string(replacement.UID), replacementPod.Labels[podGroupNameLabel])
	assert.True(t, workloadOwnedByPodGang(getTestWorkload(t, cl, old.Namespace, old.Name), oldSnapshot))
	stored := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(replacement), stored))
	assert.Empty(t, stored.Annotations[kueueWorkloadStateAnnotation], "late predecessor completion must not mark replacement created")
	pending := newKueueWorkload(replacement.Namespace, "grove-"+string(replacement.UID))
	require.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(pending), pending)))
	require.NoError(t, backend.SyncPodGang(t.Context(), replacement))
	assert.True(t, workloadOwnedByPodGang(getTestWorkload(t, cl, replacement.Namespace, "grove-"+string(replacement.UID)), replacement))
}

func TestReplacementWaitsForUnaccountedPredecessorPods(t *testing.T) {
	for _, mode := range []string{"finished-running", "finished-terminating", "missing", "deactivated", "unreserved", "reserved", "admission-gated", "terminal", "read-error"} {
		t.Run(mode, func(t *testing.T) {
			cl, backend, pcs, pg := newResyncFixture(t)
			clearIdentity(t, cl, pg)
			// Use an older UID-based identity too: the inventory cannot assume the predecessor name
			// equals the stable PodGang name.
			predecessor := "grove-older-incarnation"
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-consumer", Namespace: pg.Namespace, UID: "old-pod", Labels: enrolledPodLabels(pcs.Name, pg.Name)}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			pod.Labels[prebuiltWorkloadNameLabel] = predecessor
			pod.Labels[podGroupNameLabel] = predecessor
			if mode == "admission-gated" {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}}
			}
			if mode == "terminal" {
				pod.Status.Phase = corev1.PodSucceeded
			}
			if mode == "finished-terminating" {
				pod.Finalizers = []string{"test/hold"}
			}
			require.NoError(t, cl.Create(t.Context(), pod))
			if mode == "finished-terminating" {
				require.NoError(t, cl.Delete(t.Context(), pod))
			}
			if mode != "missing" {
				wl := newKueueWorkload(pg.Namespace, predecessor)
				require.NoError(t, unstructured.SetNestedSlice(wl.Object, []any{map[string]any{"type": "QuotaReserved", "status": "True"}}, "status", "conditions"))
				require.NoError(t, cl.Create(t.Context(), wl))
				if mode != "reserved" && mode != "deactivated" && mode != "unreserved" {
					setWorkloadCondition(t, cl, pg.Namespace, predecessor, "Finished")
				}
				if mode == "unreserved" {
					wl = getTestWorkload(t, cl, pg.Namespace, predecessor)
					require.NoError(t, unstructured.SetNestedSlice(wl.Object, []any{}, "status", "conditions"))
					require.NoError(t, cl.Update(t.Context(), wl))
				}
				if mode == "deactivated" {
					wl = getTestWorkload(t, cl, pg.Namespace, predecessor)
					require.NoError(t, unstructured.SetNestedField(wl.Object, false, "spec", "active"))
					require.NoError(t, cl.Update(t.Context(), wl))
				}
			}
			if mode == "read-error" {
				backend.(*schedulerBackend).directClient = &identityTestClient{Client: cl, get: func(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if key.Name == predecessor {
						return apierrors.NewForbidden(schema.GroupResource{Resource: "workloads"}, key.Name, fmt.Errorf("denied"))
					}
					return cl.Get(ctx, key, obj, opts...)
				}}
			}
			selected := "grove-" + string(pg.UID)
			err := backend.SyncPodGang(t.Context(), pg)
			if mode == "reserved" || mode == "terminal" {
				require.NoError(t, err)
				getTestWorkload(t, cl, pg.Namespace, selected)
				return
			}
			require.Error(t, err)
			if mode == "admission-gated" {
				require.ErrorContains(t, err, "waiting for admission-gated predecessor Pods")
				require.NoError(t, backend.SyncPodGang(t.Context(), pg))
				getTestWorkload(t, cl, pg.Namespace, selected)
				return
			}
			missing := newKueueWorkload(pg.Namespace, selected)
			assert.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(missing), missing)))
			if mode == "read-error" {
				assert.True(t, apierrors.IsForbidden(err))
				return
			}
			require.ErrorContains(t, err, "waiting for predecessor admission consumers")
			// Once the old consumer drains, retry makes progress without mutating its old lease.
			require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
			pod.Status.Phase = corev1.PodSucceeded
			require.NoError(t, cl.Status().Update(t.Context(), pod))
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))
			getTestWorkload(t, cl, pg.Namespace, selected)
		})
	}
}

func TestUpgradeReplacesLegacyGatedPodsBeforeCreatingWorkload(t *testing.T) {
	for _, incarnation := range []string{"current", "predecessor"} {
		t.Run(incarnation, func(t *testing.T) {
			cl, backend, pcs, pg := newResyncFixture(t)
			clearIdentity(t, cl, pg)
			ownerUID := pg.UID
			if incarnation == "predecessor" {
				ownerUID = "older-podgang-uid"
			}
			old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-gated", Namespace: pg.Namespace, Labels: enrolledPodLabels(pcs.Name, pg.Name), Finalizers: []string{"test/hold"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: groveschedulerv1alpha1.SchemeGroupVersion.String(), Kind: "PodGang", Name: pg.Name, UID: ownerUID}}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}}}}
			require.NoError(t, cl.Create(t.Context(), old))
			require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "waiting for admission-gated predecessor Pods")
			require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(old), old))
			require.NotNil(t, old.DeletionTimestamp)
			assert.Equal(t, pg.Name, old.Labels[prebuiltWorkloadNameLabel], "immutable group labels must not be rewritten")
			selected := "grove-" + string(pg.UID)
			wl := newKueueWorkload(pg.Namespace, selected)
			assert.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(wl), wl)))
			// Even while the legacy Pod drains, replacements prepared by the current controller
			// select the committed UID identity, never the old group. The lifecycle mode is read
			// from that Workload, so preparation waits until it exists.
			replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
			require.ErrorContains(t, backend.PreparePod(replacement, identityPreparation(pcs, pg)), selected)
			old.Finalizers = nil
			require.NoError(t, cl.Update(t.Context(), old))
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))
			assert.True(t, workloadOwnedByPodGang(getTestWorkload(t, cl, pg.Namespace, selected), pg))
			replacement = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pg.Namespace}}
			require.NoError(t, backend.PreparePod(replacement, identityPreparation(pcs, pg)))
			assert.Equal(t, selected, replacement.Labels[prebuiltWorkloadNameLabel])
		})
	}
}

func TestLegacyGatedPodAdmissionRaceRejectsDeletion(t *testing.T) {
	cl, backend, pcs, pg := newResyncFixture(t)
	clearIdentity(t, cl, pg)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "racing-gated", Namespace: pg.Namespace, Labels: enrolledPodLabels(pcs.Name, pg.Name)}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}}}}
	require.NoError(t, cl.Create(t.Context(), pod))
	raced := false
	wrapped := &identityDeleteRaceClient{Client: cl, beforeDelete: func() {
		require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
		pod.Spec.SchedulingGates = nil
		require.NoError(t, cl.Update(t.Context(), pod))
		raced = true
	}}
	backend.(*schedulerBackend).client = wrapped
	backend.(*schedulerBackend).directClient = cl
	err := backend.SyncPodGang(t.Context(), pg)
	assert.True(t, raced)
	require.True(t, apierrors.IsConflict(err), "admission changed resourceVersion, so the guarded delete must fail")
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pod), pod))
	assert.Nil(t, pod.DeletionTimestamp)
	require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "waiting for predecessor admission consumers")
}

type identityDeleteRaceClient struct {
	client.Client
	beforeDelete func()
}

func (c *identityDeleteRaceClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if c.beforeDelete != nil {
		fn := c.beforeDelete
		c.beforeDelete = nil
		fn()
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestWorkloadStateWriteValidatesUIDBeforeMatchingCachedState(t *testing.T) {
	_, backend, _, pg := newResyncFixture(t)
	pg.Annotations[kueueWorkloadStateAnnotation] = kueueWorkloadStateCreated
	live := pg.DeepCopy()
	live.UID = "replacement-uid"
	delete(live.Annotations, kueueWorkloadStateAnnotation)
	concrete := backend.(*schedulerBackend)
	concrete.directClient = testutils.NewTestClientBuilder().WithObjects(live).Build()
	require.ErrorContains(t, concrete.setPodGangWorkloadState(t.Context(), pg, kueueWorkloadStateCreated), "no longer the live incarnation")
	_, err := concrete.podGangWorkloadStateUncached(t.Context(), pg)
	require.ErrorContains(t, err, "no longer the live incarnation")
}

func TestWorkloadStateWriteRejectsConcurrentChanges(t *testing.T) {
	cl, backend, _, pg := newResyncFixture(t)
	raced := false
	wrapped := &identityTestClient{Client: cl}
	wrapped.patch = func(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		if !raced {
			raced = true
			live := &groveschedulerv1alpha1.PodGang{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(pg), live))
			live.Annotations["test/concurrent"] = "retained"
			require.NoError(t, cl.Update(ctx, live))
		}
		return cl.Patch(ctx, obj, p, opts...)
	}
	concrete := backend.(*schedulerBackend)
	concrete.client = wrapped
	concrete.directClient = cl
	require.True(t, apierrors.IsConflict(concrete.setPodGangWorkloadState(t.Context(), pg, kueueWorkloadStateCreated)))
	live := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), live))
	assert.Empty(t, live.Annotations[kueueWorkloadStateAnnotation])
	assert.Equal(t, "retained", live.Annotations["test/concurrent"])
	require.NoError(t, concrete.setPodGangWorkloadState(t.Context(), pg, kueueWorkloadStateCreated))
	assert.Equal(t, kueueWorkloadStateCreated, pg.Annotations[kueueWorkloadStateAnnotation])
	assert.Equal(t, "retained", pg.Annotations["test/concurrent"])
}

// requirePreparationAwaitsWorkload asserts that Pod preparation chose the given Workload identity
// and is waiting for that Workload: the pod lifecycle is read from it, so a Pod prepared before
// SyncPodGang creates it is retried rather than joined to any other group.
func requirePreparationAwaitsWorkload(t *testing.T, err error, workload string) {
	t.Helper()
	require.ErrorContains(t, err, "cannot choose pod lifecycle before reading Workload")
	require.ErrorContains(t, err, "/"+workload+":")
}
