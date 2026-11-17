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
	"slices"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const testAdmissionGate = "grove.io/test-gate"

// admissionGateWebhook makes a fake client reject what a real cluster rejects: Kueue's
// AdmissionGatedBy rule, under which an existing Workload may lose gates but never gain one, and
// the grove-kueue ClusterRole, which grants update but not patch on Workloads.
type admissionGateWebhook struct{ client.Client }

func (c admissionGateWebhook) check(ctx context.Context, obj client.Object) error {
	workload, ok := obj.(*unstructured.Unstructured)
	if !ok || workload.GroupVersionKind() != workloadGVK {
		return nil
	}
	stored := newKueueWorkload(workload.GetNamespace(), workload.GetName())
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		return client.IgnoreNotFound(err)
	}
	before := workloadAdmissionGates(stored)
	for _, gate := range workloadAdmissionGates(workload) {
		if !slices.Contains(before, gate) {
			return fmt.Errorf("admission webhook: cannot add admission gate %q after creation", gate)
		}
	}
	return nil
}

func (c admissionGateWebhook) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if err := c.check(ctx, obj); err != nil {
		return err
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c admissionGateWebhook) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if workload, ok := obj.(*unstructured.Unstructured); ok && workload.GroupVersionKind() == workloadGVK {
		return fmt.Errorf("rbac: grove-operator cannot patch resource \"workloads\" in API group \"kueue.x-k8s.io\"")
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// newGateFixture is the resync fixture behind the webhook, with the creation-time gate seam set to
// gates.
func newGateFixture(t *testing.T, gates ...string) (client.Client, *schedulerBackend, *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	inner, _, _, pg := newResyncFixture(t)
	cl := admissionGateWebhook{inner}
	backend := New(cl, inner.Scheme(), record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue}).(*schedulerBackend)
	require.NoError(t, backend.Init(nil))
	if gates != nil {
		backend.admissionGatesForCreate = func(context.Context, *groveschedulerv1alpha1.PodGang) ([]string, error) {
			return gates, nil
		}
	}
	return cl, backend, pg
}

func TestBackendSyncPodGangCreatesWorkloadWithoutAdmissionGateByDefault(t *testing.T) {
	cl, backend, pg := newGateFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	workload := getTestWorkload(t, cl, pg.Namespace, pg.Name)
	_, found := workload.GetAnnotations()[admissionGatedByAnnotation]
	assert.False(t, found, "no gate is requested, so the Workload must be admissible")
}

func TestBackendSyncPodGangCreatesWorkloadWithRequestedAdmissionGates(t *testing.T) {
	cl, backend, pg := newGateFixture(t, testAdmissionGate, "example.com/other")
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	workload := getTestWorkload(t, cl, pg.Namespace, pg.Name)
	assert.Equal(t, testAdmissionGate+",example.com/other", workload.GetAnnotations()[admissionGatedByAnnotation])
	active, _, _ := unstructured.NestedBool(workload.Object, "spec", "active")
	assert.True(t, active, "gating must not touch spec.active; an inactive Workload is Kueue's deactivation")
}

// Gates are applied only when the Workload is created. A later sync that still wants the gate must
// leave a released Workload alone, since adding it back is rejected by Kueue.
func TestBackendSyncPodGangDoesNotReapplyReleasedAdmissionGate(t *testing.T) {
	cl, backend, pg := newGateFixture(t, testAdmissionGate)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	workload := getTestWorkload(t, cl, pg.Namespace, pg.Name)
	released, err := backend.releaseAdmissionGate(t.Context(), workload, testAdmissionGate)
	require.NoError(t, err)
	require.True(t, released)

	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	workload = getTestWorkload(t, cl, pg.Namespace, pg.Name)
	_, found := workload.GetAnnotations()[admissionGatedByAnnotation]
	assert.False(t, found)

	setAdmissionGates(workload, []string{testAdmissionGate})
	require.ErrorContains(t, cl.Update(t.Context(), workload), "after creation",
		"the fixture must reject what Kueue rejects, or this test proves nothing")
}

func TestReleaseAdmissionGateKeepsOtherControllersGates(t *testing.T) {
	cl, backend, pg := newGateFixture(t, "example.com/other", testAdmissionGate)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	workload := getTestWorkload(t, cl, pg.Namespace, pg.Name)

	released, err := backend.releaseAdmissionGate(t.Context(), workload, testAdmissionGate)
	require.NoError(t, err)
	assert.True(t, released)
	assert.Equal(t, "example.com/other", getTestWorkload(t, cl, pg.Namespace, pg.Name).GetAnnotations()[admissionGatedByAnnotation])

	released, err = backend.releaseAdmissionGate(t.Context(), getTestWorkload(t, cl, pg.Namespace, pg.Name), testAdmissionGate)
	require.NoError(t, err)
	assert.False(t, released, "releasing a gate the Workload no longer lists is a no-op")
}

func TestReleaseAdmissionGateDropsAnnotationWithLastGate(t *testing.T) {
	cl, backend, pg := newGateFixture(t, testAdmissionGate)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	released, err := backend.releaseAdmissionGate(t.Context(), getTestWorkload(t, cl, pg.Namespace, pg.Name), testAdmissionGate)
	require.NoError(t, err)
	assert.True(t, released)
	_, found := getTestWorkload(t, cl, pg.Namespace, pg.Name).GetAnnotations()[admissionGatedByAnnotation]
	assert.False(t, found, "an empty annotation would still be parsed; remove the key")
}

// staleCachePodGang returns the stored PodGang as an informer that missed its latest state write
// would hand it to SyncPodGang.
func staleCachePodGang(t *testing.T, cl client.Client, pg *groveschedulerv1alpha1.PodGang) *groveschedulerv1alpha1.PodGang {
	t.Helper()
	stale := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stale))
	delete(stale.Annotations, kueueWorkloadStateAnnotation)
	return stale
}

// The deactivated latch is terminal, but the early return that enforces it reads the cached PodGang.
// A reconcile whose cache predates the latch must still refuse to recreate the Workload.
func TestBackendSyncPodGangRefusesRecreatingDeactivatedWorkloadOnStaleCache(t *testing.T) {
	cl, backend, pg := newGateFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	stored := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stored))
	stored.Annotations[kueueWorkloadStateAnnotation] = kueueWorkloadStateDeactivated
	require.NoError(t, cl.Update(t.Context(), stored))
	require.NoError(t, cl.Delete(t.Context(), getTestWorkload(t, cl, stored.Namespace, stored.Name)))

	require.ErrorContains(t, backend.SyncPodGang(t.Context(), staleCachePodGang(t, cl, stored)),
		"after Kueue deactivated")

	missing := newKueueWorkload(stored.Namespace, stored.Name)
	assert.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(missing), missing)))
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stored))
	assert.Equal(t, kueueWorkloadStateDeactivated, stored.Annotations[kueueWorkloadStateAnnotation],
		"the terminal latch must not be overwritten with created")
}
