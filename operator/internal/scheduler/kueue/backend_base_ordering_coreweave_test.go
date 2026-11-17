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
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	orderingBasePclq = "demo-0-worker"     // PodGroup of the base PodGang "demo-0"
	orderingTailGang = "demo-0-w-0"        // scaled PodGang: not "<part-of>-0", so a sibling
	orderingTailPclq = "demo-0-w-1-worker" // its PodGroup
)

// newOrderingFixture returns the gate fixture's PodGang as the base plus one scaled sibling, with the
// base's scheduled-replica count under the caller's control and the real ordering policy wired in.
func newOrderingFixture(t *testing.T, baseScheduledReplicas int32) (client.Client, *schedulerBackend, *groveschedulerv1alpha1.PodGang, *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	cl, backend, base := newGateFixture(t)
	addOrderingObjects(t, cl, base, baseScheduledReplicas)
	return cl, backend, base, orderingTail(base)
}

func addOrderingObjects(t *testing.T, cl client.Client, base *groveschedulerv1alpha1.PodGang, baseScheduledReplicas int32) {
	t.Helper()
	require.NoError(t, cl.Create(t.Context(), &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Namespace: base.Namespace, Name: orderingBasePclq},
		Status:     grovecorev1alpha1.PodCliqueStatus{ScheduledReplicas: baseScheduledReplicas},
	}))
	require.NoError(t, cl.Create(t.Context(), orderingTail(base)))
	require.NoError(t, cl.Create(t.Context(), &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Namespace: base.Namespace, Name: orderingTailPclq},
	}))
}

func orderingTail(base *groveschedulerv1alpha1.PodGang) *groveschedulerv1alpha1.PodGang {
	return &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   base.Namespace,
			Name:        orderingTailGang,
			UID:         "tail-podgang-uid",
			Annotations: map[string]string{kueueWorkloadNameAnnotation: orderingTailGang},
			Labels:      map[string]string{apicommon.LabelPartOfKey: base.Labels[apicommon.LabelPartOfKey]},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{Name: orderingTailPclq, MinReplicas: 1}},
		},
	}
}

func setBaseScheduled(t *testing.T, cl client.Client, ns string, replicas int32) {
	t.Helper()
	pclq := &grovecorev1alpha1.PodClique{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: orderingBasePclq}, pclq))
	pclq.Status.ScheduledReplicas = replicas
	require.NoError(t, cl.Update(t.Context(), pclq))
}

func reSync(t *testing.T, cl client.Client, backend *schedulerBackend, pg *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	stored := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), stored))
	require.NoError(t, backend.SyncPodGang(t.Context(), stored))
}

func gated(t *testing.T, cl client.Client, pg *groveschedulerv1alpha1.PodGang) bool {
	t.Helper()
	for _, gate := range workloadAdmissionGates(getTestWorkload(t, cl, pg.Namespace, pg.Name)) {
		if gate == basePodGangAdmissionGate {
			return true
		}
	}
	return false
}

func TestBackendSyncPodGangGatesScaledPodGangWhileBaseUnscheduled(t *testing.T) {
	cl, backend, _, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))

	assert.True(t, gated(t, cl, tail))
	workload := getTestWorkload(t, cl, tail.Namespace, tail.Name)
	active, _, _ := unstructured.NestedBool(workload.Object, "spec", "active")
	assert.True(t, active, "the gate holds admission; spec.active stays Kueue's")
}

// PodGangs and PodCliques are created concurrently. On dev-eu-n-04a a scaled PodGang synced before its
// PodClique existed was read as a base and admitted while the base was still unscheduled.
func TestBackendSyncPodGangGatesScaledPodGangWithNoPodCliquesYet(t *testing.T) {
	cl, backend, _, tail := newOrderingFixture(t, 0)
	require.NoError(t, cl.Delete(t.Context(), &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Namespace: tail.Namespace, Name: orderingTailPclq},
	}))
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	assert.True(t, gated(t, cl, tail))
}

func TestBackendSyncPodGangNeverGatesBasePodGang(t *testing.T) {
	cl, backend, base, _ := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), base))
	assert.False(t, gated(t, cl, base), "a base waits on nothing; gating it would deadlock the set")
}

func TestBackendSyncPodGangDoesNotGateScaledPodGangOnceBaseScheduled(t *testing.T) {
	cl, backend, _, tail := newOrderingFixture(t, 1)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	assert.False(t, gated(t, cl, tail))
}

// A gated sibling produces no events of its own, so reconciling only the base must release it.
func TestBackendSyncPodGangBaseReleasesGatedScaledSiblings(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	require.True(t, gated(t, cl, tail))

	setBaseScheduled(t, cl, base.Namespace, 1)
	reSync(t, cl, backend, base)
	assert.False(t, gated(t, cl, tail))
}

func TestBackendSyncPodGangBaseDoesNotReleaseWhileUnscheduled(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	for range 3 {
		reSync(t, cl, backend, base)
	}
	assert.True(t, gated(t, cl, tail))
}

// The PodClique watch enqueues the whole set, so a sibling's own reconcile must also release it.
func TestBackendSyncPodGangScaledPodGangReleasesItselfOnceBaseScheduled(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	require.True(t, gated(t, cl, tail))

	setBaseScheduled(t, cl, base.Namespace, 1)
	reSync(t, cl, backend, tail)
	assert.False(t, gated(t, cl, tail))
}

// scheduleBaseOnWorkloadCreate marks the base scheduled the moment the sibling's Workload is created:
// after the gate decision, before the base's release could have seen the new Workload.
type scheduleBaseOnWorkloadCreate struct {
	client.Client
	t *testing.T
}

func (c scheduleBaseOnWorkloadCreate) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if workload, ok := obj.(*unstructured.Unstructured); ok && workload.GetName() == orderingTailGang {
		setBaseScheduled(c.t, c.Client, workload.GetNamespace(), 1)
	}
	return nil
}

// Workload create events are not watched, so if the base schedules between the sibling's gate
// decision and its create, the base's release can run before the Workload exists and nothing would
// reconcile the sibling again. The live re-check after create must release it.
func TestBackendSyncPodGangReleasesGateWhenBaseSchedulesDuringCreate(t *testing.T) {
	inner, _, base := newGateFixture(t)
	addOrderingObjects(t, inner, base, 0)
	cl := scheduleBaseOnWorkloadCreate{Client: inner, t: t}
	backend := New(cl, inner.Scheme(), record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue}).(*schedulerBackend)
	require.NoError(t, backend.Init(nil))

	tail := orderingTail(base)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	assert.False(t, gated(t, cl, tail))
}

// Each PodCliqueSet replica has its own base; releasing replica 0 must not let replica 1 in.
func TestBackendSyncPodGangBaseReleasesOnlyItsOwnReplica(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))

	otherTail := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: base.Namespace, Name: "demo-1-w-0", UID: "other-tail-uid", Annotations: map[string]string{kueueWorkloadNameAnnotation: "demo-1-w-0"},
			Labels: map[string]string{apicommon.LabelPartOfKey: base.Labels[apicommon.LabelPartOfKey]},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{Name: "demo-1-w-1-worker", MinReplicas: 1}},
		},
	}
	require.NoError(t, cl.Create(t.Context(), otherTail))
	require.NoError(t, backend.SyncPodGang(t.Context(), otherTail))
	require.True(t, gated(t, cl, otherTail), "replica 1's base demo-1 does not exist yet")

	setBaseScheduled(t, cl, base.Namespace, 1)
	reSync(t, cl, backend, base)
	assert.False(t, gated(t, cl, tail))
	assert.True(t, gated(t, cl, otherTail), "replica 0's base must not release replica 1's sibling")
}

func TestBackendSyncPodGangDoesNotGateSecondReplicaBase(t *testing.T) {
	cl, backend, base, _ := newOrderingFixture(t, 0)
	secondBase := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: base.Namespace, Name: "demo-1", UID: "second-base-uid", Annotations: map[string]string{kueueWorkloadNameAnnotation: "demo-1"},
			Labels: map[string]string{apicommon.LabelPartOfKey: base.Labels[apicommon.LabelPartOfKey]},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{Name: "demo-1-worker", MinReplicas: 1}},
		},
	}
	require.NoError(t, cl.Create(t.Context(), secondBase))
	require.NoError(t, backend.SyncPodGang(t.Context(), secondBase))
	assert.False(t, gated(t, cl, secondBase), "replica 1's base must not be held behind replica 0")
}

// Gating happens only at creation. A released sibling stays admissible when its base later loses
// scheduling; re-gating it would be rejected by Kueue, and Kueue's WaitForStart timeout reclaims
// quota from a sibling that never starts.
func TestBackendSyncPodGangLeavesReleasedSiblingAloneWhenBaseLosesScheduling(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	setBaseScheduled(t, cl, base.Namespace, 1)
	reSync(t, cl, backend, base)
	require.False(t, gated(t, cl, tail))

	setBaseScheduled(t, cl, base.Namespace, 0)
	reSync(t, cl, backend, tail)
	reSync(t, cl, backend, base)
	assert.False(t, gated(t, cl, tail))
}

// A Workload recreated after the base scheduled is created ungated, whatever its previous one was.
func TestBackendSyncPodGangRecreatesWorkloadUngatedOnceBaseScheduled(t *testing.T) {
	cl, backend, base, tail := newOrderingFixture(t, 0)
	require.NoError(t, backend.SyncPodGang(t.Context(), tail))
	require.True(t, gated(t, cl, tail))

	stored := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(tail), stored))
	stored.Annotations[kueueWorkloadStateAnnotation] = kueueWorkloadStateRecreate
	require.NoError(t, cl.Update(t.Context(), stored))
	require.NoError(t, cl.Delete(t.Context(), getTestWorkload(t, cl, tail.Namespace, tail.Name)))
	setBaseScheduled(t, cl, base.Namespace, 1)

	reSync(t, cl, backend, tail)
	assert.False(t, gated(t, cl, tail))
}

func TestBasePodGangNameForDerivesReplicaIndex(t *testing.T) {
	for name, tc := range map[string]struct{ podGang, want string }{
		"replica 0 base is a base":             {"demo-0", ""},
		"replica 1 base is a base, not a tail": {"demo-1", ""},
		"replica 12 base is a base":            {"demo-12", ""},
		"replica 0 tail waits on demo-0":       {"demo-0-w-0", "demo-0"},
		"replica 1 tail waits on demo-1":       {"demo-1-w-0", "demo-1"},
		"replica 2 tail waits on demo-2":       {"demo-2-sg-3", "demo-2"},
		"unparseable name is left alone":       {"demo-x-w-0", ""},
	} {
		t.Run(name, func(t *testing.T) {
			got := basePodGangNameFor(&groveschedulerv1alpha1.PodGang{
				ObjectMeta: metav1.ObjectMeta{
					Name:   tc.podGang,
					Labels: map[string]string{apicommon.LabelPartOfKey: "demo"},
				},
			})
			assert.Equal(t, tc.want, got)
		})
	}
}
