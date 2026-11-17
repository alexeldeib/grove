// Copyright 2025 The Grove Authors.
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

package podgang

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveclient "github.com/ai-dynamo/grove/operator/internal/client"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// predicateTestCase describes a scenario and expected predicate result per event type.
type predicateTestCase struct {
	name                    string
	managedOld              bool
	managedNew              bool
	generationChanged       bool
	shouldAllowCreateEvent  bool
	shouldAllowDeleteEvent  bool
	shouldAllowGenericEvent bool
	shouldAllowUpdateEvent  bool
}

func TestPodGangSpecChangePredicate(t *testing.T) {
	pred := podGangSpecChangePredicate()

	tests := []predicateTestCase{
		{
			name:                    "managed PodGang create",
			managedOld:              true,
			managedNew:              true,
			shouldAllowCreateEvent:  true,
			shouldAllowDeleteEvent:  true,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false,
		},
		{
			name:                    "unmanaged PodGang create",
			managedOld:              false,
			managedNew:              false,
			shouldAllowCreateEvent:  false,
			shouldAllowDeleteEvent:  false,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false,
		},
		{
			name:                    "managed PodGang update with spec change (generation changed)",
			managedOld:              true,
			managedNew:              true,
			generationChanged:       true,
			shouldAllowCreateEvent:  true,
			shouldAllowDeleteEvent:  true,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  true,
		},
		{
			name:                    "managed PodGang update with status-only change (generation unchanged)",
			managedOld:              true,
			managedNew:              true,
			generationChanged:       false,
			shouldAllowCreateEvent:  true,
			shouldAllowDeleteEvent:  true,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false,
		},
		{
			name:                    "update with old managed and new unmanaged",
			managedOld:              true,
			managedNew:              false,
			generationChanged:       true,
			shouldAllowCreateEvent:  false, // Create/Delete use newPG which is unmanaged
			shouldAllowDeleteEvent:  false,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false,
		},
		{
			name:                    "update with old unmanaged and new managed",
			managedOld:              false,
			managedNew:              true,
			generationChanged:       true,
			shouldAllowCreateEvent:  true, // Create/Delete use newPG which is managed
			shouldAllowDeleteEvent:  true,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false, // old is unmanaged
		},
		{
			name:                    "generic event always rejected",
			managedOld:              true,
			managedNew:              true,
			shouldAllowCreateEvent:  true,
			shouldAllowDeleteEvent:  true,
			shouldAllowGenericEvent: false,
			shouldAllowUpdateEvent:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldPG := testutils.NewPodGangBuilder("test-pg", "default").
				WithGeneration(1).
				WithManaged(tc.managedOld).
				WithPodGroup("pg0", 1).
				Build()
			newPG := testutils.NewPodGangBuilder("test-pg", "default").
				WithGeneration(1).
				WithManaged(tc.managedNew).
				WithPodGroup("pg0", 1).
				Build()
			if tc.generationChanged {
				newPG.SetGeneration(oldPG.GetGeneration() + 1)
			}

			assert.Equal(t, tc.shouldAllowCreateEvent, pred.Create(event.CreateEvent{Object: newPG}), "Create")
			assert.Equal(t, tc.shouldAllowDeleteEvent, pred.Delete(event.DeleteEvent{Object: newPG}), "Delete")
			assert.Equal(t, tc.shouldAllowGenericEvent, pred.Generic(event.GenericEvent{Object: newPG}), "Generic")
			assert.Equal(t, tc.shouldAllowUpdateEvent, pred.Update(event.UpdateEvent{ObjectOld: oldPG, ObjectNew: newPG}), "Update")
		})
	}
}

func TestPodCliqueSetChangePredicate(t *testing.T) {
	pred := podCliqueSetChangePredicate()
	managedLabels := map[string]string{apicommon.LabelManagedByKey: apicommon.LabelManagedByValue}

	oldPCS := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Generation: 1, Labels: managedLabels},
	}
	newPCS := oldPCS.DeepCopy()
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: oldPCS, ObjectNew: newPCS}))

	newPCS.Generation = 2
	assert.True(t, pred.Update(event.UpdateEvent{ObjectOld: oldPCS, ObjectNew: newPCS}))

	newPCS = oldPCS.DeepCopy()
	newPCS.Labels = map[string]string{
		apicommon.LabelManagedByKey: apicommon.LabelManagedByValue,
		"kueue.x-k8s.io/queue-name": "queue-b",
	}
	assert.True(t, pred.Update(event.UpdateEvent{ObjectOld: oldPCS, ObjectNew: newPCS}))
}

func TestPodCliqueSetProtectionChangesEnqueuePodGangs(t *testing.T) {
	for name, tc := range map[string]struct {
		before, after map[string]string
		want          bool
	}{
		"added":     {after: map[string]string{"grove.io/kueue-protected-replicas": `{"w":2}`}, want: true},
		"changed":   {before: map[string]string{"grove.io/kueue-protected-replicas": `{"w":1}`}, after: map[string]string{"grove.io/kueue-protected-replicas": `{"w":2}`}, want: true},
		"removed":   {before: map[string]string{"grove.io/kueue-protected-replicas": `{"w":1}`}, want: true},
		"unrelated": {after: map[string]string{"unrelated": "value"}},
		"unchanged": {before: map[string]string{"grove.io/kueue-protected-replicas": `{"w":1}`}, after: map[string]string{"grove.io/kueue-protected-replicas": `{"w":1}`}},
	} {
		t.Run(name, func(t *testing.T) {
			oldPCS := &grovecorev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"nvidia.com/dynamo-graph-deployment-name": "graph"},
				Annotations: tc.before,
			}}
			newPCS := oldPCS.DeepCopy()
			newPCS.Annotations = tc.after
			require.Equal(t, tc.want, podCliqueSetChangePredicate().Update(event.UpdateEvent{ObjectOld: oldPCS, ObjectNew: newPCS}))
		})
	}
}

func TestKueueWorkloadEvictionPredicate(t *testing.T) {
	pred := kueueWorkloadEvictionPredicate()
	running := newKueueWorkloadType()
	finished := newKueueWorkloadType()
	require.NoError(t, unstructured.SetNestedSlice(finished.Object, []any{map[string]any{
		"type":   "Finished",
		"status": "True",
	}}, "status", "conditions"))
	deactivated := newKueueWorkloadType()
	require.NoError(t, unstructured.SetNestedField(deactivated.Object, false, "spec", "active"))

	assert.False(t, pred.Create(event.CreateEvent{Object: running}), "Create")
	assert.True(t, pred.Delete(event.DeleteEvent{Object: running}), "Delete")
	assert.False(t, pred.Generic(event.GenericEvent{Object: running}), "Generic")
	assert.True(t, pred.Update(event.UpdateEvent{ObjectOld: running, ObjectNew: finished}), "Update finished transition")
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: finished, ObjectNew: finished}), "Update already finished")
	assert.True(t, pred.Update(event.UpdateEvent{ObjectOld: running, ObjectNew: deactivated}), "Update deactivated transition")
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: deactivated, ObjectNew: deactivated}), "Update already deactivated")
	assert.False(t, pred.Update(event.UpdateEvent{ObjectOld: running, ObjectNew: running}), "Update running")
}

func TestMapPodCliqueSetToPodGangs(t *testing.T) {
	const namespace = "default"
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: namespace},
	}
	labelsForPCS := func(name string) map[string]string {
		return map[string]string{
			apicommon.LabelManagedByKey: apicommon.LabelManagedByValue,
			apicommon.LabelPartOfKey:    name,
			apicommon.LabelComponentKey: apicommon.LabelComponentNamePodGang,
		}
	}
	first := testutils.NewPodGangBuilder("demo-0", namespace).Build()
	first.Labels = labelsForPCS(pcs.Name)
	second := testutils.NewPodGangBuilder("demo-0-workers-0", namespace).Build()
	second.Labels = labelsForPCS(pcs.Name)
	other := testutils.NewPodGangBuilder("other-0", namespace).Build()
	other.Labels = labelsForPCS("other")

	r := &Reconciler{Client: fake.NewClientBuilder().
		WithScheme(groveclient.Scheme).
		WithObjects(first, second, other).
		Build()}
	requests := r.mapPodCliqueSetToPodGangs(context.Background(), pcs)

	require.Len(t, requests, 2)
	assert.ElementsMatch(t, []string{"demo-0", "demo-0-workers-0"}, []string{
		requests[0].Name,
		requests[1].Name,
	})
}

// pclqWithScheduled builds a Grove-managed PodClique for the watch tests.
func pclqWithScheduled(name, podGangName, basePodGangName string, scheduled int32) *grovecorev1alpha1.PodClique {
	labels := map[string]string{
		apicommon.LabelManagedByKey: apicommon.LabelManagedByValue,
		apicommon.LabelPodGang:      podGangName,
	}
	if basePodGangName != "" {
		labels[apicommon.LabelBasePodGang] = basePodGangName
	}
	return &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Status:     grovecorev1alpha1.PodCliqueStatus{ScheduledReplicas: scheduled},
	}
}

// A base PodGang becoming scheduled shows up only as a PodClique status write, which bumps no
// generation. Without this predicate the base is never reconciled on that transition and a gated
// scaled sibling is never released.
func TestPodCliqueScheduledReplicasChangePredicate(t *testing.T) {
	p := podCliqueScheduledReplicasChangePredicate()

	assert.True(t, p.Update(event.UpdateEvent{
		ObjectOld: pclqWithScheduled("demo-0-worker", "demo-0", "", 0),
		ObjectNew: pclqWithScheduled("demo-0-worker", "demo-0", "", 1),
	}), "a change in scheduledReplicas must enqueue")

	assert.False(t, p.Update(event.UpdateEvent{
		ObjectOld: pclqWithScheduled("demo-0-worker", "demo-0", "", 1),
		ObjectNew: pclqWithScheduled("demo-0-worker", "demo-0", "", 1),
	}), "an unrelated status write must not enqueue")

	// The API server drops status on create, so a new PodClique has nothing scheduled, and a base
	// losing scheduling releases nothing.
	assert.False(t, p.Create(event.CreateEvent{Object: pclqWithScheduled("demo-0-worker", "demo-0", "", 0)}))
	assert.False(t, p.Delete(event.DeleteEvent{Object: pclqWithScheduled("demo-0-worker", "demo-0", "", 1)}))

	unmanagedOld := pclqWithScheduled("demo-0-worker", "demo-0", "", 0)
	unmanagedOld.Labels = map[string]string{}
	unmanagedNew := unmanagedOld.DeepCopy()
	unmanagedNew.Status.ScheduledReplicas = 1
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: unmanagedOld, ObjectNew: unmanagedNew}),
		"unmanaged PodCliques stay filtered")
}

// Only base PodCliques are mapped, and only to their own replica: the base and its scaled siblings.
func TestMapBasePodCliqueToPodGangs(t *testing.T) {
	base := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-0", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
	tail := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-0-w-0", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
	other := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "unrelated-0", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "unrelated"}}}
	replica1 := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-1", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
	replica1Tail := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-1-w-0", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
	replica10 := &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name: "demo-10", Namespace: "default",
		Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}}}
	cl := testutils.NewTestClientBuilder().WithObjects(base, tail, other, replica1, replica1Tail, replica10).Build()
	r := &Reconciler{Client: cl}

	basePclq := pclqWithScheduled("demo-0-worker", "demo-0", "", 1)
	basePclq.Labels[apicommon.LabelPartOfKey] = "demo"
	got := r.mapBasePodCliqueToPodGangs(context.Background(), basePclq)
	names := make([]string, 0, len(got))
	for _, req := range got {
		names = append(names, req.Name)
	}
	assert.ElementsMatch(t, []string{"demo-0", "demo-0-w-0"}, names,
		"the base and its own siblings only: no other replica, no other PodCliqueSet")

	replica1Pclq := pclqWithScheduled("demo-1-worker", "demo-1", "", 1)
	replica1Pclq.Labels[apicommon.LabelPartOfKey] = "demo"
	names = names[:0]
	for _, req := range r.mapBasePodCliqueToPodGangs(context.Background(), replica1Pclq) {
		names = append(names, req.Name)
	}
	assert.ElementsMatch(t, []string{"demo-1", "demo-1-w-0"}, names, "demo-1- must not match demo-10")

	unlabelled := pclqWithScheduled("demo-0-worker", "", "", 1)
	delete(unlabelled.Labels, apicommon.LabelPodGang)
	unlabelled.Labels[apicommon.LabelPartOfKey] = "demo"
	assert.Len(t, r.mapBasePodCliqueToPodGangs(context.Background(), unlabelled), 5,
		"without its PodGang label the whole set is enqueued rather than dropping the event")

	scaledPclq := pclqWithScheduled("demo-0-w-1-worker", "demo-0-w-0", "demo-0", 1)
	scaledPclq.Labels[apicommon.LabelPartOfKey] = "demo"
	assert.Empty(t, r.mapBasePodCliqueToPodGangs(context.Background(), scaledPclq),
		"a scaled PodClique must not be mapped")

	noPartOf := pclqWithScheduled("demo-0-worker", "demo-0", "", 1)
	delete(noPartOf.Labels, apicommon.LabelPartOfKey)
	assert.Empty(t, r.mapBasePodCliqueToPodGangs(context.Background(), noPartOf),
		"a PodClique with no part-of label must not be mapped")
}
