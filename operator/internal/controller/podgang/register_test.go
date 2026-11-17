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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
