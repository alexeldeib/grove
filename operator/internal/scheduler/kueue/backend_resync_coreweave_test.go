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
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newResyncFixture returns a Kueue backend plus the PCS/PodGang it reconciles, used by the
// evict-and-recreate tests below.
func newResyncFixture(t *testing.T) (client.Client, scheduler.Backend, *grovecorev1alpha1.PodCliqueSet, *groveschedulerv1alpha1.PodGang) {
	t.Helper()
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
	)
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pcsName,
			Namespace: namespace,
			Labels:    map[string]string{queueNameLabel: "queue-a"},
		},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{{
					Name: "worker",
					Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1},
				}},
			},
		},
	}
	pg := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podGang,
			Namespace: namespace,
			UID:       "podgang-uid",
			Labels:    map[string]string{apicommon.LabelPartOfKey: pcsName},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{
				Name: "demo-0-worker", MinReplicas: 1,
			}},
		},
	}
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pg).Build()
	backend := New(
		cl,
		cl.Scheme(),
		record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue},
	)
	require.NoError(t, backend.Init(nil))
	return cl, backend, pcs, pg
}

func setWorkloadCondition(t *testing.T, cl client.Client, ns, name, condType string) {
	t.Helper()
	workload := getTestWorkload(t, cl, ns, name)
	require.NoError(t, unstructured.SetNestedSlice(workload.Object, []any{map[string]any{
		"type":   condType,
		"status": "True",
	}}, "status", "conditions"))
	require.NoError(t, cl.Update(context.Background(), workload))
}

// A spec change after Kueue has reserved quota cannot be applied in place because Kueue makes
// podSets immutable at admission. The backend must evict the stale Workload so the next reconcile
// recreates it with the current podSets.
func TestBackendSyncPodGangEvictsWorkloadOnPostAdmissionSpecChange(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, pcs, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	setWorkloadCondition(t, cl, namespace, podGang, "QuotaReserved")
	pcs.Labels[queueNameLabel] = "queue-c"
	require.NoError(t, cl.Update(context.Background(), pcs))

	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
	evicted := newKueueWorkload(namespace, podGang)
	err := cl.Get(context.Background(), client.ObjectKeyFromObject(evicted), evicted)
	require.True(t, apierrors.IsNotFound(err), "stale Workload should have been evicted, got %v", err)

	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
	recreated := getTestWorkload(t, cl, namespace, podGang)
	assert.Equal(t, "queue-c", nestedString(t, recreated, "spec", "queueName"))
}

// A Workload that Kueue has finalized (Finished/OutOfSync — e.g. after the owning PodGang changed
// shape) is terminal and can never re-admit. The backend must evict it even when the desired spec
// is otherwise unchanged.
func TestBackendSyncPodGangEvictsFinishedWorkload(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	setWorkloadCondition(t, cl, namespace, podGang, "Finished")

	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
	evicted := newKueueWorkload(namespace, podGang)
	err := cl.Get(context.Background(), client.ObjectKeyFromObject(evicted), evicted)
	require.True(t, apierrors.IsNotFound(err), "finished Workload should have been evicted, got %v", err)
}

// While an evicted Workload is still terminating (deletionTimestamp set, finalizer pending), the
// backend must wait rather than recreate it, otherwise the create races the pending deletion.
func TestBackendSyncPodGangWaitsForTerminatingWorkload(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	workload.SetFinalizers([]string{"kueue.x-k8s.io/resource-in-use"})
	require.NoError(t, cl.Update(context.Background(), workload))
	require.NoError(t, cl.Delete(context.Background(), workload))

	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
	terminating := getTestWorkload(t, cl, namespace, podGang)
	assert.NotNil(t, terminating.GetDeletionTimestamp(), "terminating Workload must not be recreated while deleting")
}
