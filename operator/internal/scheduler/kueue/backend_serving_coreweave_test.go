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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestServingPodPreparationPreservesSelectiveAdmission(t *testing.T) {
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", Labels: map[string]string{queueNameLabel: "queue"}},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
			Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
				{Name: "leader", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1}},
				{Name: "follower", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1}},
				{Name: "frontend", Annotations: map[string]string{kueueAdmissionDisabledAnnotation: "true"}, Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 2}},
			},
		}},
	}
	pg := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-0", Namespace: "default", UID: "gang", Labels: map[string]string{apicommon.LabelPartOfKey: "demo"}},
		Spec: groveschedulerv1alpha1.PodGangSpec{PodGroups: []groveschedulerv1alpha1.PodGroup{
			{Name: "demo-0-leader", MinReplicas: 1}, {Name: "demo-0-follower", MinReplicas: 1}, {Name: "demo-0-frontend", MinReplicas: 1},
		}},
	}
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pg).Build()
	b := New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue})
	require.NoError(t, b.Init(cl))
	require.NoError(t, b.SyncPodGang(t.Context(), pg))
	for _, name := range []string{"leader", "follower", "frontend"} {
		t.Run(name, func(t *testing.T) {
			pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "demo-0-" + name, Namespace: "default"}}
			if name == "frontend" {
				pclq.Annotations = map[string]string{kueueAdmissionDisabledAnnotation: "true"}
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Annotations: map[string]string{retriableInGroupAnnotation: "false"}}}
			require.NoError(t, b.PreparePod(pod, scheduler.PodPreparationContext{PodCliqueSet: pcs, PodClique: pclq, PodGang: pg, PodGangName: pg.Name}))
			if name == "frontend" {
				assert.Empty(t, pod.Labels[prebuiltWorkloadNameLabel])
				assert.Empty(t, pod.Annotations[podGroupServingAnnotation])
			} else {
				assert.Equal(t, "true", pod.Annotations[podGroupServingAnnotation])
				assert.NotContains(t, pod.Annotations, retriableInGroupAnnotation)
				assert.Equal(t, "2", pod.Annotations[podGroupTotalCountAnnotation])
			}
		})
	}
}

func TestServingFinalizerCleanupRetainsOtherOwnersAndReadinessDoesNotReleaseSlot(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "leader", Namespace: "default", Finalizers: []string{podFinalizer, "example.com/retain"},
			Labels: map[string]string{prebuiltWorkloadNameLabel: "gang"}, Annotations: map[string]string{podGroupServingAnnotation: "true"}},
		Spec:   corev1.PodSpec{NodeName: "node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "main", RestartCount: 12, Ready: false}}},
	}
	cl := testutils.NewTestClientBuilder().WithObjects(pod).Build()
	b := New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue}).(*schedulerBackend)
	assert.False(t, b.IsPodFinalized(pod), "a Running/NotReady leader still occupies its old slot")
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		terminated := pod.DeepCopy()
		terminated.Status.Phase = phase
		now := metav1.Now()
		terminated.DeletionTimestamp = &now
		assert.True(t, b.IsPodFinalized(terminated), "Kueue 0.19.4 excludes a deleting terminal serving pod")
	}
	succeeded := pod.DeepCopy()
	succeeded.Status.Phase = corev1.PodSucceeded
	assert.False(t, b.IsPodFinalized(succeeded), "Succeeded still counts until deletion has been observed")
	require.NoError(t, b.RemovePodFinalizers(context.Background(), pod))
	actual := &corev1.Pod{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(pod), actual))
	assert.Equal(t, []string{"example.com/retain"}, actual.Finalizers)
	assert.Equal(t, int32(12), actual.Status.ContainerStatuses[0].RestartCount)
	require.NoError(t, b.RemovePodFinalizers(context.Background(), actual))
}

func TestWorkloadDeletionRejectsStaleSameUIDState(t *testing.T) {
	wl := newKueueWorkload("default", "gang")
	wl.SetUID("lease")
	wl.Object["spec"] = map[string]any{"active": true}
	cl := testutils.NewTestClientBuilder().WithObjects(wl).Build()
	stale := newKueueWorkload("default", "gang")
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(wl), stale))
	current := stale.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(current.Object, false, "spec", "active"))
	require.NoError(t, cl.Update(context.Background(), current))
	b := New(cl, cl.Scheme(), record.NewFakeRecorder(10), configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue}).(*schedulerBackend)
	err := b.deleteWorkload(context.Background(), stale)
	require.True(t, apierrors.IsConflict(err), "a stale reconcile must not delete a lease whose state changed")
	retained := newKueueWorkload("default", "gang")
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(wl), retained))
	assert.False(t, workloadActive(retained))
}
