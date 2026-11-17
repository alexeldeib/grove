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
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
)

func priorityTestPCS(namespace, pcsName string, labels map[string]string) *grovecorev1alpha1.PodCliqueSet {
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: pcsName, Namespace: namespace, Labels: labels},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{Name: "worker", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1}},
				},
			},
		},
	}
}

func priorityTestPodGang(namespace, podGang, pcsName string) *groveschedulerv1alpha1.PodGang {
	return &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podGang,
			Namespace:   namespace,
			UID:         "podgang-uid",
			Annotations: map[string]string{kueueWorkloadNameAnnotation: podGang},
			Labels:      map[string]string{apicommon.LabelPartOfKey: pcsName},
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{Name: podGang + "-worker", MinReplicas: 1}},
		},
	}
}

func workloadPriorityClass(name string, value int64) *unstructured.Unstructured {
	wpc := &unstructured.Unstructured{}
	wpc.SetGroupVersionKind(workloadPriorityClassGVK)
	wpc.SetName(name)
	wpc.Object["value"] = value
	return wpc
}

func TestBackendSyncPodGangSetsWorkloadPriority(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
		className = "inference-full-node"
	)
	pcs := priorityTestPCS(namespace, pcsName, map[string]string{
		queueNameLabel:         "queue-a",
		priorityClassNameLabel: className,
	})
	pg := priorityTestPodGang(namespace, podGang, pcsName)
	cl := testutils.NewTestClientBuilder().
		WithObjects(workloadPriorityClass(className, 2000000), pcs, pg).
		Build()
	backend := New(
		cl,
		cl.Scheme(),
		record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue},
	)
	require.NoError(t, backend.Init(nil))
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	priority, found, err := unstructured.NestedInt64(workload.Object, "spec", "priority")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, int64(2000000), priority)

	ref, found, err := unstructured.NestedStringMap(workload.Object, "spec", "priorityClassRef")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, map[string]string{
		"group": workloadPriorityClassGroup,
		"kind":  workloadPriorityClassKind,
		"name":  className,
	}, ref)
}

func TestBackendSyncPodGangWithoutPriorityClassLabel(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
	)
	pcs := priorityTestPCS(namespace, pcsName, map[string]string{queueNameLabel: "queue-a"})
	pg := priorityTestPodGang(namespace, podGang, pcsName)
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pg).Build()
	backend := New(
		cl,
		cl.Scheme(),
		record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue},
	)
	require.NoError(t, backend.Init(nil))
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	if _, found, _ := unstructured.NestedMap(workload.Object, "spec", "priorityClassRef"); found {
		t.Error("priorityClassRef should be unset when the PodCliqueSet has no priority-class label")
	}
	if _, found, _ := unstructured.NestedInt64(workload.Object, "spec", "priority"); found {
		t.Error("priority should be unset when the PodCliqueSet has no priority-class label")
	}
}
