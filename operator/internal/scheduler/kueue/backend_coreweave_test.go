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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBackendSyncPodGangTopologyGroupAndReconciliation(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
		groupName = "demo-0-workers-0"
		rackKey   = "ds.coreweave.com/nvlink.domain"
	)
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pcsName,
			Namespace: namespace,
			Labels:    map[string]string{queueNameLabel: "queue-a"},
		},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{Name: "leader", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1}},
					{Name: "worker", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 2}},
				},
				PodCliqueScalingGroupConfigs: []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
					Name:        "workers",
					CliqueNames: []string{"leader", "worker"},
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
			PodGroups: []groveschedulerv1alpha1.PodGroup{
				{Name: groupName + "-leader", MinReplicas: 1},
				{Name: groupName + "-worker", MinReplicas: 2},
			},
			TopologyConstraintGroupConfigs: []groveschedulerv1alpha1.TopologyConstraintGroupConfig{{
				Name:          groupName,
				PodGroupNames: []string{groupName + "-leader", groupName + "-worker"},
				TopologyConstraint: &groveschedulerv1alpha1.TopologyConstraint{
					PackConstraint: &groveschedulerv1alpha1.TopologyPackConstraint{Required: stringPtr(rackKey)},
				},
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

	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
	workload := getTestWorkload(t, cl, namespace, podGang)
	assert.Equal(t, "true", workload.GetAnnotations()[isGroupWorkloadAnnotation])
	active, found, err := unstructured.NestedBool(workload.Object, "spec", "active")
	require.NoError(t, err)
	assert.True(t, found)
	assert.True(t, active)
	assert.Equal(t, "queue-a", nestedString(t, workload, "spec", "queueName"))

	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, podSets, 2)
	for _, rawPodSet := range podSets {
		podSet, ok := rawPodSet.(map[string]any)
		require.True(t, ok)
		topologyRequest, ok := podSet["topologyRequest"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, rackKey, topologyRequest["required"])
		assert.Equal(t, groupName, topologyRequest["podSetGroupName"])
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace}}
	require.NoError(t, backend.PreparePod(pod, scheduler.PodPreparationContext{
		PodCliqueSet: pcs,
		PodClique: &grovecorev1alpha1.PodClique{
			ObjectMeta: metav1.ObjectMeta{Name: groupName + "-worker", Namespace: namespace},
		},
		PodGang:     pg,
		PodGangName: podGang,
	}))
	assert.Equal(t, "3", pod.Annotations[podGroupTotalCountAnnotation])
	assert.Equal(t, rackKey, pod.Annotations[podSetRequiredTopologyAnnotation])

	workload.SetAnnotations(map[string]string{"kueue.x-k8s.io/foreign": "preserve"})
	require.NoError(t, cl.Update(context.Background(), workload))
	pcs.Labels[queueNameLabel] = "queue-b"
	require.NoError(t, cl.Update(context.Background(), pcs))
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload = getTestWorkload(t, cl, namespace, podGang)
	assert.Equal(t, "queue-b", nestedString(t, workload, "spec", "queueName"))
	assert.Equal(t, "preserve", workload.GetAnnotations()["kueue.x-k8s.io/foreign"])
	assert.Equal(t, "true", workload.GetAnnotations()[isGroupWorkloadAnnotation])

	require.NoError(t, unstructured.SetNestedSlice(workload.Object, []any{map[string]any{
		"type":   "QuotaReserved",
		"status": "True",
	}}, "status", "conditions"))
	require.NoError(t, cl.Update(context.Background(), workload))
	pcs.Labels[queueNameLabel] = "queue-c"
	require.NoError(t, cl.Update(context.Background(), pcs))

	// Queue selection is admission-relevant. Replacing the live Workload would release its quota and
	// topology accounting, so the backend fails closed and retains the existing admission.
	err = backend.SyncPodGang(context.Background(), pg)
	require.ErrorContains(t, err, "admission-relevant spec change")
	workload = getTestWorkload(t, cl, namespace, podGang)
	assert.Equal(t, "queue-b", nestedString(t, workload, "spec", "queueName"))
}

func TestBackendBuildPrebuiltWorkloadUnconstrainedTopology(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "single-node"
		podGang   = "single-node-0"
		podGroup  = "single-node-0-worker"
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
					Annotations: map[string]string{
						podSetUnconstrainedTopologyAnnotation: "true",
					},
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
				Name: podGroup, MinReplicas: 1,
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
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, podSets, 1)
	podSet, ok := podSets[0].(map[string]any)
	require.True(t, ok)
	topologyRequest, ok := podSet["topologyRequest"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, topologyRequest["unconstrained"])
	assert.NotContains(t, topologyRequest, "required")
	assert.NotContains(t, topologyRequest, "podSetGroupName")
	annotation, found, err := unstructured.NestedString(
		podSet,
		"template",
		"metadata",
		"annotations",
		podSetUnconstrainedTopologyAnnotation,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "true", annotation)
}

func TestBackendExcludesCliqueFromKueueAdmission(t *testing.T) {
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
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{
						Name: "frontend",
						Annotations: map[string]string{
							kueueAdmissionDisabledAnnotation: "true",
						},
						Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1},
					},
					{Name: "worker", Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 2}},
				},
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
			PodGroups: []groveschedulerv1alpha1.PodGroup{
				{Name: "demo-0-frontend", MinReplicas: 1},
				{Name: "demo-0-worker", MinReplicas: 2},
			},
		},
	}
	cl := testutils.NewTestClientBuilder().WithObjects(pcs, pg).Build()
	backend := New(
		cl,
		cl.Scheme(),
		record.NewFakeRecorder(10),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameKueue},
	).(*schedulerBackend)
	backend.config.UnderlyingSchedulerName = "binpack-scheduler"
	require.NoError(t, backend.Init(nil))
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, podSets, 1)
	assert.Equal(t, "demo-0-worker", podSets[0].(map[string]any)["name"])

	frontendPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Labels:    map[string]string{queueNameLabel: "queue-a", "keep": "value"},
	}}
	require.NoError(t, backend.PreparePod(frontendPod, scheduler.PodPreparationContext{
		PodCliqueSet: pcs,
		PodClique: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
			Name:        "demo-0-frontend",
			Namespace:   namespace,
			Annotations: map[string]string{kueueAdmissionDisabledAnnotation: "true"},
		}},
		PodGang:     pg,
		PodGangName: podGang,
	}))
	assert.Equal(t, "binpack-scheduler", frontendPod.Spec.SchedulerName)
	assert.NotContains(t, frontendPod.Labels, queueNameLabel)
	assert.Equal(t, "value", frontendPod.Labels["keep"])
	assert.Empty(t, frontendPod.Annotations)

	workerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace}}
	require.NoError(t, backend.PreparePod(workerPod, scheduler.PodPreparationContext{
		PodCliqueSet: pcs,
		PodClique: &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-0-worker",
			Namespace: namespace,
		}},
		PodGang:     pg,
		PodGangName: podGang,
	}))
	assert.Equal(t, "2", workerPod.Annotations[podGroupTotalCountAnnotation])
	assert.Equal(t, podGang, workerPod.Labels[prebuiltWorkloadNameLabel])
}

func TestBackendSyncPodGangIgnoresDefaultedEmptyTemplateMetadata(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "plain"
		podGang   = "plain-0"
		podGroup  = "plain-0-frontend"
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
					Name: "frontend",
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
				Name: podGroup, MinReplicas: 1,
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
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))

	workload := getTestWorkload(t, cl, namespace, podGang)
	podSets, found, err := unstructured.NestedSlice(workload.Object, "spec", "podSets")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, podSets, 1)
	podSet, ok := podSets[0].(map[string]any)
	require.True(t, ok)
	metadata, found, err := unstructured.NestedMap(podSet, "template", "metadata")
	require.NoError(t, err)
	require.True(t, found)
	assert.Empty(t, metadata)

	// Model the API server's PodTemplateSpec default and Kueue's immutable
	// boundary after quota reservation. Reconciliation must remain a no-op.
	require.NoError(t, unstructured.SetNestedSlice(workload.Object, []any{map[string]any{
		"type":   "QuotaReserved",
		"status": "True",
	}}, "status", "conditions"))
	require.NoError(t, cl.Update(context.Background(), workload))
	require.NoError(t, backend.SyncPodGang(context.Background(), pg))
}

func getTestWorkload(t *testing.T, cl client.Client, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	workload := newKueueWorkload(namespace, name)
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(workload), workload))
	return workload
}

func nestedString(t *testing.T, obj *unstructured.Unstructured, fields ...string) string {
	t.Helper()
	value, found, err := unstructured.NestedString(obj.Object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	return value
}

func stringPtr(value string) *string {
	return &value
}
