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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newResyncFixture returns a legacy persisted Workload identity plus its PCS/PodGang. These
// evict-and-recreate tests verify upgrade compatibility; fresh names are covered by identity tests.
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
					Spec: grovecorev1alpha1.PodCliqueSpec{
						Replicas: 1,
						PodSpec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "worker", Image: "image-a",
						}}},
					},
				}},
			},
		},
	}
	pg := &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podGang,
			Namespace:   namespace,
			UID:         "podgang-uid",
			Annotations: map[string]string{kueueWorkloadNameAnnotation: podGang},
			Labels:      map[string]string{apicommon.LabelPartOfKey: pcsName},
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

func enrolledPodLabels(pcsName, podGang string) map[string]string {
	return map[string]string{
		apicommon.LabelManagedByKey: apicommon.LabelManagedByValue,
		apicommon.LabelPartOfKey:    pcsName,
		apicommon.LabelPodGang:      podGang,
		podGroupNameLabel:           podGang,
		prebuiltWorkloadNameLabel:   podGang,
	}
}

func TestBackendSyncPodGangHandlesPostAdmissionSpecDrift(t *testing.T) {
	tests := map[string]struct {
		mutate      func(*grovecorev1alpha1.PodCliqueSet)
		wantErrText string
	}{
		"runtime-only image drift preserves admission": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Image = "image-b"
			},
		},
		"resource request drift fails closed": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				}
			},
			wantErrText: "admission-relevant spec change",
		},
		"queue drift fails closed": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Labels[queueNameLabel] = "queue-c"
			},
			wantErrText: "admission-relevant spec change",
		},
		"count drift fails closed": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.Replicas = 2
			},
			wantErrText: "admission-relevant spec change",
		},
		"topology drift fails closed": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Annotations = map[string]string{
					podSetUnconstrainedTopologyAnnotation: "true",
				}
			},
			wantErrText: "admission-relevant spec change",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const (
				namespace = "default"
				podGang   = "demo-0"
			)
			cl, backend, pcs, pg := newResyncFixture(t)
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))
			originalSpec, _, err := unstructured.NestedMap(
				getTestWorkload(t, cl, namespace, podGang).Object,
				"spec",
			)
			require.NoError(t, err)

			setWorkloadCondition(t, cl, namespace, podGang, "QuotaReserved")
			test.mutate(pcs)
			require.NoError(t, cl.Update(t.Context(), pcs))

			err = backend.SyncPodGang(t.Context(), pg)
			if test.wantErrText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErrText)
			}
			retained := getTestWorkload(t, cl, namespace, podGang)
			retainedSpec, _, err := unstructured.NestedMap(retained.Object, "spec")
			require.NoError(t, err)
			assert.Equal(t, originalSpec, retainedSpec)
		})
	}
}

func TestBackendValidatePodCliqueSetUpdateAdmissionImmutability(t *testing.T) {
	tests := map[string]struct {
		setupOld func(*grovecorev1alpha1.PodCliqueSet)
		mutate   func(*grovecorev1alpha1.PodCliqueSet)
		wantErr  bool
	}{
		"runtime image drift is allowed": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Image = "image-b"
			},
		},
		"mixed PCS allows resource drift for admission-disabled clique": {
			setupOld: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques = append(pcs.Spec.Template.Cliques, &grovecorev1alpha1.PodCliqueTemplateSpec{
					Name:        "frontend",
					Annotations: map[string]string{kueueAdmissionDisabledAnnotation: "true"},
					Spec: grovecorev1alpha1.PodCliqueSpec{
						RoleName: "frontend",
						Replicas: 1,
						PodSpec: corev1.PodSpec{Containers: []corev1.Container{{
							Name: "frontend", Image: "image-a",
						}}},
					},
				})
			},
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[1].Spec.PodSpec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				}
			},
		},
		"resource request drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Requests = corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				}
			},
			wantErr: true,
		},
		"resource limit drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.PodSpec.Containers[0].Resources.Limits = corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				}
			},
			wantErr: true,
		},
		"clique replica drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Spec.Replicas = 2
			},
			wantErr: true,
		},
		"scaling group replica drift creates new PodGangs": {
			setupOld: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
					Name: "workers", Replicas: ptr.To(int32(1)), MinAvailable: ptr.To(int32(1)), CliqueNames: []string{"worker"},
				}}
			},
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
					Name: "workers", Replicas: ptr.To(int32(2)), MinAvailable: ptr.To(int32(1)), CliqueNames: []string{"worker"},
				}}
			},
		},
		"queue drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Labels[queueNameLabel] = "queue-b"
			},
			wantErr: true,
		},
		"workload priority class transition is rejected": {
			setupOld: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Labels[priorityClassNameLabel] = "old-priority"
			},
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Labels[priorityClassNameLabel] = "new-priority"
			},
			wantErr: true,
		},
		"Pod priority class drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.PriorityClassName = "new-priority"
			},
			wantErr: true,
		},
		"topology request drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Annotations = map[string]string{
					podSetUnconstrainedTopologyAnnotation: "true",
				}
			},
			wantErr: true,
		},
		"admission opt-out drift is rejected": {
			mutate: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].Annotations = map[string]string{
					kueueAdmissionDisabledAnnotation: "true",
				}
			},
			wantErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, backend, oldPCS, _ := newResyncFixture(t)
			if test.setupOld != nil {
				test.setupOld(oldPCS)
			}
			newPCS := oldPCS.DeepCopy()
			test.mutate(newPCS)

			validator, ok := backend.(scheduler.PodCliqueSetUpdateValidator)
			require.True(t, ok)
			err := validator.ValidatePodCliqueSetUpdate(t.Context(), oldPCS, newPCS)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackendValidatePodCliqueSetRejectsEnabledStandaloneAutoscaling(t *testing.T) {
	_, backend, pcs, _ := newResyncFixture(t)
	pcs.Spec.Template.Cliques[0].Spec.ScaleConfig = &grovecorev1alpha1.AutoScalingConfig{}
	require.ErrorContains(t, backend.ValidatePodCliqueSet(t.Context(), pcs), "autoscaling admission-enabled standalone PodCliques")

	pcs.Spec.Template.Cliques[0].Annotations = map[string]string{kueueAdmissionDisabledAnnotation: "true"}
	require.NoError(t, backend.ValidatePodCliqueSet(t.Context(), pcs))
}

func TestBackendSyncPodGangSkipsAllAdmissionDisabledPodGang(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, pcs, pg := newResyncFixture(t)
	delete(pcs.Labels, queueNameLabel)
	pcs.Spec.Template.Cliques[0].Annotations = map[string]string{kueueAdmissionDisabledAnnotation: "true"}
	require.NoError(t, cl.Update(t.Context(), pcs))
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	workload := newKueueWorkload(namespace, podGang)
	require.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(workload), workload)))
}

func TestBackendSyncPodGangDoesNotResurrectDeactivatedWorkload(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	workload := getTestWorkload(t, cl, namespace, podGang)
	require.NoError(t, unstructured.SetNestedField(workload.Object, false, "spec", "active"))
	require.NoError(t, cl.Update(t.Context(), workload))

	require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "refusing to reactivate")
	storedPodGang := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), storedPodGang))
	assert.Equal(t, kueueWorkloadStateDeactivated, storedPodGang.Annotations[kueueWorkloadStateAnnotation])

	require.NoError(t, cl.Delete(t.Context(), workload))
	require.ErrorContains(t, backend.SyncPodGang(t.Context(), storedPodGang), "after Kueue deactivated")
	require.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(workload), workload)))
}

func TestBackendSyncPodGangRepairsRecreatePendingState(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	storedPodGang := &groveschedulerv1alpha1.PodGang{}
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(pg), storedPodGang))
	storedPodGang.Annotations[kueueWorkloadStateAnnotation] = kueueWorkloadStateRecreate
	require.NoError(t, cl.Update(t.Context(), storedPodGang))

	require.NoError(t, backend.SyncPodGang(t.Context(), storedPodGang))
	require.NoError(t, cl.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: podGang}, storedPodGang))
	assert.Equal(t, kueueWorkloadStateCreated, storedPodGang.Annotations[kueueWorkloadStateAnnotation])
}

func TestBackendSyncPodGangRefusesUnownedWorkload(t *testing.T) {
	const (
		namespace = "default"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	workload := getTestWorkload(t, cl, namespace, podGang)
	workload.SetOwnerReferences(nil)
	require.NoError(t, cl.Update(t.Context(), workload))

	require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "not owned by PodGang UID")
	getTestWorkload(t, cl, namespace, podGang)
}

func TestBackendSyncPodGangMissingLegacyWorkload(t *testing.T) {
	tests := map[string]struct {
		gates   []corev1.PodSchedulingGate
		wantErr bool
	}{
		"ungated legacy Pod blocks creation": {
			wantErr: true,
		},
		"admission-gated initial Pod permits creation": {
			gates: []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const (
				namespace = "default"
				pcsName   = "demo"
				podGang   = "demo-0"
			)
			cl, backend, _, pg := newResyncFixture(t)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "member", Namespace: namespace, Labels: enrolledPodLabels(pcsName, podGang),
				},
				Spec:   corev1.PodSpec{SchedulingGates: test.gates},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			}
			require.NoError(t, cl.Create(t.Context(), pod))

			err := backend.SyncPodGang(t.Context(), pg)
			workload := newKueueWorkload(namespace, podGang)
			getErr := cl.Get(t.Context(), client.ObjectKeyFromObject(workload), workload)
			if test.wantErr {
				require.ErrorContains(t, err, "unmarked legacy PodGang")
				require.True(t, apierrors.IsNotFound(getErr))
			} else {
				require.NoError(t, err)
				require.NoError(t, getErr)
			}
		})
	}
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

func TestBackendSyncPodGangFinishedWorkloadWaitsForPodsThatConsumedAdmission(t *testing.T) {
	tests := map[string]struct {
		phase        corev1.PodPhase
		gates        []corev1.PodSchedulingGate
		omitPod      bool
		unrelatedPod bool
		terminating  bool
		wantBlocked  bool
	}{
		"unreferenced running pod blocks recreation": {
			phase:       corev1.PodRunning,
			wantBlocked: true,
		},
		"ungated pending pod blocks recreation": {
			phase:       corev1.PodPending,
			wantBlocked: true,
		},
		"admission-gated pending replacement permits recreation": {
			phase: corev1.PodPending,
			gates: []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}},
		},
		"topology-only gated pod blocks recreation": {
			phase:       corev1.PodPending,
			gates:       []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/topology"}},
			wantBlocked: true,
		},
		"terminating running pod blocks recreation": {
			phase:       corev1.PodRunning,
			terminating: true,
			wantBlocked: true,
		},
		"succeeded pod permits recreation": {
			phase: corev1.PodSucceeded,
		},
		"failed pod permits recreation": {
			phase: corev1.PodFailed,
		},
		"missing pod permits recreation": {
			omitPod: true,
		},
		"unrelated pod permits recreation": {
			phase:        corev1.PodRunning,
			unrelatedPod: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const (
				namespace = "default"
				podGang   = "demo-0"
			)
			cl, backend, _, pg := newResyncFixture(t)
			require.NoError(t, backend.SyncPodGang(t.Context(), pg))

			if !test.omitPod {
				labels := map[string]string{}
				if !test.unrelatedPod {
					labels = enrolledPodLabels("demo", podGang)
				}
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: "member", Namespace: namespace, Labels: labels,
					},
					Spec:   corev1.PodSpec{SchedulingGates: test.gates},
					Status: corev1.PodStatus{Phase: test.phase},
				}
				if test.terminating {
					pod.Finalizers = []string{"test/finalizer"}
				}
				require.NoError(t, cl.Create(t.Context(), pod))
				if test.terminating {
					require.NoError(t, cl.Delete(t.Context(), pod))
				}
			}

			setWorkloadCondition(t, cl, namespace, podGang, "Finished")

			err := backend.SyncPodGang(t.Context(), pg)
			workload := newKueueWorkload(namespace, podGang)
			getErr := cl.Get(t.Context(), client.ObjectKeyFromObject(workload), workload)
			if test.wantBlocked {
				require.ErrorContains(t, err, "non-terminal Pods")
				require.NoError(t, getErr, "finished Workload should be retained while Pods that consumed admission are live")
			} else {
				require.NoError(t, err)
				require.True(t, apierrors.IsNotFound(getErr), "finished Workload should be evicted, got %v", getErr)
			}
		})
	}
}

func TestBackendSyncPodGangFinishedWorkloadWaitsForUnreferencedOldPod(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
	)
	cl, backend, _, pg := newResyncFixture(t)
	require.NoError(t, backend.SyncPodGang(t.Context(), pg))

	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old", Namespace: namespace, Labels: enrolledPodLabels(pcsName, podGang),
			Finalizers: []string{"test/finalizer"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "replacement", Namespace: namespace, Labels: enrolledPodLabels(pcsName, podGang),
		},
		Spec:   corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: kueueAdmissionSchedulingGate}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	require.NoError(t, cl.Create(t.Context(), oldPod))
	require.NoError(t, cl.Delete(t.Context(), oldPod))
	require.NoError(t, cl.Create(t.Context(), replacement))
	setWorkloadCondition(t, cl, namespace, podGang, "Finished")

	require.ErrorContains(t, backend.SyncPodGang(t.Context(), pg), "non-terminal Pods")
	getTestWorkload(t, cl, namespace, podGang)

	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(oldPod), oldPod))
	oldPod.Finalizers = nil
	require.NoError(t, cl.Update(t.Context(), oldPod))
	if err := cl.Get(t.Context(), client.ObjectKeyFromObject(oldPod), oldPod); err == nil {
		require.NoError(t, cl.Delete(t.Context(), oldPod))
	} else {
		require.True(t, apierrors.IsNotFound(err))
	}

	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	deleted := newKueueWorkload(namespace, podGang)
	require.True(t, apierrors.IsNotFound(cl.Get(t.Context(), client.ObjectKeyFromObject(deleted), deleted)))

	require.NoError(t, backend.SyncPodGang(t.Context(), pg))
	recreated := getTestWorkload(t, cl, namespace, podGang)
	assert.Equal(t, "queue-a", nestedString(t, recreated, "spec", "queueName"))
	require.NoError(t, cl.Get(t.Context(), client.ObjectKeyFromObject(replacement), replacement))
	assert.True(t, podHasSchedulingGate(replacement, kueueAdmissionSchedulingGate))
}

func TestPodGangHasNonTerminalPodsUsesDirectClient(t *testing.T) {
	const (
		namespace = "default"
		pcsName   = "demo"
		podGang   = "demo-0"
	)
	_, backend, _, pg := newResyncFixture(t)
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old", Namespace: namespace, Labels: enrolledPodLabels(pcsName, podGang),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	directClient := testutils.NewTestClientBuilder().WithObjects(oldPod, pg).Build()
	concrete := backend.(*schedulerBackend)
	concrete.directClient = directClient

	hasNonTerminalPods, err := concrete.podGangHasNonTerminalPods(t.Context(), pg)
	require.NoError(t, err)
	assert.True(t, hasNonTerminalPods)
}
