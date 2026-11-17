/*
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
// */

package kueue

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	queueNameLabel                   = "kueue.x-k8s.io/queue-name"
	podGroupNameLabel                = "kueue.x-k8s.io/pod-group-name"
	podGroupTotalCountAnnotation     = "kueue.x-k8s.io/pod-group-total-count"
	prebuiltWorkloadNameLabel        = "kueue.x-k8s.io/prebuilt-workload-name"
	podGroupServingAnnotation        = "kueue.x-k8s.io/pod-group-serving"
	retriableInGroupAnnotation       = "kueue.x-k8s.io/retriable-in-group"
	podFinalizer                     = "kueue.x-k8s.io/managed"
	podSetRequiredTopologyAnnotation = "kueue.x-k8s.io/podset-required-topology"
	kueueAdmissionSchedulingGate     = "kueue.x-k8s.io/admission"
	isGroupWorkloadAnnotation        = "kueue.x-k8s.io/is-group-workload"
	roleHashAnnotation               = "kueue.x-k8s.io/role-hash"
	kueueAdmissionDisabledAnnotation = "grove.io/kueue-admission-disabled"
	priorityClassNameLabel           = "kueue.x-k8s.io/priority-class"
	kueueWorkloadStateAnnotation     = "grove.io/kueue-workload-state"
	kueueWorkloadStateCreated        = "created"
	kueueWorkloadStateRecreate       = "recreate-pending"
	kueueWorkloadStateDeactivated    = "deactivated"
	// admissionGatedByAnnotation is Kueue's admission gate (KEP-6915, feature AdmissionGatedBy,
	// beta and on by default from Kueue 0.19). While it lists any gate, Kueue will not reserve quota
	// for the Workload. Kueue's webhook accepts gates only when the Workload is created; after that
	// they can only be removed.
	admissionGatedByAnnotation = "kueue.x-k8s.io/admission-gated-by"
	// basePodGangAdmissionGate is the gate Grove holds on a scaled PodGang's Workload until the base
	// PodGang of its PodCliqueSet replica is scheduled.
	basePodGangAdmissionGate = "grove.io/base-podgang"
)

const (
	workloadPriorityClassGroup = "kueue.x-k8s.io"
	workloadPriorityClassKind  = "WorkloadPriorityClass"
)

const podSetUnconstrainedTopologyAnnotation = "kueue.x-k8s.io/podset-unconstrained-topology"

const (
	podGroupLifecycleAnnotation = "grove.io/kueue-pod-group-lifecycle"
	podGroupLifecycleServing    = "serving"
	podGroupLifecycleLegacy     = "legacy"
)

var workloadGVK = schema.GroupVersionKind{
	Group:   "kueue.x-k8s.io",
	Version: "v1beta2",
	Kind:    "Workload",
}

var workloadPriorityClassGVK = schema.GroupVersionKind{
	Group:   workloadPriorityClassGroup,
	Version: "v1beta2",
	Kind:    workloadPriorityClassKind,
}

type schedulerBackend struct {
	client        client.Client
	directClient  client.Client
	scheme        *runtime.Scheme
	name          string
	eventRecorder record.EventRecorder
	profile       configv1alpha1.SchedulerProfile
	config        configv1alpha1.KueueSchedulerConfiguration
	// admissionGatesForCreate returns the Kueue admission gates a newly created Workload for the
	// PodGang must carry. Nil means none. It is consulted only on the create path, because Kueue
	// rejects adding a gate to an existing Workload.
	admissionGatesForCreate func(context.Context, *groveschedulerv1alpha1.PodGang) ([]string, error)
}

var _ scheduler.Backend = (*schedulerBackend)(nil)
var _ scheduler.Finalizer = (*schedulerBackend)(nil)
var _ scheduler.PodCliqueSetUpdateValidator = (*schedulerBackend)(nil)

// New creates the Kueue scheduler backend.
func New(cl client.Client, scheme *runtime.Scheme, eventRecorder record.EventRecorder, profile configv1alpha1.SchedulerProfile) scheduler.Backend {
	b := &schedulerBackend{
		client:        cl,
		scheme:        scheme,
		name:          string(profile.Name),
		eventRecorder: eventRecorder,
		profile:       profile,
		config:        defaultConfig(),
	}
	b.admissionGatesForCreate = b.baseOrderingAdmissionGates
	return b
}

func (b *schedulerBackend) Name() string {
	return b.name
}

func (b *schedulerBackend) Init(directClient client.Client) error {
	b.directClient = directClient
	if b.profile.Config == nil || len(b.profile.Config.Raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(b.profile.Config.Raw, &b.config); err != nil {
		return fmt.Errorf("failed to unmarshal kueue scheduler config: %w", err)
	}
	b.config = withDefaults(b.config)
	return nil
}

// SyncPodGang builds a prebuilt Kueue Workload for every PodGang, mapping each PodGroup to a Kueue podSet.
// Standalone PodCliques express partial-gang semantics via minCount == minAvailable, while PodCliques that
// belong to a PodCliqueScalingGroup are treated as all-or-nothing (minCount == count) under the POC
// assumption that PCSG.minAvailable == PCSG.replicas.
func (b *schedulerBackend) SyncPodGang(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) error {
	workloadState := podGang.Annotations[kueueWorkloadStateAnnotation]
	if workloadState == kueueWorkloadStateDeactivated {
		return fmt.Errorf("refusing to recreate Kueue Workload for PodGang %s/%s after Kueue deactivated its prior lease", podGang.Namespace, podGang.Name)
	}
	pcsName := podGang.Labels[apicommon.LabelPartOfKey]
	if pcsName == "" {
		return nil
	}
	pcs := &grovecorev1alpha1.PodCliqueSet{}
	if err := b.client.Get(ctx, client.ObjectKey{Namespace: podGang.Namespace, Name: pcsName}, pcs); err != nil {
		return fmt.Errorf("failed to get PodCliqueSet %s/%s for PodGang %s: %w", podGang.Namespace, pcsName, podGang.Name, err)
	}

	desired, err := b.buildPrebuiltWorkload(ctx, pcs, podGang)
	if err != nil {
		return err
	}
	if desired == nil {
		return nil
	}
	basePodGangName := basePodGangNameFor(podGang)
	if basePodGangName == "" {
		if err = b.releaseGatedScaledPodGangs(ctx, podGang); err != nil {
			return err
		}
	}
	existing := newKueueWorkload(podGang.Namespace, podGang.Name)
	if err = b.client.Get(ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		if apierrors.IsNotFound(err) {
			// The latch check at the top of SyncPodGang reads the cached PodGang, so a reconcile that
			// predates the latch can arrive here. Recreating would reuse an identity whose quota and
			// topology assignment Kueue has already released, so confirm the state live first.
			if workloadState, err = b.podGangWorkloadStateUncached(ctx, podGang); err != nil {
				return err
			}
			if workloadState == kueueWorkloadStateDeactivated {
				return fmt.Errorf("refusing to recreate Kueue Workload for PodGang %s/%s after Kueue deactivated its prior lease", podGang.Namespace, podGang.Name)
			}
			if workloadState == kueueWorkloadStateCreated {
				return fmt.Errorf("refusing to recreate unexpectedly missing prebuilt kueue Workload %s/%s under the same PodGang identity", podGang.Namespace, podGang.Name)
			}
			if workloadState == "" {
				hasNonTerminalPods, listErr := b.podGangHasNonTerminalPods(ctx, podGang)
				if listErr != nil {
					return listErr
				}
				if hasNonTerminalPods {
					return fmt.Errorf("refusing to create missing prebuilt kueue Workload %s/%s for an unmarked legacy PodGang while Pods still consume a prior admission", podGang.Namespace, podGang.Name)
				}
			}
			var gates []string
			if b.admissionGatesForCreate != nil {
				if gates, err = b.admissionGatesForCreate(ctx, podGang); err != nil {
					return err
				}
			}
			setAdmissionGates(desired, gates)
			// Choose once per lease, before any member can be created. Even a
			// finished legacy lease can leave terminal members in the API.
			mode, modeErr := b.observedPodGroupLifecycle(ctx, podGang, podGroupLifecycleServing)
			if modeErr != nil {
				return modeErr
			}
			annotations := desired.GetAnnotations()
			annotations[podGroupLifecycleAnnotation] = mode
			desired.SetAnnotations(annotations)
			if err = b.client.Create(ctx, desired); err != nil {
				return fmt.Errorf("failed to create prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
			}
			if err = b.setPodGangWorkloadState(ctx, podGang, kueueWorkloadStateCreated); err != nil {
				return err
			}
			log.FromContext(ctx).Info("Created prebuilt Kueue Workload",
				"workload", client.ObjectKeyFromObject(desired), "admissionGates", gates)
			if slices.Contains(gates, basePodGangAdmissionGate) {
				// The base may have become scheduled between the decision and the create, and its
				// release can miss a Workload it cannot see yet. Re-check live so one side always wins.
				return b.releaseIfBaseScheduled(ctx, desired, podGang.Namespace, basePodGangName, b.liveReader())
			}
			return nil
		}
		return fmt.Errorf("failed to get prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	// An evicted Workload lingers until Kueue clears its finalizer; recreating now would race the
	// pending deletion, so wait for it to disappear and let a later reconcile recreate it.
	if existing.GetDeletionTimestamp() != nil {
		return nil
	}
	if !workloadOwnedByPodGang(existing, podGang) {
		return fmt.Errorf("refusing to manage prebuilt kueue Workload %s/%s because it is not owned by PodGang UID %q", existing.GetNamespace(), existing.GetName(), podGang.UID)
	}
	if basePodGangName != "" {
		if err = b.releaseIfBaseScheduled(ctx, existing, podGang.Namespace, basePodGangName, b.client); err != nil {
			return err
		}
	}
	if !workloadActive(existing) {
		if err = b.setPodGangWorkloadState(ctx, podGang, kueueWorkloadStateDeactivated); err != nil {
			return err
		}
	} else if workloadState == "" ||
		(workloadState == kueueWorkloadStateRecreate && !workloadFinished(existing)) {
		if err = b.setPodGangWorkloadState(ctx, podGang, kueueWorkloadStateCreated); err != nil {
			return err
		}
	}
	if err := b.syncPreemptibleLabel(ctx, existing, desired); err != nil {
		return err
	}
	return b.updatePrebuiltWorkloadIfNeeded(ctx, podGang, existing, desired)
}

func workloadOwnedByPodGang(workload *unstructured.Unstructured, podGang *groveschedulerv1alpha1.PodGang) bool {
	if podGang.UID == "" {
		return false
	}
	for _, ownerReference := range workload.GetOwnerReferences() {
		if ownerReference.UID == podGang.UID && ownerReference.Kind == "PodGang" {
			return true
		}
	}
	return false
}

func (b *schedulerBackend) setPodGangWorkloadState(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
	state string,
) error {
	if podGang.Annotations[kueueWorkloadStateAnnotation] == state {
		return nil
	}
	before := podGang.DeepCopy()
	if podGang.Annotations == nil {
		podGang.Annotations = make(map[string]string)
	}
	podGang.Annotations[kueueWorkloadStateAnnotation] = state
	if err := b.client.Patch(ctx, podGang, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to set Kueue Workload state %q on PodGang %s/%s: %w", state, podGang.Namespace, podGang.Name, err)
	}
	return nil
}

// podGangWorkloadStateUncached re-reads the PodGang's Workload state annotation past the informer
// cache and refreshes the caller's copy with it, so the decisions that follow agree with the API
// server. A PodGang that no longer exists reports the cached value.
func (b *schedulerBackend) podGangWorkloadStateUncached(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) (string, error) {
	reader := b.directClient
	if reader == nil {
		reader = b.client
	}
	live := &groveschedulerv1alpha1.PodGang{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(podGang), live); err != nil {
		if apierrors.IsNotFound(err) {
			return podGang.Annotations[kueueWorkloadStateAnnotation], nil
		}
		return "", fmt.Errorf("failed to re-read PodGang %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	state := live.Annotations[kueueWorkloadStateAnnotation]
	if podGang.Annotations == nil {
		podGang.Annotations = map[string]string{}
	}
	if state == "" {
		delete(podGang.Annotations, kueueWorkloadStateAnnotation)
	} else {
		podGang.Annotations[kueueWorkloadStateAnnotation] = state
	}
	return state, nil
}

// workloadAdmissionGates returns the gates listed in a Workload's admission-gated-by annotation.
func workloadAdmissionGates(workload *unstructured.Unstructured) []string {
	var gates []string
	for _, gate := range strings.Split(workload.GetAnnotations()[admissionGatedByAnnotation], ",") {
		if gate = strings.TrimSpace(gate); gate != "" {
			gates = append(gates, gate)
		}
	}
	return gates
}

// setAdmissionGates writes gates onto a Workload that is about to be created. It is only valid
// before creation; on an existing Workload use releaseAdmissionGate.
func setAdmissionGates(workload *unstructured.Unstructured, gates []string) {
	if len(gates) == 0 {
		return
	}
	annotations := workload.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[admissionGatedByAnnotation] = strings.Join(gates, ",")
	workload.SetAnnotations(annotations)
}

// releaseAdmissionGate removes one gate from an existing Workload and deletes the annotation once no
// gates remain, which is the only change Kueue accepts after creation. Gates owned by other
// controllers are kept. It writes with Update, like the rest of this backend: the operator's role
// grants update but not patch on Workloads, and the resourceVersion check turns a concurrent write
// into a retried conflict. It reports whether the Workload was changed.
func (b *schedulerBackend) releaseAdmissionGate(ctx context.Context, workload *unstructured.Unstructured, gate string) (bool, error) {
	gates := workloadAdmissionGates(workload)
	remaining := make([]string, 0, len(gates))
	for _, g := range gates {
		if g != gate {
			remaining = append(remaining, g)
		}
	}
	if len(remaining) == len(gates) {
		return false, nil
	}
	annotations := workload.GetAnnotations()
	if len(remaining) == 0 {
		delete(annotations, admissionGatedByAnnotation)
	} else {
		annotations[admissionGatedByAnnotation] = strings.Join(remaining, ",")
	}
	workload.SetAnnotations(annotations)
	if err := b.client.Update(ctx, workload); err != nil {
		return false, fmt.Errorf("failed to release admission gate %q on prebuilt kueue Workload %s/%s: %w", gate, workload.GetNamespace(), workload.GetName(), err)
	}
	return true, nil
}

// liveReader returns the client that bypasses the informer cache when one is configured.
func (b *schedulerBackend) liveReader() client.Reader {
	if b.directClient != nil {
		return b.directClient
	}
	return b.client
}

// pcsReplicaIndexOf recovers the PodCliqueSet replica a PodGang belongs to. Grove names a base
// PodGang <pcs>-<replicaIndex> and its scaled siblings <pcs>-<replicaIndex>-<scalingGroup>-<n>, so
// the index is the first segment after the PodCliqueSet name. The replica-index label is preferred
// where present.
func pcsReplicaIndexOf(podGang *groveschedulerv1alpha1.PodGang, pcsName string) (string, bool) {
	if index, found := podGang.Labels[apicommon.LabelPodCliqueSetReplicaIndex]; found && index != "" {
		return index, true
	}
	suffix, found := strings.CutPrefix(podGang.Name, pcsName+"-")
	if !found {
		return "", false
	}
	index, _, _ := strings.Cut(suffix, "-")
	if index == "" {
		return "", false
	}
	if _, err := strconv.ParseUint(index, 10, 32); err != nil {
		return "", false
	}
	return index, true
}

// basePodGangNameFor returns the base PodGang this one waits for, or "" if it is itself a base. It is
// derived from names rather than read from apicommon.LabelBasePodGang on a PodClique, because
// PodCliques are created concurrently with PodGangs and a scaled PodGang synced before its PodClique
// exists would otherwise read as a base.
func basePodGangNameFor(podGang *groveschedulerv1alpha1.PodGang) string {
	pcsName := podGang.Labels[apicommon.LabelPartOfKey]
	if pcsName == "" {
		return ""
	}
	replicaIndex, ok := pcsReplicaIndexOf(podGang, pcsName)
	if !ok {
		return ""
	}
	basePodGangName := fmt.Sprintf("%s-%s", pcsName, replicaIndex)
	if podGang.Name == basePodGangName {
		return ""
	}
	return basePodGangName
}

// isBasePodGangScheduled mirrors the predicate that lifts grove.io/podgang-pending-creation from a
// scaled PodGang's Pods: every PodGroup of the base must have at least MinReplicas scheduled. A base
// or PodClique that does not exist yet counts as unscheduled.
func isBasePodGangScheduled(ctx context.Context, reader client.Reader, namespace, basePodGangName string) (bool, error) {
	basePodGang := &groveschedulerv1alpha1.PodGang{}
	baseKey := client.ObjectKey{Namespace: namespace, Name: basePodGangName}
	if err := reader.Get(ctx, baseKey, basePodGang); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get base PodGang %v: %w", baseKey, err)
	}
	for _, podGroup := range basePodGang.Spec.PodGroups {
		pclq := &grovecorev1alpha1.PodClique{}
		pclqKey := client.ObjectKey{Namespace: namespace, Name: podGroup.Name}
		if err := reader.Get(ctx, pclqKey, pclq); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("failed to get PodClique %v for base PodGang readiness: %w", pclqKey, err)
		}
		if pclq.Status.ScheduledReplicas < podGroup.MinReplicas {
			return false, nil
		}
	}
	return true, nil
}

// baseOrderingAdmissionGates gates a scaled PodGang's new Workload on its base being scheduled.
// Kueue admits equal-priority Workloads in creation order and Grove creates a base and its scaled
// siblings within the same second, so without the gate a sibling can take the quota its base needs
// while Grove refuses to schedule the sibling until the base is up. Bases and siblings whose base is
// already scheduled are not gated. A Workload is only created once no Pod holds a prior admission, so
// gating it cannot strand running Pods.
func (b *schedulerBackend) baseOrderingAdmissionGates(ctx context.Context, podGang *groveschedulerv1alpha1.PodGang) ([]string, error) {
	basePodGangName := basePodGangNameFor(podGang)
	if basePodGangName == "" {
		return nil, nil
	}
	baseScheduled, err := isBasePodGangScheduled(ctx, b.client, podGang.Namespace, basePodGangName)
	if err != nil || baseScheduled {
		return nil, err
	}
	return []string{basePodGangAdmissionGate}, nil
}

// releaseIfBaseScheduled removes the base-PodGang gate from a scaled PodGang's Workload once its base
// is scheduled. reader decides how fresh the base read must be.
func (b *schedulerBackend) releaseIfBaseScheduled(
	ctx context.Context,
	workload *unstructured.Unstructured,
	namespace, basePodGangName string,
	reader client.Reader,
) error {
	if !slices.Contains(workloadAdmissionGates(workload), basePodGangAdmissionGate) {
		return nil
	}
	baseScheduled, err := isBasePodGangScheduled(ctx, reader, namespace, basePodGangName)
	if err != nil || !baseScheduled {
		return err
	}
	released, err := b.releaseAdmissionGate(ctx, workload, basePodGangAdmissionGate)
	if released {
		log.FromContext(ctx).Info("Released prebuilt Kueue Workload after its base PodGang was scheduled",
			"workload", client.ObjectKeyFromObject(workload), "basePodGang", basePodGangName)
	}
	return err
}

// releaseGatedScaledPodGangs lifts the base-PodGang gate from every scaled sibling in this base's
// PodCliqueSet replica once the base is scheduled. A gated sibling produces no events of its own, so
// the base has to push the release. Gates are only ever removed, so a cached Workload without the
// gate is authoritative; only a Workload missing from the cache is confirmed live, in case it was
// created after the cache last synced.
func (b *schedulerBackend) releaseGatedScaledPodGangs(ctx context.Context, basePodGang *groveschedulerv1alpha1.PodGang) error {
	baseScheduled, err := isBasePodGangScheduled(ctx, b.client, basePodGang.Namespace, basePodGang.Name)
	if err != nil || !baseScheduled {
		return err
	}
	pcsName := basePodGang.Labels[apicommon.LabelPartOfKey]
	baseReplicaIndex, ok := pcsReplicaIndexOf(basePodGang, pcsName)
	if pcsName == "" || !ok {
		return nil
	}
	podGangs := &groveschedulerv1alpha1.PodGangList{}
	if err = b.client.List(ctx, podGangs, client.InNamespace(basePodGang.Namespace),
		client.MatchingLabels{apicommon.LabelPartOfKey: pcsName}); err != nil {
		return fmt.Errorf("failed to list PodGangs of PodCliqueSet %s/%s: %w", basePodGang.Namespace, pcsName, err)
	}
	for i := range podGangs.Items {
		scaled := &podGangs.Items[i]
		if scaled.Name == basePodGang.Name {
			continue
		}
		if index, ok := pcsReplicaIndexOf(scaled, pcsName); !ok || index != baseReplicaIndex {
			continue
		}
		workload := newKueueWorkload(scaled.Namespace, scaled.Name)
		key := client.ObjectKeyFromObject(workload)
		if err = b.client.Get(ctx, key, workload); apierrors.IsNotFound(err) {
			err = b.liveReader().Get(ctx, key, workload)
		}
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to get prebuilt kueue Workload %v while releasing it: %w", key, err)
		}
		if err = b.releaseIfBaseScheduled(ctx, workload, basePodGang.Namespace, basePodGang.Name, b.client); err != nil {
			return err
		}
	}
	return nil
}

func newKueueWorkload(namespace, name string) *unstructured.Unstructured {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(workloadGVK)
	workload.SetNamespace(namespace)
	workload.SetName(name)
	return workload
}

// updatePrebuiltWorkloadIfNeeded reconciles the Grove-owned Workload fields while preserving metadata and
// status managed by Kueue. Before quota reservation the Workload can be updated in place. Afterwards,
// runtime-only Pod changes retain the live admission, admission-relevant changes fail closed, and a Finished
// Workload is recreated only after its referenced Pods drain because Kueue has already released its accounting.
func (b *schedulerBackend) updatePrebuiltWorkloadIfNeeded(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
	existing, desired *unstructured.Unstructured,
) error {
	existingSpec, _, err := unstructured.NestedMap(existing.Object, "spec")
	if err != nil {
		return fmt.Errorf("failed to read existing prebuilt kueue Workload %s/%s spec: %w", existing.GetNamespace(), existing.GetName(), err)
	}
	desiredSpec, _, err := unstructured.NestedMap(desired.Object, "spec")
	if err != nil {
		return fmt.Errorf("failed to read desired prebuilt kueue Workload %s/%s spec: %w", desired.GetNamespace(), desired.GetName(), err)
	}
	specChanged := !equality.Semantic.DeepEqual(existingSpec, desiredSpec)
	if !workloadActive(existing) {
		return fmt.Errorf(
			"refusing to reactivate prebuilt kueue Workload %s/%s under the same PodGang identity after Kueue deactivated it",
			existing.GetNamespace(), existing.GetName(),
		)
	}
	if workloadFinished(existing) {
		hasNonTerminalPods, err := b.podGangHasNonTerminalPods(ctx, podGang)
		if err != nil {
			return err
		}
		if hasNonTerminalPods {
			// Kueue removes a Finished Workload from quota and TAS accounting even while the API object
			// remains. Recreating it while an old member Pod is still live can therefore assign the same
			// capacity twice. Fail closed until every Pod enrolled in the old admission has drained.
			return fmt.Errorf(
				"refusing to recreate finished prebuilt kueue Workload %s/%s while PodGang %s has non-terminal Pods; Kueue has already released its admission accounting",
				existing.GetNamespace(), existing.GetName(), podGang.Name,
			)
		}
		if err := b.setPodGangWorkloadState(ctx, podGang, kueueWorkloadStateRecreate); err != nil {
			return err
		}
		if err := b.deleteWorkload(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to evict stale prebuilt kueue Workload %s/%s for PodGang %s: %w", existing.GetNamespace(), existing.GetName(), podGang.Name, err)
		}
		log.FromContext(ctx).Info("Evicted stale prebuilt Kueue Workload for recreation", "workload", client.ObjectKeyFromObject(existing))
		return nil
	}
	if specChanged && workloadHasQuotaReservation(existing) {
		existingAdmissionShape, err := workloadAdmissionShape(existingSpec)
		if err != nil {
			return fmt.Errorf("failed to read existing admission shape for prebuilt kueue Workload %s/%s: %w", existing.GetNamespace(), existing.GetName(), err)
		}
		desiredAdmissionShape, err := workloadAdmissionShape(desiredSpec)
		if err != nil {
			return fmt.Errorf("failed to read desired admission shape for prebuilt kueue Workload %s/%s: %w", desired.GetNamespace(), desired.GetName(), err)
		}
		if !equality.Semantic.DeepEqual(existingAdmissionShape, desiredAdmissionShape) {
			return fmt.Errorf(
				"refusing to replace quota-reserved prebuilt kueue Workload %s/%s after an admission-relevant spec change; create a new PodGang and Workload identity",
				existing.GetNamespace(), existing.GetName(),
			)
		}
		// Image, command, environment, probes, volumes, and similar runtime-only fields are outside
		// Kueue's admission shape. Keep the existing Workload podSets and their live TAS accounting
		// while Grove rolls replacement Pods that consume the same admission.
		log.FromContext(ctx).Info(
			"Preserving quota-reserved prebuilt Kueue Workload across runtime-only Pod spec drift",
			"workload", client.ObjectKeyFromObject(existing),
		)
		return nil
	}

	originalAnnotations := existing.GetAnnotations()
	annotations := make(map[string]string, len(originalAnnotations)+len(desired.GetAnnotations()))
	for key, value := range originalAnnotations {
		annotations[key] = value
	}
	for key, value := range desired.GetAnnotations() {
		annotations[key] = value
	}
	metadataChanged := !equality.Semantic.DeepEqual(originalAnnotations, annotations)
	existing.SetAnnotations(annotations)

	originalOwnerReferences := existing.GetOwnerReferences()
	if err := controllerutil.SetOwnerReference(podGang, existing, b.scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on existing prebuilt kueue Workload %s/%s: %w", existing.GetNamespace(), existing.GetName(), err)
	}
	metadataChanged = metadataChanged || !equality.Semantic.DeepEqual(originalOwnerReferences, existing.GetOwnerReferences())

	if !specChanged && !metadataChanged {
		return nil
	}
	if specChanged {
		if err := unstructured.SetNestedMap(existing.Object, desiredSpec, "spec"); err != nil {
			return fmt.Errorf("failed to set desired prebuilt kueue Workload %s/%s spec: %w", existing.GetNamespace(), existing.GetName(), err)
		}
	}
	if err := b.client.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update prebuilt kueue Workload %s/%s: %w", existing.GetNamespace(), existing.GetName(), err)
	}
	log.FromContext(ctx).Info("Updated prebuilt Kueue Workload", "workload", client.ObjectKeyFromObject(existing))
	return nil
}

// podGangHasNonTerminalPods lists every live API Pod enrolled in this PodGang's prebuilt Workload. PodGang
// references intentionally omit terminating predecessors during rolling replacement, so they are not a safe
// drain inventory. A Pod still carrying Kueue's admission gate has not consumed the old assignment and does not
// block replacement; every nonterminal Pod without that gate, including topology-gated or terminating Pods, does.
func (b *schedulerBackend) podGangHasNonTerminalPods(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
) (bool, error) {
	pods := &corev1.PodList{}
	reader := b.directClient
	if reader == nil {
		reader = b.client
	}
	if err := reader.List(
		ctx,
		pods,
		client.InNamespace(podGang.Namespace),
		client.MatchingLabels{
			apicommon.LabelManagedByKey: apicommon.LabelManagedByValue,
			apicommon.LabelPartOfKey:    podGang.Labels[apicommon.LabelPartOfKey],
			apicommon.LabelPodGang:      podGang.Name,
			podGroupNameLabel:           podGang.Name,
			prebuiltWorkloadNameLabel:   podGang.Name,
		},
	); err != nil {
		return false, fmt.Errorf("failed to list Pods enrolled in prebuilt Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed &&
			!podHasSchedulingGate(pod, kueueAdmissionSchedulingGate) {
			return true, nil
		}
	}
	return false, nil
}

func (b *schedulerBackend) deleteWorkload(ctx context.Context, workload *unstructured.Unstructured) error {
	uid := workload.GetUID()
	if uid == "" {
		return b.client.Delete(ctx, workload)
	}
	preconditions := client.Preconditions{UID: &uid}
	if version := workload.GetResourceVersion(); version != "" {
		preconditions.ResourceVersion = &version
	}
	return b.client.Delete(ctx, workload, preconditions)
}

func podHasSchedulingGate(pod *corev1.Pod, name string) bool {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == name {
			return true
		}
	}
	return false
}

// workloadAdmissionShape returns the Workload fields that affect Kueue quota and placement. The PodSpec
// subset mirrors Kueue's util/pod.SpecShape contract; container limits are additionally retained because
// Kubernetes defaults a missing request from its limit when creating the actual Pod.
func workloadAdmissionShape(spec map[string]any) (map[string]any, error) {
	shape := make(map[string]any, 5)
	for _, fieldName := range []string{"active", "queueName", "priority", "priorityClassRef"} {
		if value, found := spec[fieldName]; found {
			shape[fieldName] = runtime.DeepCopyJSONValue(value)
		}
	}
	podSets, found, err := unstructured.NestedSlice(spec, "podSets")
	if err != nil {
		return nil, err
	}
	if !found {
		return shape, nil
	}
	for i, rawPodSet := range podSets {
		podSet, ok := rawPodSet.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("podSet %d is not an object", i)
		}
		template, found, err := unstructured.NestedMap(podSet, "template")
		if err != nil {
			return nil, fmt.Errorf("podSet %d template: %w", i, err)
		}
		if !found {
			continue
		}
		podSpecMap, found, err := unstructured.NestedMap(template, "spec")
		if err != nil {
			return nil, fmt.Errorf("podSet %d template spec: %w", i, err)
		}
		if !found {
			continue
		}
		podSpec := &corev1.PodSpec{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(podSpecMap, podSpec); err != nil {
			return nil, fmt.Errorf("podSet %d template spec conversion: %w", i, err)
		}
		template["spec"] = kueuePodSpecShape(podSpec)
		podSet["template"] = template
		podSets[i] = podSet
	}
	shape["podSets"] = podSets
	return shape, nil
}
func kueuePodSpecShape(spec *corev1.PodSpec) map[string]any {
	shape := map[string]any{
		"initContainers":            kueueContainerShapes(spec.InitContainers),
		"containers":                kueueContainerShapes(spec.Containers),
		"nodeSelector":              spec.NodeSelector,
		"affinity":                  spec.Affinity,
		"tolerations":               spec.Tolerations,
		"runtimeClassName":          spec.RuntimeClassName,
		"priority":                  spec.Priority,
		"topologySpreadConstraints": spec.TopologySpreadConstraints,
		"overhead":                  spec.Overhead,
		"resourceClaims":            spec.ResourceClaims,
	}
	if spec.Resources != nil {
		shape["resources"] = spec.Resources.Requests
	}
	return shape
}

func kueueContainerShapes(containers []corev1.Container) (shapes []map[string]any) {
	for _, container := range containers {
		shapes = append(shapes, map[string]any{
			"resources": map[string]any{"requests": container.Resources.Requests},
			"limits":    container.Resources.Limits,
			"ports":     container.Ports,
		})
	}
	return shapes
}

func workloadHasQuotaReservation(workload *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(workload.Object, "status", "conditions")
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if ok && condition["type"] == "QuotaReserved" && condition["status"] == "True" {
			return true
		}
	}
	admission, found, _ := unstructured.NestedMap(workload.Object, "status", "admission")
	return found && len(admission) > 0
}

func workloadActive(workload *unstructured.Unstructured) bool {
	active, found, err := unstructured.NestedBool(workload.Object, "spec", "active")
	return err == nil && (!found || active)
}

// workloadFinished reports whether Kueue has finalized the Workload. Kueue sets this terminal condition
// when a prebuilt Workload falls out of sync with its pod group,
// after which it can never re-admit and must be recreated.
func workloadFinished(workload *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(workload.Object, "status", "conditions")
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if ok && condition["type"] == "Finished" && condition["status"] == "True" {
			return true
		}
	}
	return false
}

// buildPrebuiltWorkload constructs a prebuilt Kueue Workload from a PodGang, mapping each PodGroup to a Kueue
// podSet. The podSet name is the PodGroup FQN so scaling-group replicas of the same clique stay unique. count
// is the clique replicas; minCount is the PodGroup minReplicas for standalone cliques and is forced to count
// for scaling-group cliques (POC assumption PCSG.minAvailable == PCSG.replicas).
func (b *schedulerBackend) buildPrebuiltWorkload(ctx context.Context, pcs *grovecorev1alpha1.PodCliqueSet, podGang *groveschedulerv1alpha1.PodGang) (*unstructured.Unstructured, error) {
	queueName := pcs.Labels[queueNameLabel]

	// Kueue permits at most one podSet per Workload to set minCount (partial admission is limited to a single
	// podSet). Grove therefore only sets minCount on the first podSet whose minCount is strictly less than its
	// count; all other podSets (including every PodCliqueScalingGroup clique, which is all-or-nothing in this POC)
	// omit minCount, which Kueue defaults to count.
	minCountUsed := false
	podSets := make([]any, 0, len(podGang.Spec.PodGroups))
	for _, podGroup := range podGang.Spec.PodGroups {
		cliqueTemplate := findCliqueTemplateForPodGroup(pcs, podGroup.Name)
		if cliqueTemplate == nil {
			return nil, fmt.Errorf("no clique template found for PodGroup %q in PodCliqueSet %s/%s", podGroup.Name, pcs.Namespace, pcs.Name)
		}
		if kueueAdmissionDisabled(cliqueTemplate.Annotations) {
			continue
		}
		podSpec, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cliqueTemplate.Spec.PodSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to convert pod spec for clique %q: %w", cliqueTemplate.Name, err)
		}
		topologyKey := scheduler.RequiredTopologyKeyForPodGroup(podGang, podGroup.Name, b.config.RequiredTopologyKey)
		// Kueue normally derives this request while constructing a Workload from
		// plain Pods. Grove owns the prebuilt Workload, so preserve the standard
		// PodClique template annotation as the equivalent PodSet request.
		unconstrainedTopology := topologyKey == "" &&
			cliqueTemplate.Annotations[podSetUnconstrainedTopologyAnnotation] == "true"

		// PodTemplateSpec metadata is defaulted to an empty object by the API
		// server. Emit it explicitly so a subsequent reconciliation does not
		// mistake that harmless default for immutable Workload spec drift after
		// Kueue reserves quota.
		template := map[string]any{
			"metadata": map[string]any{},
			"spec":     podSpec,
		}
		if topologyKey != "" {
			template["metadata"] = map[string]any{
				"annotations": map[string]any{podSetRequiredTopologyAnnotation: topologyKey},
			}
		} else if unconstrainedTopology {
			template["metadata"] = map[string]any{
				"annotations": map[string]any{podSetUnconstrainedTopologyAnnotation: "true"},
			}
		}

		count := int64(cliqueTemplate.Spec.Replicas)
		minCount := int64(podGroup.MinReplicas)

		podSet := map[string]any{
			"name":     podGroup.Name,
			"count":    count,
			"template": template,
		}
		if !isCliqueInScalingGroup(pcs, cliqueTemplate.Name) && minCount > 0 && minCount < count && !minCountUsed {
			podSet["minCount"] = minCount
			minCountUsed = true
		}
		if topologyKey != "" {
			topologyRequest := map[string]any{"required": topologyKey}
			if groupName := topologyGroupNameForPodGroup(podGang, podGroup.Name); groupName != "" {
				topologyRequest["podSetGroupName"] = groupName
			}
			podSet["topologyRequest"] = topologyRequest
		} else if unconstrainedTopology {
			podSet["topologyRequest"] = map[string]any{"unconstrained": true}
		}
		podSets = append(podSets, podSet)
	}
	if len(podSets) == 0 {
		return nil, nil
	}
	if queueName == "" {
		return nil, fmt.Errorf("PodCliqueSet %s/%s must set label %q for Kueue queue selection", pcs.Namespace, pcs.Name, queueNameLabel)
	}

	workload := newKueueWorkload(podGang.Namespace, podGang.Name)
	workload.SetAnnotations(map[string]string{isGroupWorkloadAnnotation: "true"})
	if err := unstructured.SetNestedField(workload.Object, true, "spec", "active"); err != nil {
		return nil, err
	}
	if err := unstructured.SetNestedField(workload.Object, queueName, "spec", "queueName"); err != nil {
		return nil, err
	}
	if err := unstructured.SetNestedSlice(workload.Object, podSets, "spec", "podSets"); err != nil {
		return nil, err
	}
	if err := applyPreemptionProtection(workload, pcs, podGang); err != nil {
		return nil, err
	}
	if err := b.applyWorkloadPriority(ctx, workload, pcs); err != nil {
		return nil, err
	}
	// Own the Workload by the PodGang so Kubernetes garbage collection removes it once the PodGang and all
	// member Pods (which Kueue adds as owners on admission) are gone.
	if err := controllerutil.SetOwnerReference(podGang, workload, b.scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	return workload, nil
}

// applyWorkloadPriority resolves the WorkloadPriorityClass named by the PodCliqueSet's priority-class
// label and stamps both priorityClassRef and priority on the Workload. Kueue only populates priority
// from the class for Workloads it builds from a Job; Grove owns the prebuilt Workload, and the API
// requires priority to be set whenever priorityClassRef is, so it is resolved here. A missing label
// leaves the Workload at the default priority.
func (b *schedulerBackend) applyWorkloadPriority(ctx context.Context, workload *unstructured.Unstructured, pcs *grovecorev1alpha1.PodCliqueSet) error {
	name := pcs.Labels[priorityClassNameLabel]
	if name == "" {
		return nil
	}

	wpc := &unstructured.Unstructured{}
	wpc.SetGroupVersionKind(workloadPriorityClassGVK)
	if err := b.client.Get(ctx, client.ObjectKey{Name: name}, wpc); err != nil {
		return fmt.Errorf("failed to get WorkloadPriorityClass %q for PodCliqueSet %s/%s: %w", name, pcs.Namespace, pcs.Name, err)
	}
	value, found, err := unstructured.NestedInt64(wpc.Object, "value")
	if err != nil {
		return fmt.Errorf("WorkloadPriorityClass %q has a non-integer value: %w", name, err)
	}
	if !found {
		return fmt.Errorf("WorkloadPriorityClass %q is missing the required value field", name)
	}

	priorityClassRef := map[string]any{
		"group": workloadPriorityClassGroup,
		"kind":  workloadPriorityClassKind,
		"name":  name,
	}
	if err := unstructured.SetNestedMap(workload.Object, priorityClassRef, "spec", "priorityClassRef"); err != nil {
		return err
	}
	return unstructured.SetNestedField(workload.Object, value, "spec", "priority")
}

// topologyGroupNameForPodGroup returns the Grove topology constraint group that contains the PodGroup.
// Kueue uses podSetGroupName to place all PodSets in that group within the same topology domain.
func topologyGroupNameForPodGroup(podGang *groveschedulerv1alpha1.PodGang, podGroupName string) string {
	for _, group := range podGang.Spec.TopologyConstraintGroupConfigs {
		for _, name := range group.PodGroupNames {
			if name == podGroupName {
				return group.Name
			}
		}
	}
	topologyConstraint := podGang.Spec.TopologyConstraint
	if topologyConstraint != nil &&
		topologyConstraint.PackConstraint != nil &&
		topologyConstraint.PackConstraint.Required != nil &&
		*topologyConstraint.PackConstraint.Required != "" {
		return podGang.Name
	}
	return ""
}

// isCliqueInScalingGroup reports whether the named clique is a member of any PodCliqueScalingGroup.
func isCliqueInScalingGroup(pcs *grovecorev1alpha1.PodCliqueSet, cliqueName string) bool {
	for _, config := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		for _, name := range config.CliqueNames {
			if name == cliqueName {
				return true
			}
		}
	}
	return false
}

// findCliqueTemplateForPodGroup resolves the PodClique template whose role name matches the PodGroup FQN.
// PodGroup names are fully qualified (<pcs>-<replicaIndex>-<cliqueName>), so the longest suffix match wins.
func findCliqueTemplateForPodGroup(pcs *grovecorev1alpha1.PodCliqueSet, podGroupName string) *grovecorev1alpha1.PodCliqueTemplateSpec {
	var best *grovecorev1alpha1.PodCliqueTemplateSpec
	for _, cliqueTemplate := range pcs.Spec.Template.Cliques {
		if podGroupName == cliqueTemplate.Name || strings.HasSuffix(podGroupName, "-"+cliqueTemplate.Name) {
			if best == nil || len(cliqueTemplate.Name) > len(best.Name) {
				best = cliqueTemplate
			}
		}
	}
	return best
}

func (b *schedulerBackend) PreparePod(pod *corev1.Pod, preparation scheduler.PodPreparationContext) error {
	if preparation.PodCliqueSet == nil {
		return fmt.Errorf("kueue backend requires PodCliqueSet when preparing a Pod")
	}
	if preparation.PodClique == nil {
		return fmt.Errorf("kueue backend requires PodClique when preparing a Pod")
	}
	if preparation.PodGangName == "" {
		return fmt.Errorf("kueue backend requires PodGang name when preparing a Pod")
	}
	if preparation.PodGang == nil {
		return fmt.Errorf("PodGang %s/%s is required when preparing a Pod for Kueue", pod.Namespace, preparation.PodGangName)
	}

	pod.Spec.SchedulerName = b.config.UnderlyingSchedulerName
	if kueueAdmissionDisabled(preparation.PodClique.Annotations) {
		delete(pod.Labels, queueNameLabel)
		return nil
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	queueName := preparation.PodCliqueSet.Labels[queueNameLabel]
	if queueName == "" {
		return fmt.Errorf("PodCliqueSet %s/%s must set label %q for Kueue queue selection", preparation.PodCliqueSet.Namespace, preparation.PodCliqueSet.Name, queueNameLabel)
	}
	pod.Labels[queueNameLabel] = queueName
	pod.Labels[podGroupNameLabel] = preparation.PodGangName
	pod.Labels[prebuiltWorkloadNameLabel] = preparation.PodGangName
	podGangTotalCount, err := podGangTotalPodCount(preparation.PodCliqueSet, preparation.PodGang)
	if err != nil {
		return err
	}
	pod.Annotations[podGroupTotalCountAnnotation] = strconv.Itoa(podGangTotalCount)
	pod.Annotations[roleHashAnnotation] = preparation.PodClique.Name
	// Never mix lifecycle modes during an operator-only upgrade. The Workload
	// records the choice even after every old member has disappeared.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mode, err := b.podGroupLifecycle(ctx, preparation.PodGang)
	if err != nil {
		return err
	}
	if mode == podGroupLifecycleServing {
		pod.Annotations[podGroupServingAnnotation] = "true"
		delete(pod.Annotations, retriableInGroupAnnotation)
	} else {
		delete(pod.Annotations, podGroupServingAnnotation)
		pod.Annotations[retriableInGroupAnnotation] = "false"
	}
	if topologyKey := scheduler.RequiredTopologyKeyForPodGroup(preparation.PodGang, preparation.PodClique.Name, b.config.RequiredTopologyKey); topologyKey != "" && pod.Annotations[podSetRequiredTopologyAnnotation] == "" {
		pod.Annotations[podSetRequiredTopologyAnnotation] = topologyKey
	}
	return nil
}

// podGroupLifecycle migrates unmarked leases using an uncached inventory. An
// empty existing lease stays legacy: absence of cached/live members is not an
// activation signal. Optimistic locking serializes simultaneous clique upgrades.
func (b *schedulerBackend) podGroupLifecycle(ctx context.Context, pg *groveschedulerv1alpha1.PodGang) (string, error) {
	wl := newKueueWorkload(pg.Namespace, pg.Name)
	if err := b.liveReader().Get(ctx, client.ObjectKeyFromObject(wl), wl); err != nil {
		return "", fmt.Errorf("cannot choose pod lifecycle before reading Workload %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	if !workloadOwnedByPodGang(wl, pg) || wl.GetDeletionTimestamp() != nil {
		return "", fmt.Errorf("cannot choose pod lifecycle for unowned or deleting Workload %s/%s", pg.Namespace, pg.Name)
	}
	mode := wl.GetAnnotations()[podGroupLifecycleAnnotation]
	if mode == podGroupLifecycleServing || mode == podGroupLifecycleLegacy {
		return mode, nil
	}
	if mode != "" {
		return "", fmt.Errorf("unknown pod lifecycle %q on Workload %s/%s", mode, pg.Namespace, pg.Name)
	}
	mode, err := b.observedPodGroupLifecycle(ctx, pg, podGroupLifecycleLegacy)
	if err != nil {
		return "", err
	}
	annotations := wl.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[podGroupLifecycleAnnotation] = mode
	wl.SetAnnotations(annotations)
	// Workload RBAC grants update, not patch. Keep the resourceVersion from
	// the live read so a competing migration or controller update conflicts.
	if err := b.client.Update(ctx, wl); err != nil {
		return "", fmt.Errorf("failed to persist pod lifecycle on Workload %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	return mode, nil
}

func (b *schedulerBackend) observedPodGroupLifecycle(ctx context.Context, pg *groveschedulerv1alpha1.PodGang, emptyMode string) (string, error) {
	pods := &corev1.PodList{}
	if err := b.liveReader().List(ctx, pods, client.InNamespace(pg.Namespace), client.MatchingLabels{prebuiltWorkloadNameLabel: pg.Name}); err != nil {
		return "", fmt.Errorf("failed to inventory lifecycle of PodGang %s/%s: %w", pg.Namespace, pg.Name, err)
	}
	mode := ""
	for _, pod := range pods.Items {
		memberMode := podGroupLifecycleLegacy
		if pod.Annotations[podGroupServingAnnotation] == "true" {
			memberMode = podGroupLifecycleServing
		}
		if mode != "" && mode != memberMode {
			return "", fmt.Errorf("PodGang %s/%s has mixed pod lifecycle modes; controlled group recreation is required", pg.Namespace, pg.Name)
		}
		mode = memberMode
	}
	if mode == "" {
		mode = emptyMode
	}
	return mode, nil
}

// RemovePodFinalizers releases only Kueue's finalizer; other owners retain theirs.
func (b *schedulerBackend) RemovePodFinalizers(ctx context.Context, pod *corev1.Pod) error {
	if !controllerutil.ContainsFinalizer(pod, podFinalizer) {
		return nil
	}
	patched := pod.DeepCopy()
	controllerutil.RemoveFinalizer(patched, podFinalizer)
	if err := b.client.Patch(ctx, patched, client.MergeFromWithOptions(pod, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to remove Kueue finalizer from Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// IsPodFinalized matches Kueue 0.19.4's serving-group deletion predicate.
// Bound old processes must terminate before replacements can consume the same slot.
func (b *schedulerBackend) IsPodFinalized(pod *corev1.Pod) bool {
	return pod.Labels[prebuiltWorkloadNameLabel] == "" || pod.Status.Phase == corev1.PodFailed ||
		(pod.DeletionTimestamp != nil && (pod.Spec.NodeName == "" ||
			(pod.Annotations[podGroupServingAnnotation] == "true" && pod.Status.Phase == corev1.PodSucceeded)))
}

func podGangTotalPodCount(pcs *grovecorev1alpha1.PodCliqueSet, podGang *groveschedulerv1alpha1.PodGang) (int, error) {
	total := 0
	for _, podGroup := range podGang.Spec.PodGroups {
		cliqueTemplate := findCliqueTemplateForPodGroup(pcs, podGroup.Name)
		if cliqueTemplate == nil {
			return 0, fmt.Errorf("no clique template found for PodGroup %q in PodCliqueSet %s/%s", podGroup.Name, pcs.Namespace, pcs.Name)
		}
		if kueueAdmissionDisabled(cliqueTemplate.Annotations) {
			continue
		}
		total += int(cliqueTemplate.Spec.Replicas)
	}
	return total, nil
}

func kueueAdmissionDisabled(annotations map[string]string) bool {
	return annotations[kueueAdmissionDisabledAnnotation] == "true"
}

// ValidatePodCliqueSet rejects PodCliqueSets that cannot be represented as a valid Kueue Workload.
// Grove builds one prebuilt Kueue Workload per PodGang and maps each standalone PodClique with
// minAvailable < replicas to a Kueue podSet that sets minCount. Kueue permits at most one podSet per
// Workload to set minCount, so at most one standalone PodClique may declare partial-gang
// (minAvailable < replicas) semantics. PodCliques that belong to a PodCliqueScalingGroup are
// all-or-nothing and never set minCount, so they are excluded from this check.
func (b *schedulerBackend) ValidatePodCliqueSet(_ context.Context, pcs *grovecorev1alpha1.PodCliqueSet) error {
	if _, err := protectedReplicaCounts(pcs); err != nil {
		return err
	}
	var (
		partialGangCliques     []string
		partialGangPCSGCliques []string
		autoscaledCliques      []string
	)
	for _, cliqueTemplate := range pcs.Spec.Template.Cliques {
		if cliqueTemplate == nil {
			continue
		}
		if kueueAdmissionDisabled(cliqueTemplate.Annotations) {
			continue
		}
		if cliqueTemplate.Spec.ScaleConfig != nil && !isCliqueInScalingGroup(pcs, cliqueTemplate.Name) {
			autoscaledCliques = append(autoscaledCliques, cliqueTemplate.Name)
		}
		minAvailable := cliqueTemplate.Spec.MinAvailable
		// A nil minAvailable defaults to replicas, so it is a full gang.
		if minAvailable == nil {
			continue
		}
		isPartialGang := *minAvailable > 0 && *minAvailable < cliqueTemplate.Spec.Replicas
		if !isPartialGang {
			continue
		}
		if isCliqueInScalingGroup(pcs, cliqueTemplate.Name) {
			partialGangPCSGCliques = append(partialGangPCSGCliques, cliqueTemplate.Name)
			continue
		}
		partialGangCliques = append(partialGangCliques, cliqueTemplate.Name)
	}
	if len(autoscaledCliques) > 0 {
		return fmt.Errorf("kueue backend does not support autoscaling admission-enabled standalone PodCliques because it changes an existing Workload's PodSet count: %s", strings.Join(autoscaledCliques, ", "))
	}

	// PodCliques that belong to a PodCliqueScalingGroup are treated as all-or-nothing by the Kueue backend:
	// their podSets never set minCount, so a member clique's minAvailable < replicas would be silently
	// ignored. Reject it so an accepted PodCliqueSet behaves as specified.
	if len(partialGangPCSGCliques) > 0 {
		return fmt.Errorf("kueue backend requires PodCliques that are members of a PodCliqueScalingGroup to set minAvailable == replicas (all-or-nothing), but the following set minAvailable < replicas: %s", strings.Join(partialGangPCSGCliques, ", "))
	}

	// Grove maps each standalone PodClique with minAvailable < replicas to a Kueue podSet that sets minCount,
	// and Kueue permits minCount on at most one podSet per Workload.
	if len(partialGangCliques) > 1 {
		return fmt.Errorf("kueue backend allows at most one standalone PodClique with minAvailable < replicas because Kueue permits minCount on at most one podSet per Workload, but found %d: %s", len(partialGangCliques), strings.Join(partialGangCliques, ", "))
	}
	return nil
}

// ValidatePodCliqueSetUpdate prevents a rolling update from changing the admission represented by an
// existing prebuilt Workload. Runtime-only Pod fields remain mutable, but queue, per-PodGang count, topology, and
// Kueue's PodSpec scheduling shape require a new PodCliqueSet so every Pod gets a new Workload identity.
func (b *schedulerBackend) ValidatePodCliqueSetUpdate(
	_ context.Context,
	oldPCS, newPCS *grovecorev1alpha1.PodCliqueSet,
) error {
	if _, err := protectedReplicaCounts(newPCS); err != nil {
		return err
	}
	oldCliques := make(map[string]*grovecorev1alpha1.PodCliqueTemplateSpec, len(oldPCS.Spec.Template.Cliques))
	for _, clique := range oldPCS.Spec.Template.Cliques {
		if clique != nil {
			oldCliques[clique.Name] = clique
		}
	}
	hasKueueEnabledClique := false
	for _, newClique := range newPCS.Spec.Template.Cliques {
		if newClique == nil {
			continue
		}
		oldClique := oldCliques[newClique.Name]
		if oldClique == nil {
			continue
		}
		oldAdmissionDisabled := kueueAdmissionDisabled(oldClique.Annotations)
		newAdmissionDisabled := kueueAdmissionDisabled(newClique.Annotations)
		if oldAdmissionDisabled != newAdmissionDisabled {
			return fmt.Errorf("annotation %q is immutable for Kueue-scheduled PodClique %q; create a new PodCliqueSet to change admission", kueueAdmissionDisabledAnnotation, newClique.Name)
		}
		if newAdmissionDisabled {
			continue
		}
		hasKueueEnabledClique = true
		if oldClique.Spec.Replicas != newClique.Spec.Replicas {
			return fmt.Errorf("replicas are immutable for Kueue-scheduled PodClique %q; create a new PodCliqueSet to change admission", newClique.Name)
		}
		for _, annotation := range []string{
			podSetRequiredTopologyAnnotation,
			podSetUnconstrainedTopologyAnnotation,
		} {
			if oldClique.Annotations[annotation] != newClique.Annotations[annotation] {
				return fmt.Errorf("annotation %q is immutable for Kueue-scheduled PodClique %q; create a new PodCliqueSet to change admission", annotation, newClique.Name)
			}
		}
		if oldClique.Spec.PodSpec.PriorityClassName != newClique.Spec.PodSpec.PriorityClassName ||
			!equality.Semantic.DeepEqual(
				kueuePodSpecShape(&oldClique.Spec.PodSpec),
				kueuePodSpecShape(&newClique.Spec.PodSpec),
			) {
			return fmt.Errorf("PodSpec admission shape is immutable for Kueue-scheduled PodClique %q; create a new PodCliqueSet to change admission", newClique.Name)
		}
	}
	if hasKueueEnabledClique {
		for _, label := range []string{queueNameLabel, priorityClassNameLabel} {
			if oldPCS.Labels[label] != newPCS.Labels[label] {
				return fmt.Errorf("label %q is immutable for Kueue-scheduled PodCliqueSets; create a new PodCliqueSet to change admission", label)
			}
		}
		if oldPCS.Spec.Template.PriorityClassName != newPCS.Spec.Template.PriorityClassName {
			return fmt.Errorf("priorityClassName is immutable for Kueue-scheduled PodCliqueSets; create a new PodCliqueSet to change admission")
		}
	}
	return nil
}

func defaultConfig() configv1alpha1.KueueSchedulerConfiguration {
	return configv1alpha1.KueueSchedulerConfiguration{
		UnderlyingSchedulerName: string(configv1alpha1.SchedulerNameKube),
	}
}

func withDefaults(cfg configv1alpha1.KueueSchedulerConfiguration) configv1alpha1.KueueSchedulerConfiguration {
	defaults := defaultConfig()
	if cfg.UnderlyingSchedulerName == "" {
		cfg.UnderlyingSchedulerName = defaults.UnderlyingSchedulerName
	}
	return cfg
}
