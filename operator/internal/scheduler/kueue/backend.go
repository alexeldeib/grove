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
	"strconv"
	"strings"

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
	podSetRequiredTopologyAnnotation = "kueue.x-k8s.io/podset-required-topology"
	isGroupWorkloadAnnotation        = "kueue.x-k8s.io/is-group-workload"
	roleHashAnnotation               = "kueue.x-k8s.io/role-hash"
	kueueAdmissionDisabledAnnotation = "grove.io/kueue-admission-disabled"
)

const podSetUnconstrainedTopologyAnnotation = "kueue.x-k8s.io/podset-unconstrained-topology"

var workloadGVK = schema.GroupVersionKind{
	Group:   "kueue.x-k8s.io",
	Version: "v1beta2",
	Kind:    "Workload",
}

type schedulerBackend struct {
	client        client.Client
	scheme        *runtime.Scheme
	name          string
	eventRecorder record.EventRecorder
	profile       configv1alpha1.SchedulerProfile
	config        configv1alpha1.KueueSchedulerConfiguration
}

var _ scheduler.Backend = (*schedulerBackend)(nil)

// New creates the Kueue scheduler backend.
func New(cl client.Client, scheme *runtime.Scheme, eventRecorder record.EventRecorder, profile configv1alpha1.SchedulerProfile) scheduler.Backend {
	return &schedulerBackend{
		client:        cl,
		scheme:        scheme,
		name:          string(profile.Name),
		eventRecorder: eventRecorder,
		profile:       profile,
		config:        defaultConfig(),
	}
}

func (b *schedulerBackend) Name() string {
	return b.name
}

func (b *schedulerBackend) Init(_ client.Client) error {
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
	pcsName := podGang.Labels[apicommon.LabelPartOfKey]
	if pcsName == "" {
		return nil
	}
	pcs := &grovecorev1alpha1.PodCliqueSet{}
	if err := b.client.Get(ctx, client.ObjectKey{Namespace: podGang.Namespace, Name: pcsName}, pcs); err != nil {
		return fmt.Errorf("failed to get PodCliqueSet %s/%s for PodGang %s: %w", podGang.Namespace, pcsName, podGang.Name, err)
	}

	desired, err := b.buildPrebuiltWorkload(pcs, podGang)
	if err != nil {
		return err
	}
	existing := newKueueWorkload(podGang.Namespace, podGang.Name)
	if err = b.client.Get(ctx, client.ObjectKeyFromObject(existing), existing); err != nil {
		if apierrors.IsNotFound(err) {
			if err = b.client.Create(ctx, desired); err != nil {
				return fmt.Errorf("failed to create prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
			}
			log.FromContext(ctx).Info("Created prebuilt Kueue Workload", "workload", client.ObjectKeyFromObject(desired))
			return nil
		}
		return fmt.Errorf("failed to get prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	return b.updatePrebuiltWorkloadIfNeeded(ctx, podGang, existing, desired)
}

func newKueueWorkload(namespace, name string) *unstructured.Unstructured {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(workloadGVK)
	workload.SetNamespace(namespace)
	workload.SetName(name)
	return workload
}

// updatePrebuiltWorkloadIfNeeded reconciles the Grove-owned Workload fields while preserving metadata and
// status managed by Kueue. Kueue permits podSet updates before quota reservation, but makes them immutable
// afterwards, so reject admitted drift explicitly instead of silently leaving a stale Workload behind.
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
	if specChanged && workloadHasQuotaReservation(existing) {
		return fmt.Errorf("prebuilt kueue Workload %s/%s differs from PodGang %s after quota reservation and cannot be updated", existing.GetNamespace(), existing.GetName(), podGang.Name)
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

// buildPrebuiltWorkload constructs a prebuilt Kueue Workload from a PodGang, mapping each PodGroup to a Kueue
// podSet. The podSet name is the PodGroup FQN so scaling-group replicas of the same clique stay unique. count
// is the clique replicas; minCount is the PodGroup minReplicas for standalone cliques and is forced to count
// for scaling-group cliques (POC assumption PCSG.minAvailable == PCSG.replicas).
func (b *schedulerBackend) buildPrebuiltWorkload(pcs *grovecorev1alpha1.PodCliqueSet, podGang *groveschedulerv1alpha1.PodGang) (*unstructured.Unstructured, error) {
	queueName := pcs.Labels[queueNameLabel]
	if queueName == "" {
		return nil, fmt.Errorf("PodCliqueSet %s/%s must set label %q for Kueue queue selection", pcs.Namespace, pcs.Name, queueNameLabel)
	}

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
		return nil, fmt.Errorf("PodGang %s/%s has no PodGroups enabled for Kueue admission", podGang.Namespace, podGang.Name)
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
	// Own the Workload by the PodGang so Kubernetes garbage collection removes it once the PodGang and all
	// member Pods (which Kueue adds as owners on admission) are gone.
	if err := controllerutil.SetOwnerReference(podGang, workload, b.scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on prebuilt kueue Workload %s/%s: %w", podGang.Namespace, podGang.Name, err)
	}
	return workload, nil
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
	// Grove-managed pods are deliberately NOT marked as a Kueue "serving" pod group. A serving group is never
	// considered finished, so Kueue would never remove the kueue.x-k8s.io/managed finalizer on teardown (the
	// prebuilt Workload has no Kueue finalizer, so Workload deletion cannot trigger finalization either). As a
	// non-serving unretriable group, Kueue finalizes the Pods once they all terminate, letting them drain.
	if pod.Annotations[retriableInGroupAnnotation] == "" {
		pod.Annotations[retriableInGroupAnnotation] = "false"
	}
	if topologyKey := scheduler.RequiredTopologyKeyForPodGroup(preparation.PodGang, preparation.PodClique.Name, b.config.RequiredTopologyKey); topologyKey != "" && pod.Annotations[podSetRequiredTopologyAnnotation] == "" {
		pod.Annotations[podSetRequiredTopologyAnnotation] = topologyKey
	}
	return nil
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
	var (
		partialGangCliques     []string
		partialGangPCSGCliques []string
	)
	for _, cliqueTemplate := range pcs.Spec.Template.Cliques {
		if cliqueTemplate == nil {
			continue
		}
		if kueueAdmissionDisabled(cliqueTemplate.Annotations) {
			continue
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
