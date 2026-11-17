// Copyright 2026 The Grove Authors.
// SPDX-License-Identifier: Apache-2.0

package kueue

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// A JSON object mapping scaling-group names to protected replica counts.
	protectedReplicasAnnotation = "grove.io/kueue-protected-replicas"
	preemptibleLabel            = "grove.io/kueue-preemptible"
)

func protectedReplicaCounts(pcs *grovecorev1alpha1.PodCliqueSet) (map[string]*int32, error) {
	raw, configured := pcs.Annotations[protectedReplicasAnnotation]
	if !configured {
		return nil, nil
	}
	var counts map[string]*int32
	if err := json.Unmarshal([]byte(raw), &counts); err != nil || counts == nil {
		return nil, fmt.Errorf("invalid %s on PodCliqueSet %s/%s: expected an object of nonnegative replica counts", protectedReplicasAnnotation, pcs.Namespace, pcs.Name)
	}
	groups := make(map[string]grovecorev1alpha1.PodCliqueScalingGroupConfig)
	for _, group := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		groups[group.Name] = group
	}
	for name, count := range counts {
		if _, found := groups[name]; !found || count == nil || *count < 0 {
			return nil, fmt.Errorf("invalid protected replica count for scaling group %q on PodCliqueSet %s/%s", name, pcs.Namespace, pcs.Name)
		}
	}
	return counts, nil
}

// applyPreemptionProtection classifies the whole gang, never individual pods of
// a multi-node replica. Baseline protection does not change admission priority.
// Workloads without explicit configuration remain unlabeled, retaining legacy
// priority preemption until a replacement revision supplies its floor.
func applyPreemptionProtection(wl *unstructured.Unstructured, pcs *grovecorev1alpha1.PodCliqueSet, pg *groveschedulerv1alpha1.PodGang) error {
	counts, err := protectedReplicaCounts(pcs)
	if err != nil || counts == nil {
		return err
	}
	index, ok := pcsReplicaIndexOf(pg, pcs.Name)
	if !ok || pg.Labels[apicommon.LabelPartOfKey] != pcs.Name {
		return fmt.Errorf("cannot classify PodGang %s/%s for preemption protection", pg.Namespace, pg.Name)
	}
	base := pcs.Name + "-" + index
	value := "false"
	if pg.Name != base {
		matched := false
		for _, group := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
			name := group.Name
			suffix, found := strings.CutPrefix(pg.Name, base+"-"+name+"-")
			if !found {
				continue
			}
			gangIndex, err := strconv.ParseUint(suffix, 10, 31)
			if err != nil {
				continue
			}
			matched = true
			// Scaled gang indices start at zero AFTER the replicas carried by the
			// base gang. Using the suffix directly would protect one extra replica.
			baseCount := int32(1)
			if group.MinAvailable != nil {
				baseCount = *group.MinAvailable
			}
			if baseCount < 0 {
				return fmt.Errorf("negative minAvailable for scaling group %q", name)
			}
			floor, covered := counts[name]
			if covered && int64(gangIndex)+int64(baseCount) >= int64(*floor) {
				value = "true"
			}
			break
		}
		if !matched {
			return fmt.Errorf("cannot identify scaling group for protected PodGang %s/%s", pg.Namespace, pg.Name)
		}
	}
	labels := maps.Clone(wl.GetLabels())
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[preemptibleLabel] = value
	wl.SetLabels(labels)
	return nil
}

// syncPreemptibleLabel updates metadata without recreating an admitted lease or
// disturbing its quota, topology assignment, or runtime-only spec preservation.
func (b *schedulerBackend) syncPreemptibleLabel(ctx context.Context, existing, desired *unstructured.Unstructured) error {
	value, configured := desired.GetLabels()[preemptibleLabel]
	if !configured {
		if _, wasConfigured := existing.GetLabels()[preemptibleLabel]; !wasConfigured {
			return nil
		}
		value = "false"
	}
	if existing.GetLabels()[preemptibleLabel] == value {
		return nil
	}
	before := existing.DeepCopy()
	labels := maps.Clone(existing.GetLabels())
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[preemptibleLabel] = value
	existing.SetLabels(labels)
	if err := b.client.Patch(ctx, existing, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("update preemption protection for Workload %s/%s: %w", existing.GetNamespace(), existing.GetName(), err)
	}
	return nil
}
