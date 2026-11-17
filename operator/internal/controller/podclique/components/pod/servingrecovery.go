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

package pod

import (
	"fmt"
	"slices"

	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// prepareServingRecovery also finds pods owned by a prior incarnation of this
// clique. The ordinary owner-UID index deliberately excludes those pods.
func (r _resource) prepareServingRecovery(sc *syncContext) error {
	if r.schedRegistry == nil {
		return nil
	}
	sc.finalizer, _ = r.schedRegistry.GetOrDefault(sc.pclq.Spec.PodSpec.SchedulerName).(scheduler.Finalizer)
	if sc.finalizer == nil {
		return nil
	}
	pods := &corev1.PodList{}
	if err := r.client.List(sc.ctx, pods, client.InNamespace(sc.pclq.Namespace),
		client.MatchingFields{componentutils.PodCliqueNameIndexField: sc.pclq.Name}); err != nil {
		return fmt.Errorf("failed to list predecessor pods for PodClique %s/%s: %w", sc.pclq.Namespace, sc.pclq.Name, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOfNoCopy(pod)
		if owner != nil && owner.Kind == "PodClique" && owner.Name == sc.pclq.Name && owner.UID != sc.pclq.UID {
			sc.predecessorPods = append(sc.predecessorPods, pod)
		}
	}
	return nil
}

func (r _resource) removeSchedulerFinalizersFromDeletingPods(sc *syncContext) error {
	if sc.finalizer == nil {
		return nil
	}
	for _, pod := range slices.Concat(sc.existingPCLQPods, sc.predecessorPods) {
		if pod.DeletionTimestamp == nil {
			continue
		}
		if err := sc.finalizer.RemovePodFinalizers(sc.ctx, pod); err != nil {
			return err
		}
	}
	return nil
}

// withholdUnfinalizedReplacements blocks creation within this clique until old
// bound processes stop. Holding only a count of slots is insufficient: the
// hostname allocator excludes terminating pods and does not see predecessors,
// so even one replacement could reuse an old pod's index. A delete expectation
// alone is not proof the kubelet has stopped the old bound processes.
func (r _resource) withholdUnfinalizedReplacements(sc *syncContext, diff int) (int, bool) {
	if sc.finalizer == nil || diff >= 0 {
		return diff, false
	}
	deleting := r.expectationsStore.GetDeleteExpectations(sc.pclqExpectationsStoreKey)
	for _, pod := range sc.existingPCLQPods {
		if (pod.DeletionTimestamp != nil || slices.Contains(deleting, pod.UID)) && !sc.finalizer.IsPodFinalized(pod) {
			return 0, true
		}
	}
	for _, pod := range sc.predecessorPods {
		if !sc.finalizer.IsPodFinalized(pod) {
			return 0, true
		}
	}
	return diff, false
}
