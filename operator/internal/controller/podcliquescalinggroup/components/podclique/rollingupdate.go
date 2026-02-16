// /*
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
// */

package podclique

import (
	"context"
	"fmt"
	"strconv"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// updateWork encapsulates the information needed to perform a rolling update of a PodCliqueScalingGroup.
type updateWork struct {
	oldPendingReplicaIndices     []int
	oldUnavailableReplicaIndices []int
	oldReadyReplicaIndices       []int
	// updatedReplicaCount tracks how many replicas already match the expected generation.
	// Used together with oldReady/oldPending/oldUnavailable counts to compute how many
	// replicas are currently "in-flight" (deleted/recreating but not yet updated).
	updatedReplicaCount int
}

type replicaState int

const (
	replicaStatePending replicaState = iota
	replicaStateUnAvailable
	replicaStateReady
)

// processPendingUpdates processes pending updates for the PodCliqueScalingGroup.
// This is the main entry point for handling rolling updates of PodCliques in the PodCliqueScalingGroup.
func (r _resource) processPendingUpdates(logger logr.Logger, sc *syncContext) error {
	work, err := computePendingUpdateWork(sc)
	if err != nil {
		return groveerr.WrapError(err,
			errCodeComputePendingPodCliqueScalingGroupUpdateWork,
			component.OperationSync,
			fmt.Sprintf("failed to compute pending update work for PodCliqueScalingGroup %v", client.ObjectKeyFromObject(sc.pcsg)))
	}
	// always delete PCSG replicas that are either pending or unavailable
	if err = r.deleteOldPendingAndUnavailableReplicas(logger, sc, work); err != nil {
		return err
	}

	maxUnavail := resolveMaxUnavailable(sc.pcsg)

	// Compute how many replicas are currently "in-flight" (deleted/recreating but not yet updated).
	// Total effective replicas (including surge) minus old (pending+unavailable+ready) minus already updated = in-flight.
	oldCount := len(work.oldPendingReplicaIndices) + len(work.oldUnavailableReplicaIndices) + len(work.oldReadyReplicaIndices)
	effectiveReplicas := int(sc.pcsg.Spec.Replicas) + resolveMaxSurge(sc.pcsg)
	inFlight := effectiveReplicas - oldCount - work.updatedReplicaCount

	// If maxUnavailable replicas are already in-flight, wait for them to complete.
	if inFlight > 0 && inFlight >= maxUnavail {
		return groveerr.New(
			groveerr.ErrCodeContinueReconcileAndRequeue,
			component.OperationSync,
			fmt.Sprintf("maxUnavailable reached: %d replicas in-flight (max %d), requeuing", inFlight, maxUnavail),
		)
	}

	// Select up to (maxUnavailable - inFlight) more old ready replicas to update.
	canUpdate := maxUnavail - inFlight
	if canUpdate > len(work.oldReadyReplicaIndices) {
		canUpdate = len(work.oldReadyReplicaIndices)
	}

	if canUpdate > 0 {
		if sc.pcsg.Status.AvailableReplicas < *sc.pcsg.Spec.MinAvailable {
			return groveerr.New(
				groveerr.ErrCodeContinueReconcileAndRequeue,
				component.OperationSync,
				fmt.Sprintf("available replicas %d lesser than minAvailable %d, requeuing", sc.pcsg.Status.AvailableReplicas, *sc.pcsg.Spec.MinAvailable),
			)
		}

		replicaIndicesToUpdate := work.oldReadyReplicaIndices[:canUpdate]
		logger.Info("Selected replicas to update", "replicaIndices", replicaIndicesToUpdate, "maxUnavailable", maxUnavail)

		for _, idx := range replicaIndicesToUpdate {
			if err := r.updatePCSGStatusWithNextReplicaToUpdate(sc.ctx, logger, sc.pcsg, idx); err != nil {
				return err
			}
		}

		// Trigger deletion of all selected replica indices at once.
		replicaIndexStrs := lo.Map(replicaIndicesToUpdate, func(idx int, _ int) string {
			return strconv.Itoa(idx)
		})
		deleteTasks := r.createDeleteTasks(logger, sc.pcs, sc.pcsg.Name, replicaIndexStrs, "deleting replicas for rolling update")
		if err := r.triggerDeletionOfPodCliques(sc.ctx, logger, client.ObjectKeyFromObject(sc.pcsg), deleteTasks); err != nil {
			return err
		}

		return groveerr.New(
			groveerr.ErrCodeContinueReconcileAndRequeue,
			component.OperationSync,
			fmt.Sprintf("started rolling update of %d PCSG replicas, requeuing", len(replicaIndicesToUpdate)),
		)
	}

	// If old replicas still exist but can't be updated (e.g., maxUnavailable=0 with no surge capacity),
	// requeue and wait for conditions to change.
	if oldCount > 0 {
		return groveerr.New(
			groveerr.ErrCodeContinueReconcileAndRequeue,
			component.OperationSync,
			fmt.Sprintf("waiting for %d old replicas to be updated (%d ready, %d pending, %d unavailable), requeuing",
				oldCount, len(work.oldReadyReplicaIndices), len(work.oldPendingReplicaIndices), len(work.oldUnavailableReplicaIndices)),
		)
	}

	// No old replicas remaining. If still waiting for in-flight replicas, requeue.
	if inFlight > 0 {
		return groveerr.New(
			groveerr.ErrCodeContinueReconcileAndRequeue,
			component.OperationSync,
			fmt.Sprintf("waiting for %d in-flight replicas to complete update, requeuing", inFlight),
		)
	}

	if err := r.markRollingUpdateEnd(sc.ctx, logger, sc.pcsg); err != nil {
		return err
	}
	// Requeue so the next reconcile cycle cleans up surge replicas
	// (triggerDeletionOfExcessPCSGReplicas runs before processPendingUpdates,
	// so it couldn't see the update had ended during this cycle).
	return groveerr.New(
		groveerr.ErrCodeContinueReconcileAndRequeue,
		component.OperationSync,
		"rolling update ended, requeuing to clean up surge replicas",
	)
}

// updatePCSGStatusWithNextReplicaToUpdate marks the next replica index as selected for rolling update in the PCSG status
func (r _resource) updatePCSGStatusWithNextReplicaToUpdate(ctx context.Context, logger logr.Logger, pcsg *grovecorev1alpha1.PodCliqueScalingGroup, nextReplicaIndexToUpdate int) error {
	patch := client.MergeFrom(pcsg.DeepCopy())

	if pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate == nil {
		pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate = &grovecorev1alpha1.PodCliqueScalingGroupReplicaRollingUpdateProgress{}
	} else {
		pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Completed = append(pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Completed, pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Current)
	}
	pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Current = int32(nextReplicaIndexToUpdate)

	if err := r.client.Status().Patch(ctx, pcsg, patch); err != nil {
		return groveerr.WrapError(
			err,
			errCodeUpdateStatus,
			component.OperationSync,
			fmt.Sprintf("failed to update ready replica selected to update in status of PodCliqueScalingGroup: %v", client.ObjectKeyFromObject(pcsg)),
		)
	}
	logger.Info("Updated PodCliqueScalingGroup status with new ready replica index selected to update", "nextReplicaIndexToUpdate", nextReplicaIndexToUpdate)
	return nil
}

// markRollingUpdateEnd finalizes the rolling update by setting the end timestamp and clearing update progress
func (r _resource) markRollingUpdateEnd(ctx context.Context, logger logr.Logger, pcsg *grovecorev1alpha1.PodCliqueScalingGroup) error {
	patch := client.MergeFrom(pcsg.DeepCopy())

	pcsg.Status.RollingUpdateProgress.UpdateEndedAt = ptr.To(metav1.Now())
	pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate = nil

	if err := r.client.Status().Patch(ctx, pcsg, patch); err != nil {
		return groveerr.WrapError(
			err,
			errCodeUpdateStatus,
			component.OperationSync,
			fmt.Sprintf("failed to mark end of rolling update in status of PodCliqueScalingGroup: %v", client.ObjectKeyFromObject(pcsg)),
		)
	}
	logger.Info("Marked the end of rolling update for PodCliqueScalingGroup")
	return nil
}

// computePendingUpdateWork analyzes existing replicas and categorizes them by update status and availability state.
// During rolling updates with maxSurge > 0, also iterates over surge replica indices.
func computePendingUpdateWork(sc *syncContext) (*updateWork, error) {
	work := &updateWork{}
	existingPCLQsByReplicaIndex := componentutils.GroupPCLQsByPCSGReplicaIndex(sc.existingPCLQs)
	effectiveReplicas := int(sc.pcsg.Spec.Replicas) + resolveMaxSurge(sc.pcsg)
	for pcsgReplicaIndex := range effectiveReplicas {
		pcsgReplicaIndexStr := strconv.Itoa(pcsgReplicaIndex)
		existingPCSGReplicaPCLQs := existingPCLQsByReplicaIndex[pcsgReplicaIndexStr]
		if isReplicaDeletedOrMarkedForDeletion(sc.pcsg, existingPCSGReplicaPCLQs, pcsgReplicaIndex) {
			continue
		}
		// If this replica was selected for update, check whether the update has completed.
		// If updated and ready, count it; otherwise leave it as in-flight (not counted anywhere).
		if sc.pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate != nil &&
			sc.pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Current == int32(pcsgReplicaIndex) {
			isUpdated, err := isReplicaUpdated(sc.expectedPCLQPodTemplateHashMap, existingPCSGReplicaPCLQs)
			if err == nil && isUpdated && getReplicaState(existingPCSGReplicaPCLQs) == replicaStateReady {
				work.updatedReplicaCount++
			}
			continue
		}
		isUpdated, err := isReplicaUpdated(sc.expectedPCLQPodTemplateHashMap, existingPCSGReplicaPCLQs)
		if err != nil {
			return nil, err
		}
		if isUpdated {
			work.updatedReplicaCount++
			continue
		}
		state := getReplicaState(existingPCSGReplicaPCLQs)
		switch state {
		case replicaStatePending:
			work.oldPendingReplicaIndices = append(work.oldPendingReplicaIndices, pcsgReplicaIndex)
		case replicaStateUnAvailable:
			work.oldUnavailableReplicaIndices = append(work.oldUnavailableReplicaIndices, pcsgReplicaIndex)
		case replicaStateReady:
			work.oldReadyReplicaIndices = append(work.oldReadyReplicaIndices, pcsgReplicaIndex)
		}
	}
	return work, nil
}

// deleteOldPendingAndUnavailableReplicas removes PCSG replicas that are pending or unavailable with old configurations
func (r _resource) deleteOldPendingAndUnavailableReplicas(logger logr.Logger, sc *syncContext, work *updateWork) error {
	replicaIndicesToDelete := lo.Map(append(work.oldPendingReplicaIndices, work.oldUnavailableReplicaIndices...), func(index int, _ int) string {
		return strconv.Itoa(index)
	})
	deleteTasks := r.createDeleteTasks(logger, sc.pcs, sc.pcsg.Name, replicaIndicesToDelete,
		"delete pending and unavailable PodCliqueScalingGroup replicas for rolling update")
	return r.triggerDeletionOfPodCliques(sc.ctx, logger, client.ObjectKeyFromObject(sc.pcsg), deleteTasks)
}

// isAnyReadyReplicaSelectedForUpdate checks if there is currently a ready replica selected for rolling update
func isAnyReadyReplicaSelectedForUpdate(pcsg *grovecorev1alpha1.PodCliqueScalingGroup) bool {
	return pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate != nil
}

// isCurrentReplicaUpdateComplete verifies if the currently updating replica has completed its rolling update
func isCurrentReplicaUpdateComplete(sc *syncContext) bool {
	currentlyUpdatingReplicaIndex := int(sc.pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate.Current)
	existingPCLQsByReplicaIndex := componentutils.GroupPCLQsByPCSGReplicaIndex(sc.existingPCLQs)
	// Get the expected PCLQ PodTemplateHash and compare it against all existing PCLQs for the currently updating replica index.
	expectedPCLQFQNs := sc.expectedPCLQFQNsPerPCSGReplica[currentlyUpdatingReplicaIndex]
	existingPCSGReplicaPCLQs := existingPCLQsByReplicaIndex[strconv.Itoa(currentlyUpdatingReplicaIndex)]
	if len(expectedPCLQFQNs) != len(existingPCSGReplicaPCLQs) {
		return false
	}
	return lo.EveryBy(existingPCSGReplicaPCLQs, func(pclq grovecorev1alpha1.PodClique) bool {
		return pclq.Status.CurrentPodTemplateHash != nil && *pclq.Status.CurrentPodTemplateHash == sc.expectedPCLQPodTemplateHashMap[pclq.Name] &&
			pclq.Status.CurrentPodCliqueSetGenerationHash != nil && *pclq.Status.CurrentPodCliqueSetGenerationHash == *sc.pcs.Status.CurrentGenerationHash &&
			pclq.Status.ReadyReplicas >= *pclq.Spec.MinAvailable
	})
}

// isReplicaUpdated checks if all PodCliques in a PCSG replica have the expected pod template hash
func isReplicaUpdated(expectedPCLQPodTemplateHashes map[string]string, pcsgReplicaPCLQs []grovecorev1alpha1.PodClique) (bool, error) {
	for _, pclq := range pcsgReplicaPCLQs {
		podTemplateHash, ok := pclq.Labels[apicommon.LabelPodTemplateHash]
		if !ok {
			return false, groveerr.ErrMissingPodTemplateHashLabel
		}
		if podTemplateHash != expectedPCLQPodTemplateHashes[pclq.Name] {
			return false, nil
		}
	}
	return true, nil
}

// isReplicaDeletedOrMarkedForDeletion determines if a PCSG replica is deleted or all its PodCliques are terminating
func isReplicaDeletedOrMarkedForDeletion(pcsg *grovecorev1alpha1.PodCliqueScalingGroup, pcsgReplicaPCLQs []grovecorev1alpha1.PodClique, _ int) bool {
	if pcsg.Status.RollingUpdateProgress.ReadyReplicaIndicesSelectedToUpdate == nil {
		return false
	}
	if len(pcsgReplicaPCLQs) == 0 {
		return true
	}
	return lo.EveryBy(pcsgReplicaPCLQs, func(pclq grovecorev1alpha1.PodClique) bool {
		return k8sutils.IsResourceTerminating(pclq.ObjectMeta)
	})
}

// getReplicaState determines the overall state of a PCSG replica based on its constituent PodCliques
func getReplicaState(pcsgReplicaPCLQs []grovecorev1alpha1.PodClique) replicaState {
	for _, pclq := range pcsgReplicaPCLQs {
		if pclq.Status.ScheduledReplicas < *pclq.Spec.MinAvailable {
			return replicaStatePending
		}
		if pclq.Status.ReadyReplicas < *pclq.Spec.MinAvailable {
			return replicaStateUnAvailable
		}
	}
	return replicaStateReady
}
