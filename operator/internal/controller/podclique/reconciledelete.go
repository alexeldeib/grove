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

package podclique

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	ctrlcommon "github.com/ai-dynamo/grove/operator/internal/controller/common"
	ctrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// triggerDeletionFlow handles the deletion of a PodClique. Owned Pods (and any
// PCLQ-scoped ResourceClaims) carry a controller owner reference back to this
// PodClique, so removing the finalizer hands the cascade off to the Kubernetes
// garbage collector. First release scheduler pod finalizers from an uncached
// inventory, then clear local creation/deletion expectations.
func (r *Reconciler) triggerDeletionFlow(ctx context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	dLog := logger.WithValues("operation", "delete")
	deleteStepFns := []ctrlcommon.ReconcileStepFn[grovecorev1alpha1.PodClique]{
		r.removeSchedulerPodFinalizers,
		r.clearPodCliqueExpectations,
		r.removeFinalizer,
	}
	for _, fn := range deleteStepFns {
		if stepResult := fn(ctx, dLog, pclq); ctrlcommon.ShortCircuitReconcileFlow(stepResult) {
			return stepResult
		}
	}
	dLog.Info("PodClique finalizer removed; Kubernetes garbage collector will cascade-delete owned Pods")
	return ctrlcommon.DoNotRequeue()
}

// clearPodCliqueExpectations drops the in-memory expectations entry for this
// PodClique so the store does not retain stale UIDs after the object is gone.
func (r *Reconciler) clearPodCliqueExpectations(_ context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	key, err := expect.ControlleeKeyFunc(pclq)
	if err != nil {
		return ctrlcommon.ReconcileWithErrors("error building expectations store key", fmt.Errorf("failed to build expectations store key for PodClique %v: %w", client.ObjectKeyFromObject(pclq), err))
	}
	if err := r.expectationsStore.DeleteExpectations(logger, key); err != nil {
		return ctrlcommon.ReconcileWithErrors("error clearing expectations", err)
	}
	return ctrlcommon.ContinueReconcile()
}

// removeSchedulerPodFinalizers drains scheduler finalizers before garbage collection
// removes the clique; otherwise no controller would remain to release serving pods.
func (r *Reconciler) removeSchedulerPodFinalizers(ctx context.Context, _ logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	if r.schedRegistry == nil {
		return ctrlcommon.ContinueReconcile()
	}
	backend, ok := r.schedRegistry.GetOrDefault(pclq.Spec.PodSpec.SchedulerName).(scheduler.Finalizer)
	if !ok {
		return ctrlcommon.ContinueReconcile()
	}
	if r.apiReader == nil {
		return ctrlcommon.ReconcileWithErrors("uncached Pod reader is required for scheduler finalizer cleanup", fmt.Errorf("API reader is not configured"))
	}
	key, err := expect.ControlleeKeyFunc(pclq)
	if err != nil {
		return ctrlcommon.ReconcileWithErrors("error building expectations store key", err)
	}
	// Reconciles for one clique are serialized, and create tasks finish before
	// their reconcile returns. A fresh API inventory therefore includes every
	// completed creation, even if neither its create nor delete reached the cache.
	// Keep expectations until inventory and finalizer cleanup have succeeded.
	pendingBeforeList := slices.Clone(r.expectationsStore.GetCreateExpectations(key))
	pods := &corev1.PodList{}
	if err := r.apiReader.List(ctx, pods, client.InNamespace(pclq.Namespace)); err != nil {
		return ctrlcommon.ReconcileWithErrors("error listing Pods for scheduler finalizer cleanup", err)
	}
	observed := sets.New[types.UID]()
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOfNoCopy(pod)
		if owner == nil || owner.UID != pclq.UID || owner.Kind != constants.KindPodClique || owner.Name != pclq.Name {
			continue
		}
		observed.Insert(pod.UID)
		if err := backend.RemovePodFinalizers(ctx, pod); err != nil {
			return ctrlcommon.ReconcileWithErrors("error removing scheduler pod finalizers", err)
		}
	}
	// Defensively retry if a creation was recorded during the inventory. An
	// earlier expectation absent from this authoritative list is already gone;
	// waiting for that UID to appear in the cache could block deletion forever.
	for _, uid := range r.expectationsStore.GetCreateExpectations(key) {
		if !slices.Contains(pendingBeforeList, uid) && !observed.Has(uid) {
			return ctrlcommon.ReconcileAfter(time.Second, "waiting to inventory a newly recorded Pod creation")
		}
	}
	return ctrlcommon.ContinueReconcile()
}

// removeFinalizer removes the PodClique finalizer to allow Kubernetes to complete the deletion
func (r *Reconciler) removeFinalizer(ctx context.Context, logger logr.Logger, pclq *grovecorev1alpha1.PodClique) ctrlcommon.ReconcileStepResult {
	if !controllerutil.ContainsFinalizer(pclq, constants.FinalizerPodClique) {
		logger.Info("Finalizer not found", "PodClique", pclq)
		return ctrlcommon.DoNotRequeue()
	}
	logger.Info("Removing finalizer", "PodClique", pclq, "finalizerName", constants.FinalizerPodClique)
	if err := ctrlutils.RemoveAndPatchFinalizer(ctx, r.client, pclq, constants.FinalizerPodClique); err != nil {
		return ctrlcommon.ReconcileWithErrors("error removing finalizer", fmt.Errorf("failed to remove finalizer: %s from PodClique: %v: %w", constants.FinalizerPodClique, client.ObjectKeyFromObject(pclq), err))
	}
	return ctrlcommon.ContinueReconcile()
}
