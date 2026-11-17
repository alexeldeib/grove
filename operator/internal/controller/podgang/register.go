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

package podgang

import (
	"context"
	"maps"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	grovectrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// RegisterWithManager registers the backend controller with the manager
func (r *Reconciler) RegisterWithManager(mgr ctrl.Manager) error {
	ctrlBuilder := ctrl.NewControllerManagedBy(mgr).
		For(&groveschedulerv1alpha1.PodGang{}, builder.WithPredicates(podGangSpecChangePredicate())).
		Watches(
			&grovecorev1alpha1.PodCliqueSet{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodCliqueSetToPodGangs),
			builder.WithPredicates(podCliqueSetChangePredicate()),
		).
		// A base PodGang becoming scheduled is what releases its scaled siblings' admission gate, but
		// that transition is a PodClique status write that bumps no PodGang generation, so
		// podGangSpecChangePredicate never sees it.
		Watches(
			&grovecorev1alpha1.PodClique{},
			handler.EnqueueRequestsFromMapFunc(r.mapBasePodCliqueToPodGangs),
			builder.WithPredicates(podCliqueScheduledReplicasChangePredicate()),
		)

	// The Kueue backend recreates its prebuilt Workload out of band: Kueue finalizes it
	// (Finished/OutOfSync) when the owning PodGang changes shape and makes it immutable after quota
	// reservation, so SyncPodGang evicts and later recreates it. Neither the eviction nor the recreation
	// changes the PodGang generation, so watch the Workload and re-enqueue its owner to drive that cycle.
	// Gated on the Kueue backend being registered so deployments without Kueue do not require its CRD to
	// start the informer.
	if r.schedRegistry.Get(string(configv1alpha1.SchedulerNameKueue)) != nil ||
		r.schedRegistry.Get(string(configv1alpha1.SchedulerNameKueueDefault)) != nil {
		ctrlBuilder = ctrlBuilder.Watches(
			newKueueWorkloadType(),
			handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &groveschedulerv1alpha1.PodGang{}),
			builder.WithPredicates(kueueWorkloadEvictionPredicate()),
		)
	}

	return ctrlBuilder.
		WithOptions(controller.Options{
			MaxConcurrentReconciles: *r.config.ConcurrentSyncs,
		}).
		Named("podgang").
		Complete(r)
}

// newKueueWorkloadType returns an empty Kueue Workload typed for informer registration.
func newKueueWorkloadType() *unstructured.Unstructured {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kueue.x-k8s.io",
		Version: "v1beta2",
		Kind:    "Workload",
	})
	return workload
}

// kueueWorkloadEvictionPredicate enqueues the owning PodGang when its prebuilt Workload is deleted, when
// Kueue finalizes it, or when Kueue deactivates it and releases admission accounting. Grove must observe
// those transitions without reactivating or replacing the lease while old Pods may still exist.
func kueueWorkloadEvictionPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return kueueWorkloadFinished(e.ObjectNew) && !kueueWorkloadFinished(e.ObjectOld) ||
				kueueWorkloadActive(e.ObjectOld) && !kueueWorkloadActive(e.ObjectNew)
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

func kueueWorkloadActive(obj client.Object) bool {
	workload, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return true
	}
	active, found, err := unstructured.NestedBool(workload.Object, "spec", "active")
	return err == nil && (!found || active)
}

// kueueWorkloadFinished reports whether the Workload carries a Finished=True status condition.
func kueueWorkloadFinished(obj client.Object) bool {
	workload, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	conditions, _, _ := unstructured.NestedSlice(workload.Object, "status", "conditions")
	for _, rawCondition := range conditions {
		condition, ok := rawCondition.(map[string]any)
		if ok && condition["type"] == "Finished" && condition["status"] == "True" {
			return true
		}
	}
	return false
}

// podGangSpecChangePredicate filters PodGang events to only process spec changes
// Status-only updates (like Initialized condition) are ignored
func podGangSpecChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return grovectrlutils.IsManagedPodGang(e.Object)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return grovectrlutils.IsManagedPodGang(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return grovectrlutils.IsManagedPodGang(e.ObjectOld) &&
				grovectrlutils.IsManagedPodGang(e.ObjectNew) &&
				(e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration())
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// Parent PCSs are user/provider-owned; their managed PodGangs must see spec, label, and floor changes.
func podCliqueSetChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
				!maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) ||
				e.ObjectOld.GetAnnotations()["grove.io/kueue-protected-replicas"] !=
					e.ObjectNew.GetAnnotations()["grove.io/kueue-protected-replicas"]
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// mapPodCliqueSetToPodGangs maps a parent PCS change to all of its concrete
// PodGangs so each scheduler backend can reconcile its derived resource.
// podCliqueScheduledReplicasChangePredicate enqueues on a change to PodClique.status.scheduledReplicas,
// the only signal that a base PodGang became scheduled. A PodClique is created with none scheduled and
// a base losing scheduling releases nothing, so creates and deletes are ignored.
func podCliqueScheduledReplicasChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPclq, okOld := e.ObjectOld.(*grovecorev1alpha1.PodClique)
			newPclq, okNew := e.ObjectNew.(*grovecorev1alpha1.PodClique)
			if !okOld || !okNew {
				return false
			}
			return grovectrlutils.IsManagedByGrove(newPclq.GetLabels()) &&
				oldPclq.Status.ScheduledReplicas != newPclq.Status.ScheduledReplicas
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// mapBasePodCliqueToPodGangs enqueues the base PodGang and its own replica's scaled siblings when a
// base PodClique's scheduled count moves, so each gated sibling can also release itself. Siblings are
// named <base>-<scalingGroup>-<n>, so the prefix stays within one replica. Scaled PodCliques are
// skipped since their scheduling gates nobody. A base PodClique without its PodGang label maps to
// the whole set rather than dropping the event.
func (r *Reconciler) mapBasePodCliqueToPodGangs(ctx context.Context, obj client.Object) []ctrl.Request {
	pclq, ok := obj.(*grovecorev1alpha1.PodClique)
	if !ok {
		return nil
	}
	if _, isScaled := pclq.GetLabels()[apicommon.LabelBasePodGang]; isScaled {
		return nil
	}
	pcsName := pclq.GetLabels()[apicommon.LabelPartOfKey]
	if pcsName == "" {
		return nil
	}
	podGangs := &groveschedulerv1alpha1.PodGangList{}
	if err := r.Client.List(
		ctx,
		podGangs,
		client.InNamespace(pclq.Namespace),
		client.MatchingLabels{apicommon.LabelPartOfKey: pcsName},
	); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list PodGangs for PodClique watch mapping",
			"podClique", client.ObjectKeyFromObject(pclq))
		return nil
	}
	base := pclq.GetLabels()[apicommon.LabelPodGang]
	requests := make([]ctrl.Request, 0, len(podGangs.Items))
	for i := range podGangs.Items {
		name := podGangs.Items[i].Name
		if base != "" && name != base && !strings.HasPrefix(name, base+"-") {
			continue
		}
		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&podGangs.Items[i])})
	}
	return requests
}

func (r *Reconciler) mapPodCliqueSetToPodGangs(ctx context.Context, obj client.Object) []ctrl.Request {
	pcs, ok := obj.(*grovecorev1alpha1.PodCliqueSet)
	if !ok {
		return nil
	}
	podGangs, err := componentutils.GetExistingPodGangs(ctx, r.Client, pcs.ObjectMeta, pcs.Namespace)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to list PodGangs for PodCliqueSet watch mapping", "podCliqueSet", client.ObjectKeyFromObject(pcs))
		return nil
	}
	requests := make([]ctrl.Request, 0, len(podGangs))
	for i := range podGangs {
		if podGangs[i].Labels[apicommon.LabelComponentKey] != apicommon.LabelComponentNamePodGang {
			continue
		}
		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&podGangs[i])})
	}
	return requests
}
