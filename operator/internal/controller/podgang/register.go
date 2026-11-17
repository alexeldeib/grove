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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	grovectrlutils "github.com/ai-dynamo/grove/operator/internal/controller/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&groveschedulerv1alpha1.PodGang{}, builder.WithPredicates(podGangSpecChangePredicate())).
		Watches(
			&grovecorev1alpha1.PodCliqueSet{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodCliqueSetToPodGangs),
			builder.WithPredicates(podCliqueSetChangePredicate()),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: *r.config.ConcurrentSyncs,
		}).
		Named("podgang").
		Complete(r)
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

// podCliqueSetChangePredicate enqueues PodGangs when their parent PodCliqueSet
// changes spec or labels. Scheduler backends may derive their external resource
// from both objects; Kueue, for example, takes queue selection from a PCS label.
func podCliqueSetChangePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool { return false },
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return grovectrlutils.IsManagedByGrove(e.ObjectOld.GetLabels()) &&
				grovectrlutils.IsManagedByGrove(e.ObjectNew.GetLabels()) &&
				(e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
					!maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()))
		},
		GenericFunc: func(_ event.GenericEvent) bool { return false },
	}
}

// mapPodCliqueSetToPodGangs maps a parent PCS change to all of its concrete
// PodGangs so each scheduler backend can reconcile its derived resource.
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
