/*
Copyright 2025 The Grove Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pod

import "maps"

// Kueue's plain-Pod integration uses these stable metadata keys. Grove does not otherwise
// depend on Kueue, so keep the wire contract local instead of importing controller packages.
const (
	kueueQueueNameLabel               = "kueue.x-k8s.io/queue-name"
	kueuePodGroupNameLabel            = "kueue.x-k8s.io/pod-group-name"
	kueuePodGroupTotalCountAnnotation = "kueue.x-k8s.io/pod-group-total-count"
	kueuePodSetGroupNameAnnotation    = "kueue.x-k8s.io/podset-group-name"
)

// withKueuePodGroupMetadata maps Grove's per-replica PodGang identity to Kueue's
// plain-Pod group identity before the Pod is created. The caller supplies the group size and
// queue on the PodClique template; Grove supplies the dynamic PodGang name. Requiring both
// opt-in fields preserves the existing behavior of workloads that happen to carry only a
// queue label or unrelated Kueue metadata.
func withKueuePodGroupMetadata(
	labels map[string]string,
	annotations map[string]string,
	podGangName string,
) (map[string]string, map[string]string) {
	outLabels := maps.Clone(labels)
	outAnnotations := maps.Clone(annotations)
	if outLabels[kueueQueueNameLabel] == "" || outAnnotations[kueuePodGroupTotalCountAnnotation] == "" {
		return outLabels, outAnnotations
	}
	if outLabels == nil {
		outLabels = make(map[string]string)
	}
	if outAnnotations == nil {
		outAnnotations = make(map[string]string)
	}

	outLabels[kueuePodGroupNameLabel] = podGangName
	outAnnotations[kueuePodSetGroupNameAnnotation] = podGangName

	return outLabels, outAnnotations
}
