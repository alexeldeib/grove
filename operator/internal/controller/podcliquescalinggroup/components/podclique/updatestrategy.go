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
	"math"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// resolveMaxUnavailable returns the maximum number of replicas that can be
// unavailable simultaneously during a rolling update. Defaults to 1.
func resolveMaxUnavailable(pcsg *grovecorev1alpha1.PodCliqueScalingGroup) int {
	if pcsg.Spec.UpdateStrategy == nil ||
		pcsg.Spec.UpdateStrategy.RollingUpdate == nil ||
		pcsg.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable == nil {
		return 1
	}
	return resolveIntOrPercent(*pcsg.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable, int(pcsg.Spec.Replicas))
}

// resolveMaxSurge returns the maximum number of extra replicas created above
// spec.replicas during a rolling update. Defaults to 0.
func resolveMaxSurge(pcsg *grovecorev1alpha1.PodCliqueScalingGroup) int {
	if pcsg.Spec.UpdateStrategy == nil ||
		pcsg.Spec.UpdateStrategy.RollingUpdate == nil ||
		pcsg.Spec.UpdateStrategy.RollingUpdate.MaxSurge == nil {
		return 0
	}
	return resolveIntOrPercent(*pcsg.Spec.UpdateStrategy.RollingUpdate.MaxSurge, int(pcsg.Spec.Replicas))
}

// resolveIntOrPercent resolves an IntOrString value to an integer.
// If the value is a percentage, it is resolved as a percentage of totalReplicas
// rounded up to the nearest integer. The result is clamped to at least 1 if
// the input percentage is non-zero and totalReplicas > 0.
func resolveIntOrPercent(val intstr.IntOrString, totalReplicas int) int {
	if val.Type == intstr.Int {
		return int(val.IntVal)
	}
	// percentage
	percentage, _ := intstr.GetScaledValueFromIntOrPercent(&val, totalReplicas, true)
	if percentage < 0 {
		return 0
	}
	return int(math.Max(1, float64(percentage)))
}
