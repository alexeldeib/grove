// Copyright 2026 The Grove Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"time"

	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	"github.com/ai-dynamo/grove/operator/internal/constants"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StatusResyncInterval leaves recovery headroom without accelerating retries toward zero.
// The floor is one second, or half the policy for sub-two-second policies. Near an expired
// deadline the floor takes precedence: requeue timing cannot guarantee controller ordering.
func StatusResyncInterval(terminationDelay time.Duration, conditions []metav1.Condition, now time.Time) time.Duration {
	minimum := max(time.Nanosecond, min(time.Second, terminationDelay/2))
	remaining := terminationDelay
	condition := meta.FindStatusCondition(conditions, apiconstants.ConditionTypeMinAvailableBreached)
	if condition != nil && condition.Status == metav1.ConditionTrue {
		remaining = condition.LastTransitionTime.Add(terminationDelay).Sub(now)
	}
	return max(minimum, min(remaining/2, constants.PodCliqueStatusResyncInterval))
}
