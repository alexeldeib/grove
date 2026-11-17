// Copyright 2026 The Grove Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"
	"time"

	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPersistentBreachResyncHasFloor(t *testing.T) {
	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	conditions := []metav1.Condition{{Type: apiconstants.ConditionTypeMinAvailableBreached,
		Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(start)}}
	for elapsed := time.Duration(0); elapsed <= 61*time.Second; elapsed += 100 * time.Millisecond {
		interval := StatusResyncInterval(time.Minute, conditions, start.Add(elapsed))
		if interval < time.Second || interval > 30*time.Second {
			t.Fatalf("at %s: interval %s outside [1s,30s]", elapsed, interval)
		}
	}
	if got := StatusResyncInterval(time.Minute, conditions, start.Add(59900*time.Millisecond)); got != time.Second {
		t.Fatalf("near-deadline retry = %s, want 1s", got)
	}
}
