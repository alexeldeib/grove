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
	"strconv"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type replicaFixture struct {
	scheduled   int32
	ready       int32
	terminating bool
	missing     bool
}

func newSyncContextFixture(replicas int32, maxUnavailable *int32, fixtures ...replicaFixture) *syncContext {
	pclqs := make([]grovecorev1alpha1.PodClique, 0, len(fixtures))
	expectedFQNs := make(map[int][]string, len(fixtures))
	for i, f := range fixtures {
		name := "pcsg-" + strconv.Itoa(i) + "-w"
		expectedFQNs[i] = []string{name}
		if f.missing {
			continue
		}
		pclq := grovecorev1alpha1.PodClique{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{apicommon.LabelPodCliqueScalingGroupReplicaIndex: strconv.Itoa(i)},
			},
			Spec:   grovecorev1alpha1.PodCliqueSpec{MinAvailable: ptr.To(int32(1))},
			Status: grovecorev1alpha1.PodCliqueStatus{ScheduledReplicas: f.scheduled, ReadyReplicas: f.ready},
		}
		if f.terminating {
			pclq.DeletionTimestamp = ptr.To(metav1.Now())
			pclq.Finalizers = []string{"grove.io/test"}
		}
		pclqs = append(pclqs, pclq)
	}

	var cfg *grovecorev1alpha1.PodCliqueScalingGroupConfig
	if maxUnavailable != nil {
		cfg = &grovecorev1alpha1.PodCliqueScalingGroupConfig{
			RollingUpdate: &grovecorev1alpha1.RollingUpdateConfiguration{MaxUnavailable: maxUnavailable},
		}
	}

	return &syncContext{
		pcsg: &grovecorev1alpha1.PodCliqueScalingGroup{
			Spec: grovecorev1alpha1.PodCliqueScalingGroupSpec{Replicas: replicas},
		},
		pcsgConfig:                     cfg,
		existingPCLQs:                  pclqs,
		expectedPCLQFQNsPerPCSGReplica: expectedFQNs,
	}
}

var (
	readyReplica       = replicaFixture{scheduled: 1, ready: 1}
	pendingReplica     = replicaFixture{scheduled: 0, ready: 0}
	unavailableReplica = replicaFixture{scheduled: 1, ready: 0}
	terminatingReplica = replicaFixture{scheduled: 1, ready: 1, terminating: true}
	missingReplica     = replicaFixture{missing: true}
)

func TestCountUnavailableReplicas(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		fixtures []replicaFixture
		want     int
	}{
		"all ready":                      {[]replicaFixture{readyReplica, readyReplica, readyReplica}, 0},
		"pending counts as unavailable":  {[]replicaFixture{readyReplica, pendingReplica}, 1},
		"scheduled but not ready counts": {[]replicaFixture{readyReplica, unavailableReplica}, 1},
		"terminating counts":             {[]replicaFixture{readyReplica, terminatingReplica}, 1},
		"missing replica counts":         {[]replicaFixture{readyReplica, missingReplica}, 1},
		"all unavailable":                {[]replicaFixture{pendingReplica, pendingReplica}, 2},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sc := newSyncContextFixture(int32(len(tt.fixtures)), nil, tt.fixtures...)
			assert.Equal(t, tt.want, countUnavailableReplicas(sc))
		})
	}
}

func TestComputeUnavailableBudget(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		maxUnavailable *int32
		fixtures       []replicaFixture
		wantNil        bool
		wantRemaining  int
	}{
		"unset maxUnavailable means unbounded": {
			maxUnavailable: nil,
			fixtures:       []replicaFixture{readyReplica, pendingReplica},
			wantNil:        true,
		},
		"full budget when everything is ready": {
			maxUnavailable: ptr.To(int32(2)),
			fixtures:       []replicaFixture{readyReplica, readyReplica, readyReplica},
			wantRemaining:  2,
		},
		"one unavailable consumes one slot": {
			maxUnavailable: ptr.To(int32(2)),
			fixtures:       []replicaFixture{readyReplica, pendingReplica},
			wantRemaining:  1,
		},
		"exhausted budget clamps to zero": {
			maxUnavailable: ptr.To(int32(1)),
			fixtures:       []replicaFixture{pendingReplica, pendingReplica, pendingReplica},
			wantRemaining:  0,
		},
		"zero fails closed onto the strictest valid setting": {
			maxUnavailable: ptr.To(int32(0)),
			fixtures:       []replicaFixture{readyReplica, readyReplica},
			wantRemaining:  1,
		},
		"negative fails closed onto the strictest valid setting": {
			maxUnavailable: ptr.To(int32(-3)),
			fixtures:       []replicaFixture{readyReplica, readyReplica},
			wantRemaining:  1,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sc := newSyncContextFixture(int32(len(tt.fixtures)), tt.maxUnavailable, tt.fixtures...)
			got := computeUnavailableBudget(sc)
			if tt.wantNil {
				assert.Nil(t, got)

				return
			}
			assert.NotNil(t, got)
			assert.Equal(t, tt.wantRemaining, got.remaining)
		})
	}
}

func TestRecreatedButUnavailableReplicaStillConsumesBudget(t *testing.T) {
	t.Parallel()

	// A replica deleted and recreated at the new PodTemplateHash is no longer pending an
	// update, but its Pods are not available yet. It must keep consuming the budget, otherwise
	// each reconcile would take down another ready replica and drain the scaling group.
	sc := newSyncContextFixture(3, ptr.To(int32(1)), readyReplica, readyReplica, pendingReplica)

	budget := computeUnavailableBudget(sc)
	assert.NotNil(t, budget)
	assert.Equal(t, 0, budget.remaining)
}

func TestNonPositiveMaxUnavailableDoesNotFallBackToUnbounded(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]int32{"zero": 0, "negative": -1} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sc := newSyncContextFixture(3, ptr.To(value), readyReplica, readyReplica, readyReplica)

			budget := computeUnavailableBudget(sc)
			assert.NotNil(t, budget, "must not degrade to the unbounded nil budget")
			assert.Equal(t, 1, budget.maxConcurrentReclaims, "recovery must remain possible")
			assert.Equal(t, 1, budget.remaining)
		})
	}
}

func TestExhaustedBudgetStillAllowsRecovery(t *testing.T) {
	t.Parallel()

	// Three replicas, maxUnavailable 1, one already pending. Replacing the pending replica
	// keeps the unavailable count at one, so it must stay permitted even though there is no
	// headroom to take down a ready replica. Gating recovery on remaining would stall the
	// rollout permanently.
	sc := newSyncContextFixture(3, ptr.To(int32(1)), readyReplica, readyReplica, pendingReplica)

	budget := computeUnavailableBudget(sc)
	assert.NotNil(t, budget)
	assert.Equal(t, 0, budget.remaining, "no headroom for taking down a ready replica")
	assert.Equal(t, []int{2}, limitConcurrentReclaims(log.Log, budget, []int{2}),
		"already-unavailable replica must still be reclaimed")
}

func TestLimitConcurrentReclaims(t *testing.T) {
	t.Parallel()

	candidates := []int{0, 1, 2, 3, 4}

	tests := map[string]struct {
		budget *unavailableBudget
		input  []int
		want   []int
	}{
		"nil budget reclaims everything": {nil, candidates, candidates},
		"cap truncates the batch": {
			&unavailableBudget{maxConcurrentReclaims: 2}, candidates, []int{0, 1},
		},
		"cap above candidate count is noop": {
			&unavailableBudget{maxConcurrentReclaims: 10}, candidates, candidates,
		},
		"empty candidate list stays empty": {
			&unavailableBudget{maxConcurrentReclaims: 2}, []int{}, []int{},
		},
		"exhausted remaining does not block reclaims": {
			&unavailableBudget{maxConcurrentReclaims: 2, remaining: 0}, candidates, []int{0, 1},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, limitConcurrentReclaims(log.Log, tt.budget, tt.input))
		})
	}
}

func TestPodCliqueScalingGroupConfigDeepCopyDoesNotAliasRollingUpdate(t *testing.T) {
	t.Parallel()

	original := &grovecorev1alpha1.PodCliqueScalingGroupConfig{
		Name:          "worker",
		RollingUpdate: &grovecorev1alpha1.RollingUpdateConfiguration{MaxUnavailable: ptr.To(int32(2))},
	}

	copied := original.DeepCopy()
	*copied.RollingUpdate.MaxUnavailable = 7

	assert.Equal(t, int32(2), *original.RollingUpdate.MaxUnavailable)
	assert.Equal(t, int32(7), *copied.RollingUpdate.MaxUnavailable)
}
