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

package podgang

import (
	"context"
	"testing"
	"time"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler/registry"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Exercise controller registration and source startup, rather than testing a copy
// of the profile condition. Non-Kueue configurations must not require its CRD.
func TestRegisterKueueProfileWorkloadWatch(t *testing.T) {
	for _, tc := range []struct {
		name            string
		profiles        []configv1alpha1.SchedulerName
		workloadWatches int
	}{
		{name: "no Kueue"},
		{name: "legacy only", profiles: []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKueue}, workloadWatches: 1},
		{name: "alias only", profiles: []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKueueDefault}, workloadWatches: 1},
		{name: "both profiles", profiles: []configv1alpha1.SchedulerName{configv1alpha1.SchedulerNameKueue, configv1alpha1.SchedulerNameKueueDefault}, workloadWatches: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := testutils.NewTestClientBuilder().Build()
			cfg := configv1alpha1.SchedulerConfiguration{DefaultProfileName: string(configv1alpha1.SchedulerNameKube), Profiles: []configv1alpha1.SchedulerProfile{{Name: configv1alpha1.SchedulerNameKube}}}
			for _, name := range tc.profiles {
				cfg.Profiles = append(cfg.Profiles, configv1alpha1.SchedulerProfile{Name: name})
			}
			reg, err := registry.New(cl, cl, cl.Scheme(), record.NewFakeRecorder(10), cfg)
			require.NoError(t, err)
			observed := make(chan schema.GroupVersionKind, 4)
			c := &profileWatchCache{scheme: cl.Scheme(), observed: observed}
			mgr := &profileWatchManager{cl: cl, cache: c}
			concurrency := 1
			r := NewReconciler(mgr, configv1alpha1.PodGangControllerConfiguration{ConcurrentSyncs: &concurrency}, reg)
			require.NoError(t, r.RegisterWithManager(mgr))
			require.NotNil(t, mgr.controller)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Warmup starts and synchronizes every watch source without running workers.
			require.NoError(t, mgr.controller.(interface{ Warmup(context.Context) error }).Warmup(ctx))
			require.Len(t, observed, 3+tc.workloadWatches)
			watches := map[schema.GroupVersionKind]int{}
			for range len(observed) {
				watches[<-observed]++
			}
			workload := schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
			assert.Equal(t, tc.workloadWatches, watches[workload])
		})
	}
}

// Embed the interfaces to expose only the facilities used during registration.
// Unexpected manager/cache interactions fail instead of silently using a server.
type profileWatchManager struct {
	manager.Manager
	cl         client.Client
	cache      cache.Cache
	controller manager.Runnable
}

func (m *profileWatchManager) GetClient() client.Client       { return m.cl }
func (m *profileWatchManager) GetScheme() *runtime.Scheme     { return m.cl.Scheme() }
func (m *profileWatchManager) GetRESTMapper() meta.RESTMapper { return m.cl.RESTMapper() }
func (m *profileWatchManager) GetCache() cache.Cache          { return m.cache }
func (m *profileWatchManager) GetLogger() logr.Logger         { return logr.Discard() }
func (m *profileWatchManager) GetControllerOptions() config.Controller {
	skip := true
	return config.Controller{SkipNameValidation: &skip, EnableWarmup: &skip}
}
func (m *profileWatchManager) Add(r manager.Runnable) error { m.controller = r; return nil }

type profileWatchCache struct {
	cache.Cache
	scheme   *runtime.Scheme
	observed chan<- schema.GroupVersionKind
}

func (c *profileWatchCache) GetInformer(_ context.Context, obj client.Object, _ ...cache.InformerGetOption) (cache.Informer, error) {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return nil, err
	}
	return &profileWatchInformer{gvk: gvk, observed: c.observed}, nil
}
func (c *profileWatchCache) WaitForCacheSync(context.Context) bool { return true }

type profileWatchInformer struct {
	cache.Informer
	gvk      schema.GroupVersionKind
	observed chan<- schema.GroupVersionKind
}

func (i *profileWatchInformer) AddEventHandlerWithOptions(_ toolscache.ResourceEventHandler, _ toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	i.observed <- i.gvk
	return profileWatchRegistration{}, nil
}

type profileWatchRegistration struct{}

func (profileWatchRegistration) HasSynced() bool { return true }
