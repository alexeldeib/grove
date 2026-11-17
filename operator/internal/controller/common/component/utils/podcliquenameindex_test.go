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

package utils

import (
	"context"
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRegisterPodCliqueNameIndex(t *testing.T) {
	indexer := &podCliqueNameIndexer{}
	require.NoError(t, RegisterPodCliqueNameIndex(context.Background(), indexer))
	assert.IsType(t, &corev1.Pod{}, indexer.object)
	assert.Equal(t, PodCliqueNameIndexField, indexer.field)
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   []string
	}{
		{name: "unlabelled"},
		{name: "unrelated", labels: map[string]string{"other": "clique"}},
		{name: "empty", labels: map[string]string{apicommon.LabelPodClique: ""}},
		{name: "owned", labels: map[string]string{apicommon.LabelPodClique: "clique"}, want: []string{"clique"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, indexer.extract(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}}))
		})
	}
	indexer.err = fmt.Errorf("index registration unavailable")
	assert.ErrorIs(t, RegisterPodCliqueNameIndex(context.Background(), indexer), indexer.err)
}

type podCliqueNameIndexer struct {
	object  client.Object
	field   string
	extract client.IndexerFunc
	err     error
}

func (i *podCliqueNameIndexer) IndexField(_ context.Context, obj client.Object, field string, extract client.IndexerFunc) error {
	i.object, i.field, i.extract = obj, field, extract
	return i.err
}
