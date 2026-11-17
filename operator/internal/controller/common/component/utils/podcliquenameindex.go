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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodCliqueNameIndexField finds a clique's pods across owner incarnations.
// The owner-UID index cannot find predecessors, and label selectors scan the
// namespace cache. This name index keeps the hot recovery lookup bounded to
// pods bearing this clique's label. Callers must still verify controller owners.
const PodCliqueNameIndexField = ".metadata.podClique.name"

// RegisterPodCliqueNameIndex installs the predecessor lookup before reconciliation.
func RegisterPodCliqueNameIndex(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Pod{}, PodCliqueNameIndexField, indexPodByCliqueName)
}

func indexPodByCliqueName(obj client.Object) []string {
	if name := obj.GetLabels()[apicommon.LabelPodClique]; name != "" {
		return []string{name}
	}
	return nil
}
