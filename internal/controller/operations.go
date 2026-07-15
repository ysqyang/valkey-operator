/*
Copyright 2025 Valkey Contributors.

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

package controller

import (
	"reflect"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

func desiredClusterOperation(cluster *valkeyiov1alpha1.ValkeyCluster, nodes *valkeyiov1alpha1.ValkeyNodeList, configHash string) *valkeyiov1alpha1.ValkeyClusterOperationStatus {
	switch {
	case hasExcessValkeyNodes(cluster, nodes):
		return newClusterOperation(cluster, valkeyiov1alpha1.OperationScaleIn, valkeyiov1alpha1.OperationPhaseDrainingSlots, configHash)
	case hasMissingValkeyNodes(cluster, nodes):
		return newClusterOperation(cluster, valkeyiov1alpha1.OperationScaleOut, valkeyiov1alpha1.OperationPhaseCreatingNodes, configHash)
	case anyNodeRequiresRoll(cluster, nodes, configHash):
		return newClusterOperation(cluster, valkeyiov1alpha1.OperationRollingUpdate, valkeyiov1alpha1.OperationPhaseRollingNodes, configHash)
	default:
		return nil
	}
}

func newClusterOperation(cluster *valkeyiov1alpha1.ValkeyCluster, operationType valkeyiov1alpha1.OperationType, phase valkeyiov1alpha1.OperationPhase, configHash string) *valkeyiov1alpha1.ValkeyClusterOperationStatus {
	return &valkeyiov1alpha1.ValkeyClusterOperationStatus{
		Type:       operationType,
		Phase:      phase,
		Generation: cluster.Generation,
		Surfaces:   operationSurfaces(operationType),
		Target: valkeyiov1alpha1.ValkeyClusterOperationTarget{
			Shards:     cluster.Spec.Shards,
			Replicas:   cluster.Spec.Replicas,
			ConfigHash: configHash,
		},
	}
}

func operationSurfaces(operationType valkeyiov1alpha1.OperationType) []valkeyiov1alpha1.OperationSurface {
	switch operationType {
	case valkeyiov1alpha1.OperationScaleOut, valkeyiov1alpha1.OperationScaleIn:
		return []valkeyiov1alpha1.OperationSurface{
			valkeyiov1alpha1.OperationSurfaceTopology,
			valkeyiov1alpha1.OperationSurfaceSlots,
		}
	case valkeyiov1alpha1.OperationRollingUpdate:
		return []valkeyiov1alpha1.OperationSurface{
			valkeyiov1alpha1.OperationSurfacePodRoll,
			valkeyiov1alpha1.OperationSurfacePrimaryRole,
		}
	case valkeyiov1alpha1.OperationRebalance:
		return []valkeyiov1alpha1.OperationSurface{valkeyiov1alpha1.OperationSurfaceSlots}
	default:
		return nil
	}
}

func syncActiveOperation(cluster *valkeyiov1alpha1.ValkeyCluster, desired *valkeyiov1alpha1.ValkeyClusterOperationStatus) bool {
	if desired == nil {
		return false
	}
	active := cluster.Status.ActiveOperation
	if active != nil && active.Type == desired.Type && reflect.DeepEqual(active.Target, desired.Target) {
		return false
	}
	if active != nil && !operationCanReplace(active.Type, desired.Type) {
		return false
	}

	now := metav1.Now()
	if active != nil && active.StartedAt.IsZero() {
		active.StartedAt = now
	}
	if active != nil {
		desired.StartedAt = active.StartedAt
	} else {
		desired.StartedAt = now
	}
	desired.UpdatedAt = now
	cluster.Status.ActiveOperation = desired
	return true
}

func operationCanReplace(activeType, desiredType valkeyiov1alpha1.OperationType) bool {
	if isTopologyOperation(activeType) && desiredType == valkeyiov1alpha1.OperationRollingUpdate {
		return false
	}
	return true
}

func isTopologyOperation(operationType valkeyiov1alpha1.OperationType) bool {
	return operationType == valkeyiov1alpha1.OperationScaleOut || operationType == valkeyiov1alpha1.OperationScaleIn
}

func setActiveOperationPhase(cluster *valkeyiov1alpha1.ValkeyCluster, phase valkeyiov1alpha1.OperationPhase, message string) {
	active := cluster.Status.ActiveOperation
	if active == nil {
		return
	}
	if active.Phase == phase && active.Message == message {
		return
	}
	active.Phase = phase
	active.Message = message
	active.UpdatedAt = metav1.Now()
}

func clearActiveOperation(cluster *valkeyiov1alpha1.ValkeyCluster) {
	cluster.Status.ActiveOperation = nil
}

func activeOperationAllowsSpecUpdates(cluster *valkeyiov1alpha1.ValkeyCluster) bool {
	active := cluster.Status.ActiveOperation
	return active == nil || active.Type == valkeyiov1alpha1.OperationRollingUpdate
}

func hasMissingValkeyNodes(cluster *valkeyiov1alpha1.ValkeyCluster, nodes *valkeyiov1alpha1.ValkeyNodeList) bool {
	seen := make(map[string]struct{}, len(nodes.Items))
	for i := range nodes.Items {
		seen[nodes.Items[i].Name] = struct{}{}
	}

	nodesPerShard := 1 + int(cluster.Spec.Replicas)
	for shardIndex := range int(cluster.Spec.Shards) {
		for nodeIndex := range nodesPerShard {
			desired := buildClusterValkeyNode(cluster, shardIndex, nodeIndex)
			if _, ok := seen[desired.Name]; !ok {
				return true
			}
		}
	}
	return false
}

func labelInt(labels map[string]string, key string) (int, error) {
	return strconv.Atoi(labels[key])
}

func hasExcessValkeyNodes(cluster *valkeyiov1alpha1.ValkeyCluster, nodes *valkeyiov1alpha1.ValkeyNodeList) bool {
	desiredShards := int(cluster.Spec.Shards)
	desiredNodesPerShard := 1 + int(cluster.Spec.Replicas)

	for i := range nodes.Items {
		shardIndex, shardErr := labelInt(nodes.Items[i].Labels, LabelShardIndex)
		nodeIndex, nodeErr := labelInt(nodes.Items[i].Labels, LabelNodeIndex)
		if shardErr != nil || nodeErr != nil {
			continue
		}
		if shardIndex >= desiredShards || nodeIndex >= desiredNodesPerShard {
			return true
		}
	}
	return false
}
