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
	"context"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

type shardCandidate struct {
	shard   *valkey.ShardState
	primary *valkey.NodeState
	slots   int
}

func scaleDownActive(state *valkey.ClusterState, expectedShards int) bool {
	if state == nil || expectedShards <= 0 {
		return false
	}
	slotPrimaries := 0
	for _, shard := range state.Shards {
		primary := shard.GetPrimaryNode()
		if primary == nil {
			continue
		}
		if countSlots(shard.Slots) > 0 {
			slotPrimaries++
		}
	}
	return expectedShards < len(state.Shards) && expectedShards <= slotPrimaries
}

func (r *ValkeyClusterReconciler) scaleDownShards(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, pods *corev1.PodList) (bool, error) {
	if state == nil || pods == nil {
		return false, nil
	}
	expectedShards := int(cluster.Spec.Shards)
	if expectedShards <= 0 {
		return false, nil
	}

	slotPrimaries := make([]shardCandidate, 0, len(state.Shards))
	emptyShards := make([]shardCandidate, 0, len(state.Shards))
	for _, shard := range state.Shards {
		primary := shard.GetPrimaryNode()
		if primary == nil {
			continue
		}
		slots := countSlots(shard.Slots)
		candidate := shardCandidate{shard: shard, primary: primary, slots: slots}
		if slots == 0 {
			emptyShards = append(emptyShards, candidate)
		} else {
			slotPrimaries = append(slotPrimaries, candidate)
		}
	}

	sort.Slice(slotPrimaries, func(i, j int) bool {
		return slotPrimaries[i].primary.Address < slotPrimaries[j].primary.Address
	})
	sort.Slice(emptyShards, func(i, j int) bool {
		return emptyShards[i].primary.Address < emptyShards[j].primary.Address
	})

	keepers := map[string]struct{}{}
	for i, candidate := range slotPrimaries {
		if i >= expectedShards {
			break
		}
		keepers[candidate.primary.Id] = struct{}{}
	}

	for _, candidate := range emptyShards {
		if _, ok := keepers[candidate.primary.Id]; ok {
			continue
		}
		removed, err := r.deleteShardNode(ctx, cluster, candidate.shard, pods)
		if err != nil {
			return false, err
		}
		if removed {
			return true, nil
		}
	}

	if len(slotPrimaries) > expectedShards {
		moved, err := r.rebalanceSlots(ctx, cluster, state)
		if err != nil {
			return false, err
		}
		if moved {
			return true, nil
		}
	}

	return false, nil
}

func (r *ValkeyClusterReconciler) removeExtraReplicas(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, pods *corev1.PodList) (bool, error) {
	if state == nil || pods == nil {
		return false, nil
	}
	replicasRequired := int(cluster.Spec.Replicas)
	if replicasRequired < 0 {
		return false, nil
	}

	shards := append([]*valkey.ShardState(nil), state.Shards...)
	sort.Slice(shards, func(i, j int) bool {
		pi := shards[i].GetPrimaryNode()
		pj := shards[j].GetPrimaryNode()
		if pi == nil || pj == nil {
			return false
		}
		return pi.Address < pj.Address
	})

	log := logf.FromContext(ctx)
	for _, shard := range shards {
		primary := shard.GetPrimaryNode()
		if primary == nil {
			continue
		}
		replicas := make([]*valkey.NodeState, 0, len(shard.Nodes))
		for _, node := range shard.Nodes {
			if node.IsPrimary() {
				continue
			}
			replicas = append(replicas, node)
		}
		if len(replicas) <= replicasRequired {
			continue
		}
		sort.Slice(replicas, func(i, j int) bool {
			return replicas[i].Address < replicas[j].Address
		})
		for _, candidate := range replicas[replicasRequired:] {
			pod := findPodForNode(pods, candidate)
			if pod == nil {
				continue
			}
			deployment, err := r.deploymentForPod(ctx, pod)
			if err != nil {
				return false, err
			}
			if deployment == nil {
				continue
			}
			log.V(1).Info("deleting deployment for extra replica", "primary", primary.Address, "replica", candidate.Address, "deployment", deployment.Name)
			if err := r.Delete(ctx, deployment); err != nil {
				return false, err
			}
			r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "ReplicaRemoved", "Removed replica %v", candidate.Address)
			return true, nil
		}
	}
	return false, nil
}

func (r *ValkeyClusterReconciler) deleteShardNode(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shard *valkey.ShardState, pods *corev1.PodList) (bool, error) {
	if shard == nil || pods == nil {
		return false, nil
	}
	nodes := append([]*valkey.NodeState(nil), shard.Nodes...)
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].IsPrimary() != nodes[j].IsPrimary() {
			return !nodes[i].IsPrimary()
		}
		return nodes[i].Address < nodes[j].Address
	})

	log := logf.FromContext(ctx)
	for _, node := range nodes {
		pod := findPodForNode(pods, node)
		if pod == nil {
			continue
		}
		deployment, err := r.deploymentForPod(ctx, pod)
		if err != nil {
			return false, err
		}
		if deployment == nil {
			continue
		}
		log.V(1).Info("deleting deployment for removed shard node", "address", node.Address, "deployment", deployment.Name)
		if err := r.Delete(ctx, deployment); err != nil {
			return false, err
		}
		r.Recorder.Eventf(cluster, corev1.EventTypeNormal, "NodeRemoved", "Removed node %v from cluster", node.Address)
		return true, nil
	}
	return false, nil
}

func findPodForNode(pods *corev1.PodList, node *valkey.NodeState) *corev1.Pod {
	if pods == nil || node == nil {
		return nil
	}
	for i := range pods.Items {
		if pods.Items[i].Status.PodIP == node.Address {
			return &pods.Items[i]
		}
	}
	return nil
}

func (r *ValkeyClusterReconciler) deploymentForPod(ctx context.Context, pod *corev1.Pod) (*appsv1.Deployment, error) {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind != "ReplicaSet" || owner.Name == "" {
			continue
		}
		rs := &appsv1.ReplicaSet{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: owner.Name}, rs); err != nil {
			if client.IgnoreNotFound(err) == nil {
				return nil, nil
			}
			return nil, err
		}
		for _, rsOwner := range rs.OwnerReferences {
			if rsOwner.Kind != "Deployment" || rsOwner.Name == "" {
				continue
			}
			deployment := &appsv1.Deployment{}
			if err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: rsOwner.Name}, deployment); err != nil {
				if client.IgnoreNotFound(err) == nil {
					return nil, nil
				}
				return nil, err
			}
			return deployment, nil
		}
	}
	return nil, nil
}
