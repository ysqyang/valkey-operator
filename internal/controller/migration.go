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
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

const (
	rebalanceSlotBatchSize    = 400
	rebalanceKeysBatchSize    = 100
	rebalanceMigrateTimeoutMs = 5000
)

func (r *ValkeyClusterReconciler) rebalanceSlots(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shards []*valkey.ShardState) (bool, error) {
	move, err := valkey.BuildRebalanceMove(shards, int(cluster.Spec.Shards), rebalanceSlotBatchSize)
	if err != nil {
		return false, err
	}
	if move == nil || len(move.Slots) == 0 {
		return false, nil
	}

	log := logf.FromContext(ctx)
	inProgress, err := slotMigrationInProgress(ctx, move.Src)
	if err != nil {
		return false, err
	}
	if inProgress {
		log.V(1).Info("slot migration already in progress", "src", move.Src.Address)
		return true, nil
	}

	known, err := nodeKnownToSource(ctx, move.Src, move.Dst)
	if err != nil {
		return false, err
	}
	if !known {
		log.V(1).Info("destination node not known to source yet; waiting", "src", move.Src.Address, "dst", move.Dst.Address, "dstId", move.Dst.Id)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "SlotsRebalancePending", "RebalanceSlots", "Waiting for %s to learn node %s", move.Src.Address, move.Dst.Address)
		return true, nil
	}

	log.V(1).Info("rebalancing slots", "src", move.Src.Address, "dst", move.Dst.Address, "slots", len(move.Slots))
	r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "SlotsRebalancing", "RebalanceSlots", "Moving %d slots from %s to %s", len(move.Slots), move.Src.Address, move.Dst.Address)

	ranges := slotsToRanges(move.Slots)
	if err := migrateSlotsAtomic(ctx, move.Src, move.Dst, ranges); err != nil {
		if isSlotsNotServedByNode(err) {
			log.V(1).Info("slots no longer served by source; will retry with fresh state", "src", move.Src.Address, "dst", move.Dst.Address)
			return true, nil
		}
		if !isAtomicMigrationUnsupported(err) {
			return false, err
		}
		log.V(1).Info("atomic slot migration unsupported; falling back to legacy migration", "src", move.Src.Address, "dst", move.Dst.Address)
		for _, slot := range move.Slots {
			if err := migrateSlotLegacy(ctx, move.Src, move.Dst, slot); err != nil {
				return false, err
			}
		}
	} else {
		log.V(1).Info("atomic slot migration started", "src", move.Src.Address, "dst", move.Dst.Address, "ranges", len(ranges))
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "SlotsRebalancingAtomic", "RebalanceSlots", "Atomic migration started from %s to %s for %d slot ranges", move.Src.Address, move.Dst.Address, len(ranges))
	}
	return true, nil
}

func slotMigrationInProgress(ctx context.Context, src *valkey.NodeState) (bool, error) {
	if src == nil || src.Client == nil {
		return false, fmt.Errorf("missing source node")
	}
	log := logf.FromContext(ctx)
	cmd := src.Client.B().Arbitrary("CLUSTER", "GETSLOTMIGRATIONS").Build()
	migrations, err := src.Client.Do(ctx, cmd).ToArray()
	if err != nil {
		if isAtomicMigrationUnsupported(err) {
			return false, nil
		}
		return false, fmt.Errorf("getslotmigrations failed on %s: %w", src.Address, err)
	}
	for _, migration := range migrations {
		values, parseErr := migration.AsStrMap()
		if parseErr != nil {
			log.V(1).Info("unable to parse slot migration entry; treating as in progress", "src", src.Address, "error", parseErr)
			return true, nil
		}
		state := strings.ToLower(values["state"])
		if state == "" {
			log.V(1).Info("slot migration entry missing state; treating as in progress", "src", src.Address)
			return true, nil
		}
		if !isSlotMigrationTerminal(state) {
			return true, nil
		}
	}
	return false, nil
}

func isSlotMigrationTerminal(state string) bool {
	switch strings.ToLower(state) {
	case "success", "failed", "canceled", "cancelled":
		return true
	default:
		return false
	}
}

func nodeKnownToSource(ctx context.Context, src *valkey.NodeState, dst *valkey.NodeState) (bool, error) {
	if src == nil || dst == nil || src.Client == nil {
		return false, fmt.Errorf("missing source or destination node")
	}
	if dst.Id == "" {
		return false, nil
	}
	cmd := src.Client.B().ClusterNodes().Build()
	nodes, err := src.Client.Do(ctx, cmd).ToString()
	if err != nil {
		return false, fmt.Errorf("cluster nodes failed on %s: %w", src.Address, err)
	}
	return strings.Contains(nodes, dst.Id), nil
}

func migrateSlotsAtomic(ctx context.Context, src *valkey.NodeState, dst *valkey.NodeState, ranges []valkey.SlotsRange) error {
	if src == nil || dst == nil {
		return fmt.Errorf("missing source or destination node")
	}
	if len(ranges) == 0 {
		return nil
	}
	cmd := src.Client.B().Arbitrary("CLUSTER", "MIGRATESLOTS")
	for _, slotRange := range ranges {
		cmd = cmd.Args(
			"SLOTSRANGE",
			strconv.Itoa(slotRange.Start),
			strconv.Itoa(slotRange.End),
			"NODE",
			dst.Id,
		)
	}
	if err := src.Client.Do(ctx, cmd.Build()).Error(); err != nil {
		return fmt.Errorf("migrateslots failed from %s to %s: %w", src.Address, dst.Address, err)
	}
	return nil
}

func migrateSlotLegacy(ctx context.Context, src *valkey.NodeState, dst *valkey.NodeState, slot int) error {
	if src == nil || dst == nil {
		return fmt.Errorf("missing source or destination node")
	}

	if err := src.Client.Do(ctx, src.Client.B().ClusterSetslot().Slot(int64(slot)).Migrating().NodeId(dst.Id).Build()).Error(); err != nil {
		return fmt.Errorf("setslot migrating failed for slot %d on %s: %w", slot, src.Address, err)
	}
	if err := dst.Client.Do(ctx, dst.Client.B().ClusterSetslot().Slot(int64(slot)).Importing().NodeId(src.Id).Build()).Error(); err != nil {
		return fmt.Errorf("setslot importing failed for slot %d on %s: %w", slot, dst.Address, err)
	}

	for {
		keys, err := src.Client.Do(ctx, src.Client.B().ClusterGetkeysinslot().Slot(int64(slot)).Count(int64(rebalanceKeysBatchSize)).Build()).AsStrSlice()
		if err != nil {
			return fmt.Errorf("getkeysinslot failed for slot %d on %s: %w", slot, src.Address, err)
		}
		if len(keys) == 0 {
			break
		}
		for _, key := range keys {
			if err := src.Client.Do(ctx, src.Client.B().Migrate().Host(dst.Address).Port(int64(dst.Port)).Key(key).DestinationDb(0).Timeout(rebalanceMigrateTimeoutMs).Replace().Build()).Error(); err != nil {
				return fmt.Errorf("migrate failed for slot %d key %s from %s to %s: %w", slot, key, src.Address, dst.Address, err)
			}
		}
	}

	if err := dst.Client.Do(ctx, dst.Client.B().ClusterSetslot().Slot(int64(slot)).Node().NodeId(dst.Id).Build()).Error(); err != nil {
		return fmt.Errorf("setslot node failed for slot %d on %s: %w", slot, dst.Address, err)
	}
	if err := src.Client.Do(ctx, src.Client.B().ClusterSetslot().Slot(int64(slot)).Node().NodeId(dst.Id).Build()).Error(); err != nil {
		return fmt.Errorf("setslot node failed for slot %d on %s: %w", slot, src.Address, err)
	}
	return nil
}

func slotsToRanges(slots []int) []valkey.SlotsRange {
	if len(slots) == 0 {
		return nil
	}
	ordered := append([]int(nil), slots...)
	sort.Ints(ordered)
	ranges := make([]valkey.SlotsRange, 0, len(ordered))
	start := ordered[0]
	prev := ordered[0]
	for _, slot := range ordered[1:] {
		if slot == prev+1 {
			prev = slot
			continue
		}
		ranges = append(ranges, valkey.SlotsRange{Start: start, End: prev})
		start = slot
		prev = slot
	}
	ranges = append(ranges, valkey.SlotsRange{Start: start, End: prev})
	return ranges
}

func isAtomicMigrationUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unknown subcommand") ||
		strings.Contains(msg, "syntax error") ||
		strings.Contains(msg, "wrong number of arguments") ||
		strings.Contains(msg, "not supported")
}

func isSlotsNotServedByNode(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "slots are not served by this node")
}

// drainExcessShards handles scale-down by migrating slots away from shards
// whose pod shard-index >= spec.Shards. Once a shard is fully drained (0
// slots), its deployments are deleted; forgetStaleNodes on the next reconcile
// cleans up the Valkey topology.
// Returns true if any work was done (caller should requeue).
func (r *ValkeyClusterReconciler) drainExcessShards(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, pods *corev1.PodList) (bool, error) {
	log := logf.FromContext(ctx)
	expectedShards := int(cluster.Spec.Shards)

	var remaining, draining []*valkey.ShardState
	for _, shard := range state.Shards {
		si := shardIndexFromState(shard, pods)
		if si >= 0 && si < expectedShards {
			remaining = append(remaining, shard)
		} else {
			draining = append(draining, shard)
		}
	}
	if len(draining) == 0 {
		return false, nil
	}

	for _, shard := range draining {
		if countSlots(shard.Slots) == 0 {
			continue
		}
		move, err := valkey.BuildDrainMove(shard, remaining, rebalanceSlotBatchSize)
		if err != nil {
			return false, err
		}
		if move == nil {
			continue
		}

		inProgress, err := slotMigrationInProgress(ctx, move.Src)
		if err != nil {
			return false, err
		}
		if inProgress {
			log.V(1).Info("drain migration in progress", "src", move.Src.Address)
			return true, nil
		}

		known, err := nodeKnownToSource(ctx, move.Src, move.Dst)
		if err != nil {
			return false, err
		}
		if !known {
			log.V(1).Info("drain destination not yet known to source", "src", move.Src.Address, "dst", move.Dst.Address)
			return true, nil
		}

		log.V(1).Info("draining slots", "src", move.Src.Address, "dst", move.Dst.Address, "slots", len(move.Slots))
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "SlotsDraining", "ScaleDown", "Moving %d slots from %s to %s", len(move.Slots), move.Src.Address, move.Dst.Address)

		ranges := slotsToRanges(move.Slots)
		if err := migrateSlotsAtomic(ctx, move.Src, move.Dst, ranges); err != nil {
			if isSlotsNotServedByNode(err) {
				return true, nil
			}
			if !isAtomicMigrationUnsupported(err) {
				return false, err
			}
			for _, slot := range move.Slots {
				if err := migrateSlotLegacy(ctx, move.Src, move.Dst, slot); err != nil {
					return false, err
				}
			}
		}
		return true, nil
	}

	// All draining shards have 0 slots — delete their deployments.
	for _, shard := range draining {
		si := shardIndexFromState(shard, pods)
		if si < 0 {
			continue
		}
		nodesPerShard := 1 + int(cluster.Spec.Replicas)
		for ni := range nodesPerShard {
			name := deploymentName(cluster.Name, si, ni)
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: cluster.Namespace,
				},
			}
			if err := r.Delete(ctx, dep); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("delete deployment %s: %w", name, err)
				}
			} else {
				log.V(1).Info("deleted deployment for drained shard", "deployment", name, "shard", si)
				r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "DeploymentDeleted", "ScaleDown", "Deleted deployment %s (shard %d)", name, si)
			}
		}
	}
	return true, nil
}

// deleteExcessDeployments removes deployments that are outside the desired
// spec: shard-index >= spec.Shards OR node-index >= 1 + spec.Replicas.
// This catches leftover deployments from shard scale-down (where drained
// primaries became replicas) and from replica scale-down.
func (r *ValkeyClusterReconciler) deleteExcessDeployments(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) (bool, error) {
	log := logf.FromContext(ctx)
	allDeps := &appsv1.DeploymentList{}
	if err := r.List(ctx, allDeps, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster))); err != nil {
		return false, err
	}
	nodesPerShard := 1 + int(cluster.Spec.Replicas)
	deleted := false
	for i := range allDeps.Items {
		dep := &allDeps.Items[i]
		si, err := strconv.Atoi(dep.Labels[LabelShardIndex])
		if err != nil {
			continue
		}
		ni, err := strconv.Atoi(dep.Labels[LabelNodeIndex])
		if err != nil {
			continue
		}
		if si >= int(cluster.Spec.Shards) || ni >= nodesPerShard {
			if err := r.Delete(ctx, dep); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, fmt.Errorf("delete excess deployment %s: %w", dep.Name, err)
				}
			} else {
				log.V(1).Info("deleted excess deployment", "name", dep.Name, "shard", si, "node", ni)
				r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "DeploymentDeleted", "ScaleDown", "Deleted excess deployment %s (shard %d, node %d)", dep.Name, si, ni)
				deleted = true
			}
		}
	}
	return deleted, nil
}

// shardIndexFromState determines the pod shard-index for a given Valkey shard
// by matching any of its nodes' addresses to pod labels.
func shardIndexFromState(shard *valkey.ShardState, pods *corev1.PodList) int {
	for _, node := range shard.Nodes {
		_, si := podRoleAndShard(node.Address, pods)
		if si >= 0 {
			return si
		}
	}
	return -1
}
