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

package valkey

import (
	"fmt"
	"sort"
)

// SlotMove describes a small, incremental slot migration between two primaries.
type SlotMove struct {
	Src   *NodeState
	Dst   *NodeState
	Slots []int
}

type primarySlots struct {
	node   *NodeState
	ranges []SlotsRange
	count  int
	target int
}

// BuildRebalanceMove computes a single, deterministic slot move to improve balance.
// It returns nil when the cluster is already balanced or not ready for rebalancing.
func BuildRebalanceMove(shards []*ShardState, expectedShards int, maxSlots int) (*SlotMove, error) {
	if expectedShards <= 0 || maxSlots <= 0 {
		return nil, nil
	}
	if len(shards) != expectedShards {
		return nil, nil
	}

	all := make([]*primarySlots, 0, len(shards))
	for _, shard := range shards {
		primary := shard.GetPrimaryNode()
		if primary == nil {
			return nil, fmt.Errorf("primary missing for shard %s", shard.Id)
		}
		count := 0
		for _, slot := range shard.Slots {
			count += slot.End - slot.Start + 1
		}
		all = append(all, &primarySlots{
			node:   primary,
			ranges: append([]SlotsRange(nil), shard.Slots...),
			count:  count,
		})
	}
	// Sort by address for deterministic target assignment.
	sort.Slice(all, func(i, j int) bool { return all[i].node.Address < all[j].node.Address })

	// Assign per-shard slot targets and find the first imbalance.
	base, rem := TotalSlots/expectedShards, TotalSlots%expectedShards
	var src, dst *primarySlots
	for i, p := range all {
		p.target = base
		if i < rem {
			p.target++
		}
		if src == nil && p.count > p.target {
			src = p
		}
		if dst == nil && p.count < p.target {
			dst = p
		}
	}
	if src == nil || dst == nil {
		return nil, nil
	}

	// Tolerate ±1-slot rounding differences that arise because Phase 2's
	// slot assignment order may not match the address-sorted target order.
	if src.count-src.target <= 1 && dst.target-dst.count <= 1 {
		return nil, nil
	}

	moveCount := min(src.count-src.target, dst.target-dst.count, maxSlots)
	slots, err := takeSlotsFromRanges(src.ranges, moveCount)
	if err != nil {
		return nil, err
	}
	return &SlotMove{Src: src.node, Dst: dst.node, Slots: slots}, nil
}

func takeSlotsFromRanges(ranges []SlotsRange, count int) ([]int, error) {
	if count <= 0 {
		return nil, nil
	}
	out := make([]int, 0, count)
	for _, slotRange := range ranges {
		for slot := slotRange.Start; slot <= slotRange.End && len(out) < count; slot++ {
			out = append(out, slot)
		}
		if len(out) == count {
			break
		}
	}
	if len(out) != count {
		return nil, fmt.Errorf("insufficient slots available to move: requested %d, got %d", count, len(out))
	}
	return out, nil
}
