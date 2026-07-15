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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var _ = Describe("active cluster operations", func() {
	const configHash = "roll-hash"

	newCluster := func() *valkeyiov1alpha1.ValkeyCluster {
		return &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "operation-test",
				Namespace:  "default",
				Generation: 7,
			},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards:       2,
				Replicas:     0,
				Image:        "valkey/valkey:9.1.0",
				WorkloadType: valkeyiov1alpha1.WorkloadTypeStatefulSet,
			},
		}
	}

	nodeList := func(cluster *valkeyiov1alpha1.ValkeyCluster, count int) *valkeyiov1alpha1.ValkeyNodeList {
		nodes := &valkeyiov1alpha1.ValkeyNodeList{}
		for shardIndex := range count {
			node := buildClusterValkeyNode(cluster, shardIndex, 0)
			node.Spec.ServerConfigHash = configHash
			node.Status.PodIP = fmt.Sprintf("10.0.0.%d", shardIndex+1)
			nodes.Items = append(nodes.Items, *node)
		}
		return nodes
	}

	It("prefers topology work over a rolling update when both are pending", func() {
		cluster := newCluster()
		nodes := nodeList(cluster, 1)
		nodes.Items[0].Spec.Image = "valkey/valkey:9.0.0"

		operation := desiredClusterOperation(cluster, nodes, configHash)

		Expect(operation).NotTo(BeNil())
		Expect(operation.Type).To(Equal(valkeyiov1alpha1.OperationScaleOut))
		Expect(operation.Phase).To(Equal(valkeyiov1alpha1.OperationPhaseCreatingNodes))
	})

	It("does not let a rolling update replace an active topology operation", func() {
		cluster := newCluster()
		cluster.Status.ActiveOperation = newClusterOperation(cluster, valkeyiov1alpha1.OperationScaleOut, valkeyiov1alpha1.OperationPhaseJoiningNodes, configHash)
		nodes := nodeList(cluster, 2)
		nodes.Items[0].Spec.Image = "valkey/valkey:9.0.0"

		changed := syncActiveOperation(cluster, desiredClusterOperation(cluster, nodes, configHash))

		Expect(changed).To(BeFalse())
		Expect(cluster.Status.ActiveOperation.Type).To(Equal(valkeyiov1alpha1.OperationScaleOut))
		Expect(cluster.Status.ActiveOperation.Phase).To(Equal(valkeyiov1alpha1.OperationPhaseJoiningNodes))
	})

	It("lets topology work replace an active rolling update", func() {
		cluster := newCluster()
		cluster.Status.ActiveOperation = newClusterOperation(cluster, valkeyiov1alpha1.OperationRollingUpdate, valkeyiov1alpha1.OperationPhaseRollingNodes, configHash)

		changed := syncActiveOperation(cluster, desiredClusterOperation(cluster, nodeList(cluster, 1), configHash))

		Expect(changed).To(BeTrue())
		Expect(cluster.Status.ActiveOperation.Type).To(Equal(valkeyiov1alpha1.OperationScaleOut))
	})
})
