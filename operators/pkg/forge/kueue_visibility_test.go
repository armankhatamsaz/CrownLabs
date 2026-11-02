// Copyright 2020-2026 Politecnico di Torino
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

package forge_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("The pending workloads of the visibility API of Kueue", func() {
	// summary is a response of the pending workloads endpoint of a ClusterQueue, as Kueue v0.18.3 writes it: three tenant
	// namespaces, each with a LocalQueue named like the ClusterQueue (so positionInLocalQueue repeats the ClusterQueue
	// position, P12a), the second workload owned by something that is not a Job.
	const summary = `{"kind":"PendingWorkloadsSummary","apiVersion":"visibility.kueue.x-k8s.io/v1beta2","metadata":{},"items":[
	 {"metadata":{"name":"job-a1-4f2c1","namespace":"tenant-m1","creationTimestamp":"2026-11-02T12:00:00Z",
	   "ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","name":"a1","uid":"uid-a1","controller":true}]},
	  "priority":0,"localQueueName":"crownlabs-ws","positionInClusterQueue":0,"positionInLocalQueue":0},
	 {"metadata":{"name":"pod-x-1","namespace":"tenant-m2","ownerReferences":[{"apiVersion":"v1","kind":"Pod","name":"x","uid":"uid-x"}]},
	  "priority":0,"localQueueName":"crownlabs-ws","positionInClusterQueue":1,"positionInLocalQueue":1},
	 {"metadata":{"name":"job-b2-77aa0","namespace":"tenant-m2",
	   "ownerReferences":[{"apiVersion":"example.com/v1","kind":"Job","name":"b2","uid":"uid-other"},
	                      {"apiVersion":"batch/v1","kind":"Job","name":"b2","uid":"uid-b2"}]},
	  "priority":100,"localQueueName":"crownlabs-ws","positionInClusterQueue":2,"positionInLocalQueue":2}]}`

	Describe("The forge.KueuePendingWorkloadsPath function", func() {
		It("Should be the ClusterQueue endpoint of v1beta2", func() {
			Expect(forge.KueuePendingWorkloadsPath("crownlabs-ws")).To(
				Equal("/apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/crownlabs-ws/pendingworkloads"))
		})
	})

	Describe("The forge.KueueDecodePendingWorkloads function", func() {
		It("Should read the name, the owner Job and the position in the ClusterQueue of each workload, in order", func() {
			workloads, err := forge.KueueDecodePendingWorkloads([]byte(summary))
			Expect(err).ToNot(HaveOccurred())
			Expect(workloads).To(Equal([]forge.KueuePendingWorkload{
				{Namespace: "tenant-m1", Name: "job-a1-4f2c1", JobUID: "uid-a1", JobName: "a1", PositionInClusterQueue: 0},
				{Namespace: "tenant-m2", Name: "pod-x-1", PositionInClusterQueue: 1},
				{Namespace: "tenant-m2", Name: "job-b2-77aa0", JobUID: "uid-b2", JobName: "b2", PositionInClusterQueue: 2},
			}))
		})

		It("Should read an empty queue", func() {
			workloads, err := forge.KueueDecodePendingWorkloads(
				[]byte(`{"kind":"PendingWorkloadsSummary","apiVersion":"visibility.kueue.x-k8s.io/v1beta2","metadata":{},"items":[]}`))
			Expect(err).ToNot(HaveOccurred())
			Expect(workloads).To(BeEmpty())
		})

		DescribeTable("Should refuse what is not a list of pending workloads",
			func(data, message string) {
				_, err := forge.KueueDecodePendingWorkloads([]byte(data))
				Expect(err).To(MatchError(ContainSubstring(message)))
			},
			Entry("not JSON (e.g., an HTML error page)", `<html>503</html>`, "failed decoding the pending workloads"),
			Entry("items of another type", `{"items":{"a":1}}`, "failed decoding the pending workloads"),
			Entry("a workload without a position", `{"items":[{"metadata":{"name":"w","namespace":"n"}}]}`,
				"the pending workload n/w has no positionInClusterQueue"),
		)
	})

	Describe("The forge.KueueQueuePositions function", func() {
		It("Should give each Job its position from 1, by UID, and skip the workloads not owned by a batch Job", func() {
			workloads, err := forge.KueueDecodePendingWorkloads([]byte(summary))
			Expect(err).ToNot(HaveOccurred())
			Expect(forge.KueueQueuePositions(workloads)).To(Equal(map[types.UID]int32{"uid-a1": 1, "uid-b2": 3}))
		})

		It("Should be empty with no workloads", func() {
			Expect(forge.KueueQueuePositions(nil)).To(BeEmpty())
		})
	})
})
