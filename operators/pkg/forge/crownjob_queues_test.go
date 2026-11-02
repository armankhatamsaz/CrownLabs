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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("Job queue quotas", func() {
	quantities := func(kv ...string) map[string]resource.Quantity {
		m := map[string]resource.Quantity{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = resource.MustParse(kv[i+1])
		}
		return m
	}
	list := func(kv ...string) corev1.ResourceList {
		l := corev1.ResourceList{}
		for name, q := range quantities(kv...) {
			l[corev1.ResourceName(name)] = q
		}
		return l
	}
	// q("cpu", "1", "3") is nominal 1, borrowing limit 3; q("cpu", "1") has no borrowing limit.
	q := func(name, nominal string, borrowing ...string) forge.KueueResourceQuota {
		quota := forge.KueueResourceQuota{Name: corev1.ResourceName(name), Nominal: resource.MustParse(nominal)}
		if len(borrowing) > 0 {
			quota.BorrowingLimit = ptr.To(resource.MustParse(borrowing[0]))
		}
		return quota
	}
	const gpu = "nvidia.com/gpu"
	cpuMem := []corev1.ResourceName{"cpu", "memory"}

	Describe("The forge.KueueMaximum function", func() {
		It("Should be maxAtOnce, else no limit for cpu and memory, and 0 for any other resource", func() {
			limits := clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "2", gpu, "1")}
			Expect(forge.KueueMaximum(&limits, "cpu").String()).To(Equal("2"))
			Expect(forge.KueueMaximum(&limits, gpu).String()).To(Equal("1"))
			Expect(forge.KueueMaximum(&limits, "memory")).To(BeNil())
			Expect(forge.KueueMaximum(&clv1alpha1.QueueLimits{}, "cpu")).To(BeNil())
			Expect(forge.KueueMaximum(&limits, "example.com/fpga").IsZero()).To(BeTrue())
		})
	})

	Describe("The forge.KueueCoveredResources function", func() {
		It("Should cover cpu and memory first, then every resource named anywhere, sorted, and keep the previous ones", func() {
			spec := clv1alpha1.WorkspaceSpec{
				QueueLimits: &clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "2", gpu, "1")},
				Windows: []clv1alpha1.JobWindow{{Days: []clv1alpha1.WeekDay{"Mon"}, Start: "20:00", End: "24:00",
					Overrides: clv1alpha1.QueueLimits{Guaranteed: quantities("example.com/fpga", "1")}}},
			}
			Expect(forge.KueueCoveredResources([]corev1.ResourceName{"cpu", "memory", "amd.com/gpu"},
				list("example.com/zz", "4", "memory", "1Gi"), []*clv1alpha1.WorkspaceSpec{&spec})).To(Equal([]corev1.ResourceName{
				"cpu", "memory", "amd.com/gpu", "example.com/fpga", "example.com/zz", gpu,
			}))
		})

		It("Should cover cpu and memory with nothing named", func() {
			Expect(forge.KueueCoveredResources(nil, nil, nil)).To(Equal(cpuMem))
		})
	})

	Describe("The forge.KueueQueuePlanFor function", func() {
		ws := func(name string, limits clv1alpha1.QueueLimits) forge.KueueWorkspaceLimits {
			return forge.KueueWorkspaceLimits{Name: name, Limits: limits}
		}

		It("Should, without a job capacity, give every queue its maximum as its own quota, as job queues without a Cohort", func() {
			plan := forge.KueueQueuePlanFor([]forge.KueueWorkspaceLimits{
				ws("b", clv1alpha1.QueueLimits{}),
				ws("a", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "2"), Guaranteed: quantities("cpu", "1")}),
			}, nil, []corev1.ResourceName{"cpu", "memory", gpu})
			Expect(forge.KueueQuotasEqual(plan.Queues["a"], []forge.KueueResourceQuota{q("cpu", "2", "0"), q("memory", "100Ti", "0"), q(gpu, "0", "0")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Queues["a"]))
			Expect(forge.KueueQuotasEqual(plan.Queues["b"], []forge.KueueResourceQuota{q("cpu", "10k", "0"), q("memory", "100Ti", "0"), q(gpu, "0", "0")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Queues["b"]))
			Expect(forge.KueueQuotasEqual(plan.Pool, []forge.KueueResourceQuota{q("cpu", "10k"), q("memory", "100Ti"), q(gpu, "10k")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Pool))
			Expect(plan.Events).To(BeEmpty())
		})

		It("Should guarantee the maximum when the guarantee is above it, with an event", func() {
			plan := forge.KueueQueuePlanFor([]forge.KueueWorkspaceLimits{
				ws("a", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "2"), Guaranteed: quantities("cpu", "3")}),
			}, list("cpu", "4"), cpuMem)
			Expect(forge.KueueQuotasEqual(plan.Queues["a"], []forge.KueueResourceQuota{q("cpu", "2", "0"), q("memory", "100Ti", "0")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Queues["a"]))
			Expect(forge.KueueQuotasEqual(plan.Pool, []forge.KueueResourceQuota{q("cpu", "2"), q("memory", "100Ti")})).To(BeTrue())
			Expect(plan.Events).To(ConsistOf(forge.KueueQueueEvent{Workspace: "a", Reason: "GuaranteeAboveMaximum",
				Message: "The guarantee of the jobs of this workspace is above their maximum (cpu 3 (maximum 2)): the maximum is guaranteed instead."}))
		})

		It("Should serve the guarantees in workspace name order within the capacity, and cut the last (Q4)", func() {
			// Capacity 3: p12b-a is guaranteed 2, p12b-b nothing, p12b-c 2 (it gets 1).
			plan := forge.KueueQueuePlanFor([]forge.KueueWorkspaceLimits{
				ws("p12b-c", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "3"), Guaranteed: quantities("cpu", "2")}),
				ws("p12b-a", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "2"), Guaranteed: quantities("cpu", "2")}),
				ws("p12b-b", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "3")}),
			}, list("cpu", "3"), cpuMem)
			Expect(forge.KueueQuotasEqual(plan.Queues["p12b-a"], []forge.KueueResourceQuota{q("cpu", "2", "0"), q("memory", "100Ti", "0")})).To(BeTrue())
			Expect(forge.KueueQuotasEqual(plan.Queues["p12b-b"], []forge.KueueResourceQuota{q("cpu", "0", "3"), q("memory", "100Ti", "0")})).To(BeTrue())
			Expect(forge.KueueQuotasEqual(plan.Queues["p12b-c"], []forge.KueueResourceQuota{q("cpu", "1", "2"), q("memory", "100Ti", "0")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Queues["p12b-c"]))
			Expect(forge.KueueQuotasEqual(plan.Pool, []forge.KueueResourceQuota{q("cpu", "0"), q("memory", "100Ti")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Pool))
			Expect(plan.Events).To(ConsistOf(forge.KueueQueueEvent{Workspace: "p12b-c", Reason: "GuaranteeCut",
				Message: "The job capacity (cpu=3) cannot hold every guarantee, served in workspace name order: this workspace gets cpu 1 of the 2 it is guaranteed."}))
		})

		It("Should keep the pool equal to the capacity minus the guarantees (Q1: 3 units of 500m, a guaranteed 1)", func() {
			plan := forge.KueueQueuePlanFor([]forge.KueueWorkspaceLimits{
				ws("p12b-a", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "1"), Guaranteed: quantities("cpu", "500m")}),
				ws("p12b-b", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "1500m")}),
			}, list("cpu", "1500m"), cpuMem)
			Expect(forge.KueueQuotasEqual(plan.Pool, []forge.KueueResourceQuota{q("cpu", "1"), q("memory", "100Ti")})).To(BeTrue(),
				forge.FormatKueueQuotas(plan.Pool))
			Expect(forge.KueueQuotasEqual(plan.Queues["p12b-a"], []forge.KueueResourceQuota{q("cpu", "500m", "500m"), q("memory", "100Ti", "0")})).To(BeTrue())
			Expect(forge.KueueQuotasEqual(plan.Queues["p12b-b"], []forge.KueueResourceQuota{q("cpu", "0", "1500m"), q("memory", "100Ti", "0")})).To(BeTrue())

			By("A job of p12b-a within its guarantee borrows nothing, memory included (Q1: Kueue serves such jobs first)")
			for _, r := range []corev1.ResourceName{"cpu", "memory"} {
				request := map[corev1.ResourceName]string{"cpu": "500m", "memory": "1Gi"}[r]
				for _, quota := range plan.Queues["p12b-a"] {
					if quota.Name == r {
						requested := resource.MustParse(request)
						Expect(requested.Cmp(quota.Nominal)).To(BeNumerically("<=", 0), "the job would borrow %s", r)
					}
				}
			}
		})

		It("Should give no quota to a covered resource no longer named, and an unlimited pool when it is not in the capacity", func() {
			plan := forge.KueueQueuePlanFor([]forge.KueueWorkspaceLimits{ws("a", clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "1")})},
				list("cpu", "4", gpu, "2"), []corev1.ResourceName{"cpu", "memory", "amd.com/gpu", gpu})
			Expect(forge.KueueQuotasEqual(plan.Queues["a"], []forge.KueueResourceQuota{
				q("cpu", "0", "1"), q("memory", "100Ti", "0"), q("amd.com/gpu", "0", "0"), q(gpu, "0", "0")})).To(BeTrue(), forge.FormatKueueQuotas(plan.Queues["a"]))
			Expect(forge.KueueQuotasEqual(plan.Pool, []forge.KueueResourceQuota{
				q("cpu", "4"), q("memory", "100Ti"), q("amd.com/gpu", "10k"), q(gpu, "2")})).To(BeTrue(), forge.FormatKueueQuotas(plan.Pool))
		})
	})

	DescribeTable("The forge.KueuePhase1 function",
		func(current, desired, phase1 []forge.KueueResourceQuota) {
			got := forge.KueuePhase1(current, desired)
			Expect(forge.KueueQuotasEqual(got, phase1)).To(BeTrue(), "got %s", forge.FormatKueueQuotas(got))
		},
		Entry("a decrease is applied", []forge.KueueResourceQuota{q("cpu", "2", "2")}, []forge.KueueResourceQuota{q("cpu", "1", "1")},
			[]forge.KueueResourceQuota{q("cpu", "1", "1")}),
		Entry("an increase waits for phase 2", []forge.KueueResourceQuota{q("cpu", "1", "1")}, []forge.KueueResourceQuota{q("cpu", "2", "3")},
			[]forge.KueueResourceQuota{q("cpu", "1", "1")}),
		Entry("each field on its own", []forge.KueueResourceQuota{q("cpu", "1", "3")}, []forge.KueueResourceQuota{q("cpu", "2", "0")},
			[]forge.KueueResourceQuota{q("cpu", "1", "0")}),
		Entry("a new object or resource starts at 0", nil, []forge.KueueResourceQuota{q("cpu", "2", "1"), q("memory", "1Gi")},
			[]forge.KueueResourceQuota{q("cpu", "0", "0"), q("memory", "0", "0")}),
		Entry("an unset current borrowing limit is no limit", []forge.KueueResourceQuota{q("cpu", "2")}, []forge.KueueResourceQuota{q("cpu", "2", "1")},
			[]forge.KueueResourceQuota{q("cpu", "2", "1")}),
		Entry("an unset desired borrowing limit keeps the current one", []forge.KueueResourceQuota{q("cpu", "0", "1")}, []forge.KueueResourceQuota{q("cpu", "0")},
			[]forge.KueueResourceQuota{q("cpu", "0", "1")}),
		Entry("both unset stay unset", []forge.KueueResourceQuota{q("memory", "1Gi")}, []forge.KueueResourceQuota{q("memory", "0")},
			[]forge.KueueResourceQuota{q("memory", "0")}),
		Entry("the order is the desired one", []forge.KueueResourceQuota{q("memory", "0"), q("cpu", "1")}, []forge.KueueResourceQuota{q("cpu", "1"), q("memory", "0")},
			[]forge.KueueResourceQuota{q("cpu", "1"), q("memory", "0")}),
	)

	Describe("The forge.KueueCohortFree function", func() {
		It("Should add the nominal quotas of the members to the pool and subtract what they use, below zero when over-committed", func() {
			free := forge.KueueCohortFree([]forge.KueueResourceQuota{q("cpu", "2"), q("memory", "100Ti")}, []forge.KueueQueueState{
				{Quotas: []forge.KueueResourceQuota{q("cpu", "1", "1")}, Used: list("cpu", "3")},
				{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4")}, Used: list("cpu", "1")},
			})
			Expect(free.Cpu().String()).To(Equal("-1"))
			Expect(free.Memory().String()).To(Equal("100Ti"))
		})
	})

	// The cases of P12a (results/p12a/DESIGN-P12.md §4), with 1 unit = 1 cpu.
	type waitingCase struct {
		requests corev1.ResourceList
		state    forge.KueueQueueState
		reason   forge.CrownJobWaiting
		resource corev1.ResourceName
		above    bool
	}
	DescribeTable("The forge.CrownJobWaitingReason function",
		func(c waitingCase) {
			got := forge.CrownJobWaitingReason(c.requests, &c.state)
			Expect(got.Reason).To(Equal(c.reason))
			Expect(got.AboveMaximum).To(Equal(c.above))
			if c.resource != "" {
				Expect(got.Resource).To(Equal(c.resource))
			}
		},
		Entry("E1b: own maximum reached with units free (a uses 4 of its maximum 4, 2 free)", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4")}, Used: list("cpu", "4"), Free: list("cpu", "2")},
			reason:   forge.CrownJobWaitingQuotaReached, resource: "cpu"}),
		Entry("E2: guarantee lent and in use (a guaranteed 2 uses 0, nothing free)", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "2", "4")}, Used: list("cpu", "0"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingGuaranteeInUse}),
		Entry("E2: the guarantee covers a second job too (uses 1 of 2)", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "2", "4")}, Used: list("cpu", "1"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingGuaranteeInUse}),
		Entry("a 2-unit job that its guarantee does not cover (1 of 2 used), nothing free", waitingCase{
			requests: list("cpu", "2"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "2", "4")}, Used: list("cpu", "1"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingCapacityReached}),
		Entry("E1: the pool used up (no guarantee, nothing free)", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "6")}, Used: list("cpu", "3"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingCapacityReached}),
		Entry("E2: the guarantee in use beyond it (uses 2 of 2, the next job borrows)", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "2", "4")}, Used: list("cpu", "2"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingCapacityReached}),
		Entry("E3b: over-committed (6 on 4) reads as capacity reached", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "6")}, Used: list("cpu", "2"), Free: list("cpu", "-2")},
			reason:   forge.CrownJobWaitingCapacityReached}),
		Entry("E5: a 2-unit job with nothing free", waitingCase{
			requests: list("cpu", "2"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "6")}, Used: list("cpu", "3"), Free: list("cpu", "0")},
			reason:   forge.CrownJobWaitingCapacityReached}),
		Entry("E5b: a 2-unit job with 1 unit free", waitingCase{
			requests: list("cpu", "2"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "6")}, Used: list("cpu", "3"), Free: list("cpu", "1")},
			reason:   forge.CrownJobWaitingLargerThanFree}),
		Entry("a job that fits waits for its turn", waitingCase{
			requests: list("cpu", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "6")}, Used: list("cpu", "3"), Free: list("cpu", "1")},
			reason:   forge.CrownJobWaitingInQueue}),
		Entry("Q6: a job alone above the maximum in force (3 on 0 + 2)", waitingCase{
			requests: list("cpu", "3"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "2")}, Free: list("cpu", "3")},
			reason:   forge.CrownJobWaitingQuotaReached, above: true}),
		Entry("an unlimited maximum is never reached", waitingCase{
			requests: list("memory", "64Gi"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("memory", "0")}, Used: list("memory", "1Ti"), Free: list("memory", "100Ti")},
			reason:   forge.CrownJobWaitingInQueue}),
		Entry("the first reason by rank wins over the resource order (cpu LargerThanFree, gpu QuotaReached)", waitingCase{
			requests: list("cpu", "2", gpu, "1"),
			state: forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4"), q(gpu, "0", "1")},
				Used: list(gpu, "1"), Free: list("cpu", "1", gpu, "1")},
			reason: forge.CrownJobWaitingQuotaReached, resource: gpu}),
		Entry("among equal reasons, the first resource by name", waitingCase{
			requests: list("memory", "2Gi", "cpu", "2"),
			state: forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4"), q("memory", "0", "4Gi")},
				Free: list("cpu", "1", "memory", "1Gi")},
			reason: forge.CrownJobWaitingLargerThanFree, resource: "cpu"}),
		Entry("a resource the queue does not cover is its maximum reached", waitingCase{
			requests: list("example.com/fpga", "1"),
			state:    forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4")}, Free: list("cpu", "4")},
			reason:   forge.CrownJobWaitingQuotaReached, above: true}),
		Entry("a zero request is ignored, even of a resource used above its maximum", waitingCase{
			requests: list("cpu", "1", gpu, "0"),
			state: forge.KueueQueueState{Quotas: []forge.KueueResourceQuota{q("cpu", "0", "4"), q(gpu, "0", "0")},
				Used: list(gpu, "1"), Free: list("cpu", "4")},
			reason: forge.CrownJobWaitingInQueue}),
	)

	It("Should report the numbers of LargerThanFree", func() {
		got := forge.CrownJobWaitingReason(list("cpu", "1"), &forge.KueueQueueState{
			Quotas: []forge.KueueResourceQuota{q("cpu", "0", "3")}, Free: list("cpu", "500m")})
		Expect(got.Reason).To(Equal(forge.CrownJobWaitingLargerThanFree))
		Expect(got.Requested.String()).To(Equal("1"))
		Expect(got.Free.String()).To(Equal("500m"))
	})
})
