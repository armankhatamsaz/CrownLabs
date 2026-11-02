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

package crownjob_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/controller/crownjob"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("The CrownJob controller and the queue policies", func() {
	sh := []string{"sh", "-c", "sleep 1"}
	// noon is the time of the reconcilers between the tests, a Monday (rome is loaded by BeforeSuite).
	noon := func() time.Time { return time.Date(2026, 11, 2, 12, 0, 0, 0, rome) }

	reconcileQueuesDefault := func() {
		_, err := queueReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: forge.CrownJobRootCohort}})
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
	}

	BeforeEach(func() { drainEvents() })
	AfterEach(func() {
		testClock.SetTime(noon())
		reconcileQueuesDefault()
	})

	Describe("Rule 8, with the job windows and the job capacity", func() {
		It("Should create a job above the maximum in force but within a window's, which waits for the window", func() {
			cj := newCrownJob("inst-win-big", sh, "")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			_, err := getJob(cj.Name)
			Expect(err).ToNot(HaveOccurred(), "the Job is created")
			reconcileCrownJob(cj)
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonWaitingForWindow,
				"Your job needs 8 of cpu at the same time, which workspace ws-win allows only during a job window: it can start from the next one (Mon 20:00).")
		})

		It("Should refuse a job above the job capacity", func() {
			withCapacity := crownJobReconciler
			withCapacity.Capacity = corev1.ResourceList{"cpu": resource.MustParse("400m")}
			cj := newCrownJob("inst-win", sh, "")
			_, err := withCapacity.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
			Expect(err).ToNot(HaveOccurred())
			expectPhase(cj, clv1alpha2.CrownJobPhaseFailed, crownjob.ReasonCannotStart,
				"The job asks for 500m of cpu, but jobs can use at most 400m in total: it could never start.")
			_, err = getJob(cj.Name)
			Expect(kerrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Describe("The time limit of a job, with the job windows", func() {
		It("Should wait for the window that allows the time limit, then create the Job with it at the window's start", func() {
			cj := newCrownJob("inst-win", sh, "6h")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 8*time.Hour + time.Second}), "20:00 is 8 h away, plus 1 s")
			message := "Your job asks for a time limit of 6h, which workspace ws-win allows only during a job window: it will join the queue at the next one (Mon 20:00)."
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonWaitingForWindow, message)
			Expect(drainEvents()).To(ConsistOf("Normal WaitingForWindow " + message))
			_, err := getJob(cj.Name)
			Expect(kerrors.IsNotFound(err)).To(BeTrue(), "no Job before the window")
			Expect(crownJobStatus(cj).RunFolder).To(BeEmpty())

			By("At the start of the window")
			testClock.SetTime(time.Date(2026, 11, 2, 20, 0, 1, 0, rome))
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, "Your job is waiting in the queue of workspace ws-win.")
			job, err := getJob(cj.Name)
			Expect(err).ToNot(HaveOccurred())
			Expect(job.Spec.Template.Spec.ActiveDeadlineSeconds).To(Equal(ptr.To[int64](6 * 3600)))
			Expect(job.Spec.ActiveDeadlineSeconds).To(Equal(ptr.To[int64](6*3600 + 600)))
		})

		It("Should refuse a time limit that no window allows, telling the largest", func() {
			cj := newCrownJob("inst-win", sh, "7h")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseFailed, crownjob.ReasonCannotStart, "The job asks for a time limit of 7h, but at most 6h is allowed.")
		})

		It("Should accept, during the window, the time limit it allows", func() {
			testClock.SetTime(time.Date(2026, 11, 2, 21, 0, 0, 0, rome))
			cj := newCrownJob("inst-win", sh, "6h")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, "Your job is waiting in the queue of workspace ws-win.")
		})
	})

	Describe("The reason of a waiting job, from the state of the queue", func() {
		// The job capacity is 3 units (1500m); ws-ra is guaranteed 1 unit and may use 2; ws-rb may use 6. So the pool
		// is 2 units (1 cpu). The jobs of inst-ra and inst-rb need 1 unit, those of inst-rb-big 2 units.
		var setReservation func(ws, cpu string)

		BeforeEach(func() {
			var ra clv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: reasonWorkspaceA}, &ra)).To(Succeed())
			ra.Spec.QueueLimits.Guaranteed = map[string]resource.Quantity{"cpu": resource.MustParse("500m")}
			Expect(k8sClient.Update(ctx, &ra)).To(Succeed())
			withCapacity := crownjob.QueueReconciler{Client: k8sClient, APIReader: k8sClient, TargetLabel: targetLabel,
				EventsRecorder: record.NewFakeRecorder(100), Capacity: corev1.ResourceList{"cpu": resource.MustParse("1500m")},
				DefaultTimeZone: time.UTC, Clock: testClock, ReconcileDeferHook: GinkgoRecover}
			_, err := withCapacity.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: forge.CrownJobRootCohort}})
			Expect(err).ToNot(HaveOccurred())

			// setReservation writes the status of a ClusterQueue as Kueue does: the quota reserved by its workloads.
			setReservation = func(ws, cpu string) {
				queue := &unstructured.Unstructured{}
				queue.SetGroupVersionKind(forge.KueueClusterQueueGVK)
				ExpectWithOffset(1, k8sClient.Get(ctx, types.NamespacedName{Name: forge.CrownJobQueueName(ws)}, queue)).To(Succeed())
				ExpectWithOffset(1, unstructured.SetNestedSlice(queue.Object, []interface{}{map[string]interface{}{
					"name": forge.CrownJobResourceFlavor,
					"resources": []interface{}{
						map[string]interface{}{"name": "cpu", "total": cpu, "borrowed": "0"},
						map[string]interface{}{"name": "memory", "total": "0", "borrowed": "0"},
					},
				}}, "status", "flavorsReservation")).To(Succeed())
				ExpectWithOffset(1, k8sClient.Status().Update(ctx, queue)).To(Succeed())
			}
		})

		AfterEach(func() {
			setReservation(reasonWorkspaceA, "0")
			setReservation(reasonWorkspaceB, "0")
			var ra clv1alpha1.Workspace
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: reasonWorkspaceA}, &ra)).To(Succeed())
			ra.Spec.QueueLimits.Guaranteed = nil
			Expect(k8sClient.Update(ctx, &ra)).To(Succeed())
		})

		type reasonCase struct {
			instance, reservedA, reservedB, reason, message string
		}
		DescribeTable("Each reason, with the numbers of the queue",
			func(c reasonCase) {
				setReservation(reasonWorkspaceA, c.reservedA)
				setReservation(reasonWorkspaceB, c.reservedB)
				cj := newCrownJob(c.instance, sh, "")
				reconcileCrownJob(cj)
				_, err := getJob(cj.Name)
				Expect(err).ToNot(HaveOccurred())
				Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
				expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, c.reason, c.message)
			},
			Entry("QuotaReached: ws-ra uses its 2 units, with units free", reasonCase{instance: "inst-ra", reservedA: "1", reservedB: "0",
				reason:  crownjob.ReasonQuotaReached,
				message: "Workspace ws-ra is already using everything its jobs may use at the same time: your job will start when one of its jobs ends."}),
			Entry("GuaranteeInUse: ws-rb uses the pool and the unit reserved for ws-ra", reasonCase{instance: "inst-ra", reservedA: "0", reservedB: "1500m",
				reason:  crownjob.ReasonGuaranteeInUse,
				message: "Other workspaces are using the share reserved for workspace ws-ra: your job will start as soon as one of their jobs ends. No job is stopped."}),
			Entry("CapacityReached: nothing is free, and ws-rb has no guarantee", reasonCase{instance: "inst-rb", reservedA: "0", reservedB: "1500m",
				reason: crownjob.ReasonCapacityReached, message: "All the resources for jobs are in use: your job will start when another job ends."}),
			Entry("LargerThanFree: 1 unit free, the job needs 2", reasonCase{instance: "inst-rb-big", reservedA: "0", reservedB: "1",
				reason:  crownjob.ReasonLargerThanFree,
				message: "Your job needs 1 of cpu at the same time, and only 500m is free now: smaller jobs may start before it."}),
			Entry("InQueue: the job fits", reasonCase{instance: "inst-rb", reservedA: "0", reservedB: "500m",
				reason: crownjob.ReasonInQueue, message: "Your job is waiting in the queue of workspace ws-rb."}),
		)

		It("Should give their reason to two identical waiting jobs, with no Workload at all", func() {
			setReservation(reasonWorkspaceB, "1500m")
			first, second := newCrownJob("inst-rb", sh, ""), newCrownJob("inst-rb", sh, "")
			for _, cj := range []*clv1alpha2.CrownJob{first, second} {
				reconcileCrownJob(cj)
				reconcileCrownJob(cj)
				expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonCapacityReached,
					"All the resources for jobs are in use: your job will start when another job ends.")
			}

			By("When a unit frees up, both read InQueue at their next reconcile")
			setReservation(reasonWorkspaceB, "1")
			for _, cj := range []*clv1alpha2.CrownJob{first, second} {
				reconcileCrownJob(cj)
				expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, "Your job is waiting in the queue of workspace ws-rb.")
			}
		})
	})
})
