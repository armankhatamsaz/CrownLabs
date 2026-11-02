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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/controller/crownjob"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// perUserCounter numbers the CrownJobs of the per-user tests, in creation order.
var perUserCounter = 0

// newMemberCrownJob creates a CrownJob of the instance in the given namespace (the member), named pu-<n>.
func newMemberCrownJob(namespace, instanceName string) *clv1alpha2.CrownJob {
	perUserCounter++
	cj := &clv1alpha2.CrownJob{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pu-%d", perUserCounter), Namespace: namespace},
		Spec:       clv1alpha2.CrownJobSpec{Instance: clv1alpha2.GenericRef{Name: instanceName}, Command: []string{"sh", "-c", "sleep 1"}},
	}
	ExpectWithOffset(1, k8sClient.Create(ctx, cj)).To(Succeed())
	return cj
}

// memberJobExists returns whether the Job of the given CrownJob exists.
func memberJobExists(cj *clv1alpha2.CrownJob) bool {
	var job batchv1.Job
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cj), &job)
	if err != nil {
		ExpectWithOffset(1, kerrors.IsNotFound(err)).To(BeTrue())
	}
	return err == nil
}

// endMemberJob simulates the Job controller ending the Job of the given CrownJob successfully.
func endMemberJob(cj *clv1alpha2.CrownJob) {
	var job batchv1.Job
	ExpectWithOffset(1, k8sClient.Get(ctx, client.ObjectKeyFromObject(cj), &job)).To(Succeed())
	updateJobStatus(&job, func(status *batchv1.JobStatus) {
		status.StartTime = ptr.To(metav1.Now())
		status.CompletionTime = ptr.To(metav1.Now())
		status.Succeeded = 1
		status.Conditions = []batchv1.JobCondition{
			jobCondition(batchv1.JobSuccessCriteriaMet, "CompletionsReached", "Reached expected number of succeeded pods"),
			jobCondition(batchv1.JobComplete, "CompletionsReached", "Reached expected number of succeeded pods"),
		}
	})
}

// cleanUpPerUser deletes the CrownJobs of the per-user instances and the Jobs of the per-user job queues: the count of
// a member is over the Jobs of its namespace, and the order over its held CrownJobs.
func cleanUpPerUser() {
	for _, namespace := range []string{tenantNamespace, otherNamespace} {
		var crownJobs clv1alpha2.CrownJobList
		ExpectWithOffset(1, k8sClient.List(ctx, &crownJobs, client.InNamespace(namespace))).To(Succeed())
		for i := range crownJobs.Items {
			if strings.HasPrefix(crownJobs.Items[i].Spec.Instance.Name, "inst-pu") {
				ExpectWithOffset(1, client.IgnoreNotFound(k8sClient.Delete(ctx, &crownJobs.Items[i]))).To(Succeed())
			}
		}
		for _, ws := range []string{perUserWorkspace, perUserWindowWorkspace} {
			ExpectWithOffset(1, k8sClient.DeleteAllOf(ctx, &batchv1.Job{}, client.InNamespace(namespace),
				client.MatchingLabels{forge.KueueQueueNameLabel: forge.CrownJobQueueName(ws)},
				client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		}
	}
	EventuallyWithOffset(1, func() int {
		count := 0
		for _, ws := range []string{perUserWorkspace, perUserWindowWorkspace} {
			var jobs batchv1.JobList
			Expect(k8sClient.List(ctx, &jobs, client.MatchingLabels{forge.KueueQueueNameLabel: forge.CrownJobQueueName(ws)})).To(Succeed())
			count += len(jobs.Items)
		}
		return count
	}).Should(BeZero())
}

var _ = Describe("The per-user limit, held by the CrownJob controller (P12c)", func() {
	noon := func() time.Time { return time.Date(2026, 11, 2, 12, 0, 0, 0, rome) }
	const usingLimit = "You are already using your own limit in workspace ws-pu (1 of cpu in the queue or running): your job will join the queue when one of your jobs ends."
	const inQueue = "Your job is waiting in the queue of workspace ws-pu."

	BeforeEach(func() {
		drainEvents()
		cleanUpPerUser()
	})
	AfterEach(func() {
		testClock.SetTime(noon())
		crownJobReconciler.SetBeforeJobCreate(nil)
		cleanUpPerUser()
	})

	It("Should hold the jobs above the limit of their member, with no Job, and release them oldest first as the member's jobs end", func() {
		By("An older job of the member is held in another workspace: it does not count, and does not go first")
		otherWorkspace := []*clv1alpha2.CrownJob{newMemberCrownJob(tenantNamespace, "inst-puwin"), newMemberCrownJob(tenantNamespace, "inst-puwin")}
		for _, cj := range otherWorkspace {
			reconcileCrownJob(cj)
		}
		Expect(isHeld(otherWorkspace[1])).To(BeTrue())

		By("Two jobs of 500m fill the limit of 1 cpu of the member")
		cj1, cj2 := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
		for _, cj := range []*clv1alpha2.CrownJob{cj1, cj2} {
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, inQueue)
			Expect(memberJobExists(cj)).To(BeTrue())
		}

		By("The third and the fourth are held, with no Job and no position")
		cj3, cj4 := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
		drainEvents()
		for _, cj := range []*clv1alpha2.CrownJob{cj3, cj4} {
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached, usingLimit)
			Expect(memberJobExists(cj)).To(BeFalse())
			Expect(crownJobStatus(cj).QueuePosition).To(BeNil())
			Expect(crownJobStatus(cj).RunFolder).To(BeEmpty())
		}
		Expect(drainEvents()).To(Equal([]string{"Normal UserLimitReached " + usingLimit, "Normal UserLimitReached " + usingLimit}))

		By("Another member is not held: the limit is per member")
		other := newMemberCrownJob(otherNamespace, "inst-pu")
		_, err := crownJobReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(other)})
		Expect(err).ToNot(HaveOccurred())
		Expect(memberJobExists(other)).To(BeTrue())

		By("When the first job ends, the younger held job does not pass the older one")
		endMemberJob(cj1)
		reconcileCrownJob(cj4)
		expectPhase(cj4, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached,
			fmt.Sprintf("Your job %s, sent before this one, is waiting for your own limit in workspace ws-pu: this job will join the queue after it.", cj3.Name))
		Expect(memberJobExists(cj4)).To(BeFalse())

		By("The oldest held job is released: its Job is created now, at the back of the queue")
		Expect(reconcileCrownJob(cj3)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
		expectPhase(cj3, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, inQueue)
		Expect(memberJobExists(cj3)).To(BeTrue())

		By("The next one is held again by the count")
		reconcileCrownJob(cj4)
		expectPhase(cj4, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached, usingLimit)

		By("A deleted Job frees the limit too")
		Expect(k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: tenantNamespace, Name: cj2.Name}},
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func() bool { return memberJobExists(cj2) }).Should(BeFalse())
		reconcileCrownJob(cj4)
		expectPhase(cj4, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, inQueue)
	})

	It("Should refuse a job above the limit of its member in the defaults and in every window", func() {
		cj := newMemberCrownJob(tenantNamespace, "inst-pu-big")
		Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{}))
		expectPhase(cj, clv1alpha2.CrownJobPhaseFailed, crownjob.ReasonCannotStart,
			"Your own limit in workspace ws-pu is 1 of cpu: this job could never start.")
		Expect(memberJobExists(cj)).To(BeFalse())

		By("The largest limit of the defaults and the windows is told, not the one in force (500m)")
		cj = newMemberCrownJob(tenantNamespace, "inst-puwin-3")
		Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{}))
		expectPhase(cj, clv1alpha2.CrownJobPhaseFailed, crownjob.ReasonCannotStart,
			"Your own limit in workspace ws-puwin is 1 of cpu: this job could never start.")
	})

	It("Should count the jobs of the member without the cache, which may not have seen the last Job created", func() {
		first, second := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
		reconcileCrownJob(first)
		reconcileCrownJob(second)
		Expect(memberJobExists(first) && memberJobExists(second)).To(BeTrue())

		lagging := crownJobReconciler
		lagging.Client = jobBlindClient{Client: k8sClient}
		third := newMemberCrownJob(tenantNamespace, "inst-pu")
		_, err := lagging.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(third)})
		Expect(err).ToNot(HaveOccurred())
		expectPhase(third, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached, usingLimit)
		Expect(memberJobExists(third)).To(BeFalse())
	})

	It("Should not hold a job behind an older one whose Job exists, though the cache may still show it held", func() {
		older := newMemberCrownJob(tenantNamespace, "inst-pu")
		reconcileCrownJob(older)
		Expect(memberJobExists(older)).To(BeTrue())
		stale := crownJobStatus(older)
		var current clv1alpha2.CrownJob
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(older), &current)).To(Succeed())
		current.Status = stale
		current.Status.Reason, current.Status.Message = crownjob.ReasonUserLimitReached, usingLimit
		Expect(k8sClient.Status().Update(ctx, &current)).To(Succeed())

		younger := newMemberCrownJob(tenantNamespace, "inst-pu")
		reconcileCrownJob(younger)
		expectPhase(younger, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, inQueue)
	})

	Describe("With job windows", func() {
		It("Should wait for the window that raises the limit of the member, and create the Job at its start", func() {
			cj := newMemberCrownJob(tenantNamespace, "inst-puwin-2")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 8*time.Hour + time.Second}), "20:00 is 8 h away, plus 1 s")
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonWaitingForWindow,
				"Your job needs 1 of cpu, which your own limit in workspace ws-puwin allows only during a job window: it will join the queue at the next one (Mon 20:00).")
			Expect(memberJobExists(cj)).To(BeFalse())

			By("At the start of the window")
			testClock.SetTime(time.Date(2026, 11, 2, 20, 0, 1, 0, rome))
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, "Your job is waiting in the queue of workspace ws-puwin.")
			Expect(memberJobExists(cj)).To(BeTrue())
		})

		It("Should release a held job at the start of the window that raises the limit", func() {
			testClock.SetTime(time.Date(2026, 11, 2, 19, 59, 45, 0, rome))
			cj1, cj2 := newMemberCrownJob(tenantNamespace, "inst-puwin"), newMemberCrownJob(tenantNamespace, "inst-puwin")
			reconcileCrownJob(cj1)
			Expect(memberJobExists(cj1)).To(BeTrue())
			Expect(reconcileCrownJob(cj2)).To(Equal(ctrl.Result{RequeueAfter: 16 * time.Second}), "the window starts in 15 s, plus 1 s")
			expectPhase(cj2, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached,
				"You are already using your own limit in workspace ws-puwin (500m of cpu in the queue or running): your job will join the queue when one of your jobs ends.")

			testClock.SetTime(time.Date(2026, 11, 2, 20, 0, 1, 0, rome))
			reconcileCrownJob(cj2)
			expectPhase(cj2, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, "Your job is waiting in the queue of workspace ws-puwin.")
			Expect(memberJobExists(cj2)).To(BeTrue())
		})

		It("Should wait for the end of a window that lowers the limit of the member", func() {
			testClock.SetTime(time.Date(2026, 11, 3, 9, 0, 0, 0, rome))
			cj := newMemberCrownJob(tenantNamespace, "inst-puwin")
			Expect(reconcileCrownJob(cj)).To(Equal(ctrl.Result{RequeueAfter: 3*time.Hour + time.Second}))
			expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonWaitingForWindow,
				"Your job needs 500m of cpu, which your own limit in workspace ws-puwin does not allow during this job window: it will join the queue when the window ends (Tue 12:00).")
			Expect(memberJobExists(cj)).To(BeFalse())
		})
	})

	Describe("Two jobs of one member reconciled at the same time", func() {
		// reconcileTogether reconciles the given CrownJobs at the same time. Right before a Job is created, the reconcile
		// waits up to 200 ms for the other one: without the lock of the member, both would count before either creates.
		reconcileTogether := func(crownJobs ...*clv1alpha2.CrownJob) {
			var mu sync.Mutex
			arrived, all := 0, make(chan struct{})
			crownJobReconciler.SetBeforeJobCreate(func() {
				mu.Lock()
				arrived++
				if arrived == len(crownJobs) {
					close(all)
				}
				mu.Unlock()
				select {
				case <-all:
				case <-time.After(200 * time.Millisecond):
				}
			})
			var wg sync.WaitGroup
			for _, cj := range crownJobs {
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					_, err := crownJobReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
					Expect(err).ToNot(HaveOccurred())
				}()
			}
			wg.Wait()
			crownJobReconciler.SetBeforeJobCreate(nil)
		}

		It("Should create the Job of only one, when only one more fits", MustPassRepeatedly(10), func() {
			first := newMemberCrownJob(tenantNamespace, "inst-pu")
			reconcileCrownJob(first)
			Expect(memberJobExists(first)).To(BeTrue(), "the instance holds the finalizer: the two reconciles below do not write it")

			a, b := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
			reconcileTogether(a, b)
			created := 0
			for _, cj := range []*clv1alpha2.CrownJob{a, b} {
				if memberJobExists(cj) {
					created++
					expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonInQueue, inQueue)
				} else {
					expectPhase(cj, clv1alpha2.CrownJobPhaseWaiting, crownjob.ReasonUserLimitReached, usingLimit)
				}
			}
			Expect(created).To(Equal(1))
		})

		It("Should release only the older of two held jobs, when only one more fits", MustPassRepeatedly(10), func() {
			first, second := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
			reconcileCrownJob(first)
			reconcileCrownJob(second)
			older, younger := newMemberCrownJob(tenantNamespace, "inst-pu"), newMemberCrownJob(tenantNamespace, "inst-pu")
			reconcileCrownJob(older)
			reconcileCrownJob(younger)
			Expect(isHeld(older) && isHeld(younger)).To(BeTrue())

			endMemberJob(first)
			reconcileTogether(younger, older)
			Expect(memberJobExists(older)).To(BeTrue())
			Expect(memberJobExists(younger)).To(BeFalse())
			Expect(crownJobStatus(younger).Reason).To(Equal(crownjob.ReasonUserLimitReached))
		})
	})
})

var _ = Describe("The per-user limit, the watch and the queue position (P12c)", func() {
	BeforeEach(func() {
		drainEvents()
		cleanUpPerUser()
	})
	AfterEach(func() { cleanUpPerUser() })

	reconcileWith := func(r *crownjob.Reconciler, cj *clv1alpha2.CrownJob) {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
	}

	It("Should give no queue position to a job held by the per-user limit", func() {
		visibility := &fakeVisibility{}
		withPositions := crownJobReconciler
		withPositions.Visibility = visibility
		first := newMemberCrownJob(tenantNamespace, "inst-pu")
		second := newMemberCrownJob(tenantNamespace, "inst-pu")
		held := newMemberCrownJob(tenantNamespace, "inst-pu")
		for _, cj := range []*clv1alpha2.CrownJob{first, second} {
			reconcileWith(&withPositions, cj)
		}
		jobFirst, err := getJob(first.Name)
		Expect(err).ToNot(HaveOccurred())
		jobSecond, err := getJob(second.Name)
		Expect(err).ToNot(HaveOccurred())
		visibility.set(forge.CrownJobQueueName(perUserWorkspace), jobFirst, jobSecond)
		reconcileWith(&withPositions, first)
		Expect(crownJobStatus(first).QueuePosition).To(Equal(ptr.To[int32](1)))
		reconcileWith(&withPositions, held)
		Expect(crownJobStatus(held).Reason).To(Equal(crownjob.ReasonUserLimitReached))
		Expect(crownJobStatus(held).QueuePosition).To(BeNil())
		Expect(visibility.count()).To(Equal(1), "the visibility API is read only for a job whose Job waits")
	})

	It("Should check again the held jobs of a member when one of their Jobs is created, ends or is deleted, and only then", func() {
		By("Two jobs of the member fill its limit, two are held; one job waits in another workspace")
		waitB, _ := waitingJob("inst-rb")
		members := []*clv1alpha2.CrownJob{}
		for range 4 {
			cj := newMemberCrownJob(tenantNamespace, "inst-pu")
			reconcileCrownJob(cj)
			members = append(members, cj)
		}
		held := members[2:]
		for _, cj := range held {
			Expect(isHeld(cj)).To(BeTrue())
		}

		_, collect := startWatches()
		name := func(cj *clv1alpha2.CrownJob) string { return cj.Namespace + "/" + cj.Name }
		Expect(collect()).To(HaveKey(name(held[0])), "the initial list enqueues everything")

		By("A Job of the member that changes without ending: only its own CrownJob")
		jobOfFirst, err := getJob(members[0].Name)
		Expect(err).ToNot(HaveOccurred())
		updateJobStatus(jobOfFirst, func(status *batchv1.JobStatus) { status.StartTime = ptr.To(metav1.Now()) })
		got := collect()
		Expect(got).To(HaveKey(name(members[0])))
		for _, cj := range held {
			Expect(got).ToNot(HaveKey(name(cj)))
		}

		By("A Job of the member that ends: the held jobs of the member")
		endMemberJob(members[0])
		got = collect()
		for _, cj := range held {
			Expect(got).To(HaveKey(name(cj)))
		}
		Expect(got).ToNot(HaveKey(name(waitB)))

		By("A Job of a job queue created in the namespace, then deleted: the held jobs, each time")
		bareJob := func(name string, labels map[string]string) *batchv1.Job {
			return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: tenantNamespace, Name: name, Labels: labels},
				Spec: batchv1.JobSpec{Suspend: ptr.To(true), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "c", Image: image}}}}}}
		}
		bare := bareJob("bare-pu", map[string]string{forge.KueueQueueNameLabel: forge.CrownJobQueueName(perUserWorkspace)})
		Expect(k8sClient.Create(ctx, bare)).To(Succeed())
		got = collect()
		for _, cj := range held {
			Expect(got).To(HaveKey(name(cj)))
		}
		Expect(k8sClient.Delete(ctx, bare, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		got = collect()
		for _, cj := range held {
			Expect(got).To(HaveKey(name(cj)))
		}

		By("A Job with no job queue (a log helper): nothing")
		helper := bareJob("bare-helper", map[string]string{forge.LabelCrownJobComponentKey: forge.CrownJobComponentLogHelper})
		Expect(k8sClient.Create(ctx, helper)).To(Succeed())
		Expect(collect()).To(BeEmpty())
		Expect(k8sClient.Delete(ctx, helper, client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Expect(collect()).To(BeEmpty())
	})
})

// jobBlindClient is a client whose lists of Jobs are empty, as those of a cache that has not seen them yet.
type jobBlindClient struct {
	client.Client
}

func (c jobBlindClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*batchv1.JobList); ok {
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

// isHeld returns whether the given CrownJob is held by the per-user limit.
func isHeld(cj *clv1alpha2.CrownJob) bool {
	status := crownJobStatus(cj)
	return status.Phase == clv1alpha2.CrownJobPhaseWaiting && status.Reason == crownjob.ReasonUserLimitReached
}
