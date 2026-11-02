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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/controller/crownjob"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// queueWrite is a write of the queue controller to a ClusterQueue or to the Cohort, with the quotas the object had
// right before it (read live) and those of the response.
type queueWrite struct {
	op, kind, name string
	before, after  []forge.KueueResourceQuota
	// existed tells whether the object existed before the write.
	existed bool
}

// recordingClient records the writes to the Kueue ClusterQueues and Cohorts, in order.
type recordingClient struct {
	client.Client
	mu     sync.Mutex
	writes []queueWrite
}

func (c *recordingClient) quotasOf(ctx context.Context, obj client.Object) ([]forge.KueueResourceQuota, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || (u.GetKind() != "ClusterQueue" && u.GetKind() != "Cohort") {
		return nil, false
	}
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(u.GroupVersionKind())
	if err := c.Get(ctx, types.NamespacedName{Name: u.GetName()}, current); err != nil {
		return nil, false
	}
	quotas, _, err := forge.KueueQuotas(current)
	ExpectWithOffset(2, err).ToNot(HaveOccurred())
	return quotas, true
}

func (c *recordingClient) record(op string, obj client.Object, before []forge.KueueResourceQuota, existed bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || (u.GetKind() != "ClusterQueue" && u.GetKind() != "Cohort") {
		return
	}
	var after []forge.KueueResourceQuota
	if op != "delete" {
		var err error
		after, _, err = forge.KueueQuotas(u)
		ExpectWithOffset(2, err).ToNot(HaveOccurred())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, queueWrite{op: op, kind: u.GetKind(), name: u.GetName(), before: before, after: after, existed: existed})
}

func (c *recordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	before, existed := c.quotasOf(ctx, obj)
	err := c.Client.Patch(ctx, obj, patch, opts...)
	if err == nil {
		c.record(string(patch.Type()), obj, before, existed)
	}
	return err
}

func (c *recordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	err := c.Client.Create(ctx, obj, opts...)
	if err == nil {
		c.record("create", obj, nil, false)
	}
	return err
}

func (c *recordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	before, existed := c.quotasOf(ctx, obj)
	err := c.Client.Update(ctx, obj, opts...)
	if err == nil {
		c.record("update", obj, before, existed)
	}
	return err
}

func (c *recordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	before, existed := c.quotasOf(ctx, obj)
	err := c.Client.Delete(ctx, obj, opts...)
	if err == nil {
		c.record("delete", obj, before, existed)
	}
	return err
}

// changes returns, for a write, whether some quota went up and whether some went down. An object that did not exist
// counts as having every quota at 0; an unset borrowing limit counts as no limit.
func changes(w *queueWrite) (up, down bool) {
	before := map[corev1.ResourceName]forge.KueueResourceQuota{}
	for _, q := range w.before {
		before[q.Name] = q
	}
	compare := func(was, now *resource.Quantity) {
		switch {
		case was == nil && now == nil:
		case was == nil:
			down = true
		case now == nil:
			up = true
		case now.Cmp(*was) > 0:
			up = true
		case now.Cmp(*was) < 0:
			down = true
		}
	}
	for _, q := range w.after {
		old, found := before[q.Name]
		if !found {
			old = forge.KueueResourceQuota{Name: q.Name, BorrowingLimit: &resource.Quantity{}}
		}
		compare(&old.Nominal, &q.Nominal)
		if w.kind != "Cohort" { // a root Cohort has no borrowing limit
			compare(old.BorrowingLimit, q.BorrowingLimit)
		}
	}
	return up, down
}

// expectDecreasesFirst asserts that no write raises a quota and lowers another, and that no write lowers a quota after
// one raised a quota: every decrease comes before any increase.
func expectDecreasesFirst(writes []queueWrite) {
	firstUp := -1
	for i := range writes {
		w := &writes[i]
		up, down := changes(w)
		ExpectWithOffset(1, up && down).To(BeFalse(), "write %d (%s %s/%s) raises and lowers: %s -> %s", i, w.op, w.kind, w.name,
			forge.FormatKueueQuotas(w.before), forge.FormatKueueQuotas(w.after))
		if up && firstUp < 0 {
			firstUp = i
		}
		if down && firstUp >= 0 {
			Fail(fmt.Sprintf("write %d (%s %s/%s) lowers a quota after write %d raised one", i, w.op, w.kind, w.name, firstUp))
		}
	}
}

// describeWrites returns the writes as text, for the failure messages.
func describeWrites(writes []queueWrite) string {
	text := ""
	for i, w := range writes {
		text += fmt.Sprintf("\n  %d %s %s/%s: %s -> %s", i, w.op, w.kind, w.name, forge.FormatKueueQuotas(w.before), forge.FormatKueueQuotas(w.after))
	}
	return text
}

var _ = Describe("The CrownJob queue controller", func() {
	// These tests use their own Workspaces, since the order of the top-level containers is random. The controller
	// reconciles every Workspace together: each test restores the shared state (no capacity, its Workspaces deleted).

	var (
		recording *recordingClient
		events    *record.FakeRecorder
		created   []*clv1alpha1.Workspace
	)

	newReconciler := func(capacity corev1.ResourceList) *crownjob.QueueReconciler {
		recording = &recordingClient{Client: k8sClient}
		events = record.NewFakeRecorder(100)
		return &crownjob.QueueReconciler{
			Client: recording, APIReader: k8sClient, TargetLabel: targetLabel, EventsRecorder: events,
			Capacity: capacity, DefaultTimeZone: time.UTC, Clock: testClock, ReconcileDeferHook: GinkgoRecover,
		}
	}
	reconcileQueues := func(r *crownjob.QueueReconciler) ctrl.Result {
		result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: forge.CrownJobRootCohort}})
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		return result
	}
	createWorkspace := func(name string, limits *clv1alpha1.QueueLimits) *clv1alpha1.Workspace {
		ws := workspace(name, limits)
		ExpectWithOffset(1, k8sClient.Create(ctx, ws)).To(Succeed())
		created = append(created, ws)
		return ws
	}
	updateWorkspace := func(ws *clv1alpha1.Workspace, mutate func(*clv1alpha1.Workspace)) {
		ExpectWithOffset(1, k8sClient.Get(ctx, client.ObjectKeyFromObject(ws), ws)).To(Succeed())
		mutate(ws)
		ExpectWithOffset(1, k8sClient.Update(ctx, ws)).To(Succeed())
	}
	kueueObject := func(kind, name string) (*unstructured.Unstructured, error) {
		obj := &unstructured.Unstructured{}
		if kind == "Cohort" {
			obj.SetGroupVersionKind(forge.KueueCohortGVK)
		} else {
			obj.SetGroupVersionKind(forge.KueueClusterQueueGVK)
		}
		return obj, k8sClient.Get(ctx, types.NamespacedName{Name: name}, obj)
	}
	quotasOf := func(kind, name string) string {
		obj, err := kueueObject(kind, name)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		quotas, _, err := forge.KueueQuotas(obj)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		return forge.FormatKueueQuotas(quotas)
	}
	// cpuOf returns the quota of cpu only, as "nominal/borrowing limit".
	cpuOf := func(kind, name string) string {
		obj, err := kueueObject(kind, name)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		quotas, _, err := forge.KueueQuotas(obj)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		for _, q := range quotas {
			if q.Name == corev1.ResourceCPU {
				return forge.FormatKueueQuotas([]forge.KueueResourceQuota{q})
			}
		}
		return "absent"
	}
	cpu := func(value string) map[string]resource.Quantity {
		return map[string]resource.Quantity{"cpu": resource.MustParse(value)}
	}
	drain := func() []string {
		var out []string
		for {
			select {
			case e := <-events.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	BeforeEach(func() { created = nil })

	AfterEach(func() {
		for _, ws := range created {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ws))).To(Succeed())
		}
		testClock.SetTime(time.Date(2026, 11, 2, 12, 0, 0, 0, rome))
		_, err := queueReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: forge.CrownJobRootCohort}})
		Expect(err).ToNot(HaveOccurred())
		for _, ws := range created {
			_, err := kueueObject("ClusterQueue", forge.CrownJobQueueName(ws.Name))
			Expect(kerrors.IsNotFound(err)).To(BeTrue(), "the ClusterQueue of %s must be gone", ws.Name)
		}
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=10k/-"), "the pool must be unlimited again")
	})

	It("Should create the Cohort and the ClusterQueue of a first Workspace in two phases, every quota at 0 first", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("3")})
		createWorkspace("q-first", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2"), Guaranteed: cpu("1")})
		Expect(reconcileQueues(r)).To(Equal(ctrl.Result{RequeueAfter: 10 * time.Minute}))

		var ours []queueWrite
		for _, w := range recording.writes {
			if w.name == "crownlabs-q-first" {
				ours = append(ours, w)
			}
		}
		Expect(ours).To(HaveLen(2), describeWrites(recording.writes))
		Expect(ours[0].existed).To(BeFalse())
		Expect(ours[0].after).ToNot(BeEmpty())
		for _, q := range ours[0].after {
			Expect(q.Nominal.IsZero()).To(BeTrue(), "created with every quota at 0: %s", forge.FormatKueueQuotas(ours[0].after))
			Expect(q.BorrowingLimit).ToNot(BeNil())
			Expect(q.BorrowingLimit.IsZero()).To(BeTrue(), "created with every quota at 0: %s", forge.FormatKueueQuotas(ours[0].after))
		}
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-first")).To(Equal("cpu=1/+1"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=2/-"), "capacity 3 - guarantee 1")
		for _, w := range recording.writes {
			if w.kind == "Cohort" {
				for _, q := range w.after {
					Expect(q.BorrowingLimit).To(BeNil(), "a root Cohort has no borrowing limit: %s", describeWrites([]queueWrite{w}))
				}
			}
		}

		queue, err := kueueObject("ClusterQueue", "crownlabs-q-first")
		Expect(err).ToNot(HaveOccurred())
		Expect(queue.GetOwnerReferences()).To(HaveLen(1))
		Expect(queue.GetOwnerReferences()[0].Name).To(Equal("q-first"))
		Expect(queue.GetManagedFields()).To(ContainElement(HaveField("Manager", forge.CrownJobFieldManager)))
		cohortName, _, _ := unstructured.NestedString(queue.Object, "spec", "cohortName")
		Expect(cohortName).To(Equal(forge.CrownJobRootCohort))
		cohort, err := kueueObject("Cohort", forge.CrownJobRootCohort)
		Expect(err).ToNot(HaveOccurred())
		Expect(cohort.GetOwnerReferences()).To(BeEmpty())
		Expect(cohort.GetLabels()).To(HaveKeyWithValue(forge.LabelManagedByKey, "crownjob"))
		flavor := &unstructured.Unstructured{}
		flavor.SetGroupVersionKind(forge.KueueResourceFlavorGVK)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: forge.CrownJobResourceFlavor}, flavor)).To(Succeed())
	})

	It("Should apply every decrease before any increase when a guarantee is raised and lowered", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("4")})
		a := createWorkspace("q-order-a", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("3"), Guaranteed: cpu("1")})
		createWorkspace("q-order-b", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("4")})
		reconcileQueues(r)
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=3/-"))
		Expect(cpuOf("ClusterQueue", "crownlabs-q-order-a")).To(Equal("cpu=1/+2"))

		By("Raising the guarantee: the pool and the borrowing limit go down first, then the nominal quota up")
		updateWorkspace(a, func(ws *clv1alpha1.Workspace) { ws.Spec.QueueLimits.Guaranteed = cpu("2") })
		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=2/-"))
		Expect(cpuOf("ClusterQueue", "crownlabs-q-order-a")).To(Equal("cpu=2/+1"))
		Expect(lastWriteIndex(recording.writes, "Cohort", "crownlabs-jobs")).To(BeNumerically("<",
			lastWriteIndex(recording.writes, "ClusterQueue", "crownlabs-q-order-a")), describeWrites(recording.writes))

		By("Lowering the guarantee: the nominal quota goes down first, then the pool and the borrowing limit up")
		updateWorkspace(a, func(ws *clv1alpha1.Workspace) { ws.Spec.QueueLimits.Guaranteed = nil })
		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=4/-"))
		Expect(cpuOf("ClusterQueue", "crownlabs-q-order-a")).To(Equal("cpu=0/+3"))
		Expect(firstWriteIndex(recording.writes, "ClusterQueue", "crownlabs-q-order-a")).To(BeNumerically("<",
			firstWriteIndex(recording.writes, "Cohort", "crownlabs-jobs")), describeWrites(recording.writes))

		By("Reconciling again with nothing changed: no write changes anything")
		recording.writes = nil
		reconcileQueues(r)
		for i := range recording.writes {
			up, down := changes(&recording.writes[i])
			Expect(up || down).To(BeFalse(), describeWrites(recording.writes))
		}
	})

	It("Should cut the guarantees that do not fit in the capacity, in workspace name order, with an event once per change (Q4)", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("3")})
		createWorkspace("q-cut-a", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2"), Guaranteed: cpu("2")})
		createWorkspace("q-cut-c", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("3"), Guaranteed: cpu("2")})
		reconcileQueues(r)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-cut-a")).To(Equal("cpu=2/+0"))
		Expect(cpuOf("ClusterQueue", "crownlabs-q-cut-c")).To(Equal("cpu=1/+2"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=0/-"))
		Expect(drain()).To(ConsistOf("Warning GuaranteeCut The job capacity (cpu=3) cannot hold every guarantee, " +
			"served in workspace name order: this workspace gets cpu 1 of the 2 it is guaranteed."))
		reconcileQueues(r)
		Expect(drain()).To(BeEmpty(), "the same cut is reported once")
	})

	It("Should guarantee the maximum when the guarantee is above it, with an event", func() {
		r := newReconciler(nil)
		createWorkspace("q-above", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2"), Guaranteed: cpu("3")})
		reconcileQueues(r)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-above")).To(Equal("cpu=2/+0"))
		Expect(drain()).To(ConsistOf("Warning GuaranteeAboveMaximum The guarantee of the jobs of this workspace is above their maximum " +
			"(cpu 3 (maximum 2)): the maximum is guaranteed instead."))
		reconcileQueues(r)
		Expect(drain()).To(BeEmpty())
	})

	It("Should ignore the windows of a Workspace with an invalid time zone, with an event", func() {
		r := newReconciler(nil)
		ws := workspace("q-badzone", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2")})
		ws.Spec.JobPolicy = &clv1alpha1.JobPolicy{TimeZone: "Europe/Atlantis"}
		ws.Spec.Windows = []clv1alpha1.JobWindow{{Days: []clv1alpha1.WeekDay{"Mon"}, Start: "00:00", End: "24:00",
			Overrides: clv1alpha1.QueueLimits{MaxAtOnce: cpu("5")}}}
		Expect(k8sClient.Create(ctx, ws)).To(Succeed())
		created = append(created, ws)
		Expect(reconcileQueues(r)).To(Equal(ctrl.Result{RequeueAfter: 10 * time.Minute}))
		Expect(cpuOf("ClusterQueue", "crownlabs-q-badzone")).To(Equal("cpu=2/+0"), "the defaults, not the window")
		Expect(drain()).To(ConsistOf("Warning InvalidTimeZone The time zone Europe/Atlantis of the job windows is not valid: " +
			"the windows are ignored, and the default limits apply."))
	})

	It("Should requeue at the next window boundary, then apply the window's values in two phases", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("4")})
		ws := workspace("q-window", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2")})
		ws.Spec.JobPolicy = &clv1alpha1.JobPolicy{TimeZone: "Europe/Rome"}
		ws.Spec.Windows = []clv1alpha1.JobWindow{{Days: []clv1alpha1.WeekDay{"Mon"}, Start: "20:00", End: "21:00",
			Overrides: clv1alpha1.QueueLimits{MaxAtOnce: cpu("3"), Guaranteed: cpu("2")}}}
		Expect(k8sClient.Create(ctx, ws)).To(Succeed())
		created = append(created, ws)

		testClock.SetTime(time.Date(2026, 11, 2, 19, 58, 30, 0, rome))
		Expect(reconcileQueues(r)).To(Equal(ctrl.Result{RequeueAfter: 91 * time.Second}), "90 s to 20:00, plus 1 s")
		Expect(cpuOf("ClusterQueue", "crownlabs-q-window")).To(Equal("cpu=0/+2"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=4/-"))

		By("At the start of the window: the pool down, then the guarantee up")
		testClock.SetTime(time.Date(2026, 11, 2, 20, 0, 1, 0, rome))
		recording.writes = nil
		Expect(reconcileQueues(r)).To(Equal(ctrl.Result{RequeueAfter: 10 * time.Minute}), "21:00 is 1 h away: at most 10 min")
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-window")).To(Equal("cpu=2/+1"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=2/-"))
		Expect(lastWriteIndex(recording.writes, "Cohort", "crownlabs-jobs")).To(BeNumerically("<",
			lastWriteIndex(recording.writes, "ClusterQueue", "crownlabs-q-window")), describeWrites(recording.writes))

		By("At the end of the window: the guarantee down, then the pool up")
		testClock.SetTime(time.Date(2026, 11, 2, 21, 0, 1, 0, rome))
		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-window")).To(Equal("cpu=0/+2"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=4/-"))
		Expect(firstWriteIndex(recording.writes, "ClusterQueue", "crownlabs-q-window")).To(BeNumerically("<",
			firstWriteIndex(recording.writes, "Cohort", "crownlabs-jobs")), describeWrites(recording.writes))
	})

	It("Should set the quotas to 0, then delete the ClusterQueue, when a Workspace loses queueLimits", func() {
		r := newReconciler(nil)
		ws := createWorkspace("q-gone", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2")})
		reconcileQueues(r)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-gone")).To(Equal("cpu=2/+0"), "outside the capacity: the maximum, no borrowing")

		updateWorkspace(ws, func(ws *clv1alpha1.Workspace) { ws.Spec.QueueLimits = nil })
		recording.writes = nil
		reconcileQueues(r)
		var ours []queueWrite
		for _, w := range recording.writes {
			if w.name == "crownlabs-q-gone" {
				ours = append(ours, w)
			}
		}
		Expect(ours).To(HaveLen(2), describeWrites(recording.writes))
		Expect(ours[0].op).To(Equal("application/merge-patch+json"))
		for _, q := range ours[0].after {
			Expect(q.Nominal.IsZero() && q.BorrowingLimit != nil && q.BorrowingLimit.IsZero()).To(BeTrue(), describeWrites(ours))
		}
		Expect(ours[1].op).To(Equal("delete"))
		_, err := kueueObject("ClusterQueue", "crownlabs-q-gone")
		Expect(kerrors.IsNotFound(err)).To(BeTrue())
	})

	It("Should zero, before the pool grows, a ClusterQueue already being deleted with its Workspace (Kueue keeps it while jobs run)", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("3")})
		ws := createWorkspace("q-deleted", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2"), Guaranteed: cpu("1")})
		reconcileQueues(r)
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=2/-"))
		queue, err := kueueObject("ClusterQueue", "crownlabs-q-deleted")
		Expect(err).ToNot(HaveOccurred())
		// The finalizer of Kueue, which keeps a ClusterQueue while workloads use it; then the garbage collector's delete.
		queue.SetFinalizers([]string{"kueue.x-k8s.io/resource-in-use"})
		Expect(k8sClient.Update(ctx, queue)).To(Succeed())
		Expect(k8sClient.Delete(ctx, ws)).To(Succeed())
		Expect(k8sClient.Delete(ctx, queue)).To(Succeed())

		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-deleted")).To(Equal("cpu=0/+0"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=3/-"))
		Expect(firstWriteIndex(recording.writes, "ClusterQueue", "crownlabs-q-deleted")).To(BeNumerically("<",
			firstWriteIndex(recording.writes, "Cohort", "crownlabs-jobs")), describeWrites(recording.writes))
		for _, w := range recording.writes {
			Expect(w.op).ToNot(Equal("delete"), "already being deleted: no second delete")
		}

		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "crownlabs-q-deleted"}, queue)).To(Succeed())
		queue.SetFinalizers(nil)
		Expect(k8sClient.Update(ctx, queue)).To(Succeed())
	})

	It("Should not delete a ClusterQueue with the same name that no Workspace controls", func() {
		r := newReconciler(nil)
		createWorkspace("q-foreign", nil)
		foreign := &unstructured.Unstructured{}
		foreign.SetGroupVersionKind(forge.KueueClusterQueueGVK)
		foreign.SetName("crownlabs-q-foreign")
		foreign.SetLabels(map[string]string{forge.LabelManagedByKey: "crownjob"})
		foreign.Object["spec"] = map[string]interface{}{}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		reconcileQueues(r)
		_, err := kueueObject("ClusterQueue", "crownlabs-q-foreign")
		Expect(err).ToNot(HaveOccurred())
		Expect(k8sClient.Delete(ctx, foreign)).To(Succeed())
	})

	It("Should ignore a Workspace without the target label", func() {
		r := newReconciler(nil)
		ws := workspace("q-untargeted", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("1")})
		ws.Labels = nil
		Expect(k8sClient.Create(ctx, ws)).To(Succeed())
		created = append(created, ws)
		reconcileQueues(r)
		_, err := kueueObject("ClusterQueue", "crownlabs-q-untargeted")
		Expect(kerrors.IsNotFound(err)).To(BeTrue())
	})

	It("Should migrate a ClusterQueue of PR #2 (no cohort, nominal = maxAtOnce) in two phases", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("4")})
		ws := createWorkspace("q-migrate", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2")})
		// The ClusterQueue as PR #2 forged it: nominal quota = maxAtOnce, no cohort, no borrowing limit.
		old := &unstructured.Unstructured{}
		old.SetGroupVersionKind(forge.KueueClusterQueueGVK)
		old.SetName("crownlabs-q-migrate")
		old.SetLabels(map[string]string{forge.LabelManagedByKey: "crownjob"})
		old.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(ws, clv1alpha1.GroupVersion.WithKind("Workspace"))})
		old.Object["spec"] = map[string]interface{}{
			"namespaceSelector": map[string]interface{}{"matchLabels": map[string]interface{}{"crownlabs.polito.it/type": "tenant"}},
			"queueingStrategy":  "BestEffortFIFO",
			"resourceGroups": []interface{}{map[string]interface{}{
				"coveredResources": []interface{}{"cpu", "memory"},
				"flavors": []interface{}{map[string]interface{}{"name": "crownlabs-default", "resources": []interface{}{
					map[string]interface{}{"name": "cpu", "nominalQuota": "2"},
					map[string]interface{}{"name": "memory", "nominalQuota": "100Ti"},
				}}},
			}},
		}
		Expect(k8sClient.Create(ctx, old)).To(Succeed())

		reconcileQueues(r)
		var ours []queueWrite
		for _, w := range recording.writes {
			if w.name == "crownlabs-q-migrate" {
				ours = append(ours, w)
			}
		}
		Expect(ours).To(HaveLen(2), describeWrites(recording.writes))
		// Phase 1: cpu (in the capacity) down to the guarantee (0), and no borrowing yet (it could not borrow before);
		// memory (outside the capacity) keeps its maximum as its own quota, and never borrows.
		Expect(forge.FormatKueueQuotas(ours[0].after)).To(HavePrefix("cpu=0/+0 memory=100Ti/+0"), describeWrites(ours))
		// Phase 2: borrowing cpu up to the maximum.
		Expect(forge.FormatKueueQuotas(ours[1].after)).To(HavePrefix("cpu=0/+2 memory=100Ti/+0"), describeWrites(ours))
		queue, err := kueueObject("ClusterQueue", "crownlabs-q-migrate")
		Expect(err).ToNot(HaveOccurred())
		cohortName, _, _ := unstructured.NestedString(queue.Object, "spec", "cohortName")
		Expect(cohortName).To(Equal(forge.CrownJobRootCohort))
		expectDecreasesFirst(recording.writes)
	})

	It("Should cover a new resource everywhere, and keep it covered with no quota once no Workspace names it", func() {
		r := newReconciler(nil)
		ws := createWorkspace("q-cover", &clv1alpha1.QueueLimits{MaxAtOnce: map[string]resource.Quantity{"example.com/new-dev": resource.MustParse("1")}})
		reconcileQueues(r)
		Expect(quotasOf("ClusterQueue", "crownlabs-q-cover")).To(ContainSubstring("example.com/new-dev=1/+0"))
		Expect(quotasOf("Cohort", forge.CrownJobRootCohort)).To(ContainSubstring("example.com/new-dev=10k/-"))
		Expect(quotasOf("ClusterQueue", "crownlabs-ws")).To(ContainSubstring("example.com/new-dev=0/+0"))

		updateWorkspace(ws, func(ws *clv1alpha1.Workspace) { ws.Spec.QueueLimits.MaxAtOnce = cpu("1") })
		reconcileQueues(r)
		Expect(quotasOf("ClusterQueue", "crownlabs-q-cover")).To(ContainSubstring("example.com/new-dev=0/+0"))
		Expect(quotasOf("Cohort", forge.CrownJobRootCohort)).To(ContainSubstring("example.com/new-dev=10k/-"))
		cohort, err := kueueObject("Cohort", forge.CrownJobRootCohort)
		Expect(err).ToNot(HaveOccurred())
		_, covered, err := forge.KueueQuotas(cohort)
		Expect(err).ToNot(HaveOccurred())
		Expect(covered).To(ContainElement(corev1.ResourceName("example.com/new-dev")))
	})

	It("Should restore a ClusterQueue and the Cohort changed by hand, and re-create a deleted Cohort", func() {
		r := newReconciler(corev1.ResourceList{"cpu": resource.MustParse("4")})
		createWorkspace("q-restore", &clv1alpha1.QueueLimits{MaxAtOnce: cpu("2"), Guaranteed: cpu("1")})
		reconcileQueues(r)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-restore")).To(Equal("cpu=1/+1"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=3/-"))

		for _, kind := range []string{"ClusterQueue", "Cohort"} {
			name := map[string]string{"ClusterQueue": "crownlabs-q-restore", "Cohort": forge.CrownJobRootCohort}[kind]
			obj, err := kueueObject(kind, name)
			Expect(err).ToNot(HaveOccurred())
			groups, _, _ := unstructured.NestedSlice(obj.Object, "spec", "resourceGroups")
			resources := groups[0].(map[string]interface{})["flavors"].([]interface{})[0].(map[string]interface{})["resources"].([]interface{})
			resources[0].(map[string]interface{})["nominalQuota"] = "7"
			Expect(unstructured.SetNestedSlice(obj.Object, groups, "spec", "resourceGroups")).To(Succeed())
			Expect(k8sClient.Update(ctx, obj)).To(Succeed())
		}
		Expect(cpuOf("ClusterQueue", "crownlabs-q-restore")).To(Equal("cpu=7/+1"))
		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("ClusterQueue", "crownlabs-q-restore")).To(Equal("cpu=1/+1"))
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=3/-"))

		cohort, err := kueueObject("Cohort", forge.CrownJobRootCohort)
		Expect(err).ToNot(HaveOccurred())
		Expect(k8sClient.Delete(ctx, cohort)).To(Succeed())
		recording.writes = nil
		reconcileQueues(r)
		expectDecreasesFirst(recording.writes)
		Expect(cpuOf("Cohort", forge.CrownJobRootCohort)).To(Equal("cpu=3/-"))
		Expect(recording.writes[0].kind).To(Equal("Cohort"))
		Expect(recording.writes[0].existed).To(BeFalse())
		Expect(forge.FormatKueueQuotas(recording.writes[0].after)).To(HavePrefix("cpu=0/- memory=0/-"), "re-created with a pool of 0 first")
	})

	It("Should keep the per-Workspace behavior of PR #2 without a capacity: each Workspace up to its maxAtOnce", func() {
		r := newReconciler(nil)
		createWorkspace("q-nocap", &clv1alpha1.QueueLimits{MaxAtOnce: map[string]resource.Quantity{
			"cpu": resource.MustParse("2"), "memory": resource.MustParse("6Gi"),
		}, Guaranteed: cpu("1")})
		reconcileQueues(r)
		Expect(quotasOf("ClusterQueue", "crownlabs-q-nocap")).To(HavePrefix("cpu=2/+0 memory=6Gi/+0"), "nominal = maxAtOnce, as PR #2")
		Expect(quotasOf("Cohort", forge.CrownJobRootCohort)).To(HavePrefix("cpu=10k/- memory=100Ti/-"))
		queue, err := kueueObject("ClusterQueue", "crownlabs-q-nocap")
		Expect(err).ToNot(HaveOccurred())
		preemption, _, _ := unstructured.NestedMap(queue.Object, "spec", "preemption")
		Expect(preemption).To(Equal(map[string]interface{}{"reclaimWithinCohort": "Never", "withinClusterQueue": "Never",
			"borrowWithinCohort": map[string]interface{}{"policy": "Never"}}))
		Expect(ptr.Deref(queue.GetOwnerReferences()[0].Controller, false)).To(BeTrue())
	})
})

// firstWriteIndex and lastWriteIndex return the index of the first and last write to the given object (-1 if none).
func firstWriteIndex(writes []queueWrite, kind, name string) int {
	for i := range writes {
		if writes[i].kind == kind && writes[i].name == name {
			if up, down := changes(&writes[i]); up || down {
				return i
			}
		}
	}
	return -1
}

func lastWriteIndex(writes []queueWrite, kind, name string) int {
	last := -1
	for i := range writes {
		if writes[i].kind == kind && writes[i].name == name {
			if up, down := changes(&writes[i]); up || down {
				last = i
			}
		}
	}
	return last
}
