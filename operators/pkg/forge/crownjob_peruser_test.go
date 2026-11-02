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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("The per-user limit of a job queue", func() {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		panic(err)
	}
	const gpu = "nvidia.com/gpu"
	const mon = clv1alpha1.WeekDayMonday
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
	perUser := func(kv ...string) *clv1alpha1.PerUserLimits {
		return &clv1alpha1.PerUserLimits{Resources: quantities(kv...)}
	}
	// spec returns the spec of a Workspace with the given default per-user limit, and the given windows.
	spec := func(defaults *clv1alpha1.PerUserLimits, windows ...clv1alpha1.JobWindow) *clv1alpha1.WorkspaceSpec {
		return &clv1alpha1.WorkspaceSpec{
			QueueLimits: &clv1alpha1.QueueLimits{MaxAtOnce: quantities("cpu", "8"), PerUser: defaults},
			Windows:     windows,
		}
	}
	// window returns a window on Monday from start to end, with the given per-user limit (nil: not overridden).
	window := func(start, end string, override *clv1alpha1.PerUserLimits) clv1alpha1.JobWindow {
		return clv1alpha1.JobWindow{Days: []clv1alpha1.WeekDay{mon}, Start: start, End: end,
			Overrides: clv1alpha1.QueueLimits{PerUser: override}}
	}
	at := func(value string) time.Time {
		t, err := time.ParseInLocation("2006-01-02 15:04", value, rome)
		ExpectWithOffset(1, err).ToNot(HaveOccurred())
		return t
	}
	// job returns a Job whose pod requests the given cpu in its container and initCPU in an init container.
	job := func(name, cpu, initCPU string, conditions ...batchv1.JobCondition) batchv1.Job {
		j := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: batchv1.JobStatus{Conditions: conditions}}
		j.Spec.Template.Spec.Containers = []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
			Requests: list("cpu", cpu, "memory", "1Gi")}}}
		if initCPU != "" {
			j.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{
				Requests: list("cpu", initCPU)}}}
		}
		return j
	}
	condition := func(conditionType batchv1.JobConditionType, status corev1.ConditionStatus) batchv1.JobCondition {
		return batchv1.JobCondition{Type: conditionType, Status: status}
	}

	Describe("The forge.CrownJobPerUserLimit function", func() {
		It("Should be the per-user limit of the resource, or nil if none", func() {
			limits := clv1alpha1.QueueLimits{PerUser: perUser("cpu", "1")}
			Expect(forge.CrownJobPerUserLimit(&limits, "cpu").String()).To(Equal("1"))
			Expect(forge.CrownJobPerUserLimit(&limits, "memory")).To(BeNil())
			Expect(forge.CrownJobPerUserLimit(&clv1alpha1.QueueLimits{}, "cpu")).To(BeNil())
			Expect(forge.CrownJobPerUserLimit(&clv1alpha1.QueueLimits{PerUser: &clv1alpha1.PerUserLimits{}}, "cpu")).To(BeNil())
		})
	})

	Describe("The forge.CrownJobPerUserUsage function", func() {
		It("Should sum the effective requests of the Jobs in the queue or running, and skip the others", func() {
			deleting := job("deleting", "4", "")
			deleting.DeletionTimestamp = &metav1.Time{Time: at("2026-11-02 12:00")}
			jobs := []batchv1.Job{
				job("waiting", "500m", ""),
				job("running", "500m", "", condition(batchv1.JobComplete, corev1.ConditionFalse)),
				job("init-larger", "100m", "700m"),
				job("complete", "4", "", condition(batchv1.JobComplete, corev1.ConditionTrue)),
				job("failed", "4", "", condition(batchv1.JobFailed, corev1.ConditionTrue)),
				deleting,
				job("self", "4", ""),
			}
			used := forge.CrownJobPerUserUsage(jobs, "self")
			Expect(used.Cpu().String()).To(Equal("1700m"), "500m + 500m + max(100m, 700m)")
			Expect(used.Memory().String()).To(Equal("3Gi"))
		})

		It("Should be empty with no Jobs", func() {
			Expect(forge.CrownJobPerUserUsage(nil, "")).To(BeEmpty())
		})
	})

	Describe("The forge.CrownJobCheckPerUserLimits function", func() {
		type checkCase struct {
			spec     *clv1alpha1.WorkspaceSpec
			loc      *time.Location
			requests corev1.ResourceList
			message  string
		}
		DescribeTable("Should refuse only a job above the largest per-user limit of the defaults and every window",
			func(c checkCase) {
				err := forge.CrownJobCheckPerUserLimits("ws", c.spec, c.loc, c.requests)
				if c.message == "" {
					Expect(err).ToNot(HaveOccurred())
					return
				}
				var cannotStart *forge.CannotStartError
				Expect(err).To(BeAssignableToTypeOf(cannotStart))
				Expect(err.Error()).To(Equal(c.message))
			},
			Entry("no per-user limit", checkCase{spec: spec(nil), loc: rome, requests: list("cpu", "4")}),
			Entry("within the default", checkCase{spec: spec(perUser("cpu", "1")), loc: rome, requests: list("cpu", "1")}),
			Entry("above the default, no window", checkCase{spec: spec(perUser("cpu", "1")), loc: rome, requests: list("cpu", "1500m"),
				message: "Your own limit in workspace ws is 1 of cpu: this job could never start."}),
			Entry("above the default, within a window's", checkCase{spec: spec(perUser("cpu", "1"), window("20:00", "24:00", perUser("cpu", "2"))),
				loc: rome, requests: list("cpu", "1500m")}),
			Entry("above every set: the largest is told", checkCase{spec: spec(perUser("cpu", "1"), window("20:00", "24:00", perUser("cpu", "2"))),
				loc: rome, requests: list("cpu", "2500m"), message: "Your own limit in workspace ws is 2 of cpu: this job could never start."}),
			Entry("a window that does not override perUser keeps the default", checkCase{spec: spec(perUser("cpu", "1"), window("20:00", "24:00", nil)),
				loc: rome, requests: list("cpu", "1500m"), message: "Your own limit in workspace ws is 1 of cpu: this job could never start."}),
			Entry("a window whose perUser does not name the resource: no limit then", checkCase{
				spec: spec(perUser("cpu", "1"), window("20:00", "24:00", perUser(gpu, "1"))), loc: rome, requests: list("cpu", "4")}),
			Entry("no time zone: the windows never apply", checkCase{spec: spec(perUser("cpu", "1"), window("20:00", "24:00", perUser("cpu", "2"))),
				loc: nil, requests: list("cpu", "1500m"), message: "Your own limit in workspace ws is 1 of cpu: this job could never start."}),
			Entry("a limited resource the job does not request", checkCase{spec: spec(perUser("cpu", "1", gpu, "0")), loc: rome,
				requests: list("cpu", "1", "memory", "1Gi")}),
			Entry("a zero request", checkCase{spec: spec(perUser(gpu, "0")), loc: rome, requests: list("cpu", "1", gpu, "0")}),
			Entry("a limit of 0", checkCase{spec: spec(perUser(gpu, "0")), loc: rome, requests: list(gpu, "1"),
				message: "Your own limit in workspace ws is 0 of nvidia.com/gpu: this job could never start."}),
			Entry("two resources above: the first by name", checkCase{spec: spec(perUser("cpu", "1", gpu, "1")), loc: rome,
				requests: list(gpu, "2", "cpu", "2"), message: "Your own limit in workspace ws is 1 of cpu: this job could never start."}),
		)
	})

	Describe("The forge.CrownJobPerUserFits function", func() {
		limits := &clv1alpha1.QueueLimits{PerUser: perUser("cpu", "1", "memory", "4Gi")}
		type fitsCase struct {
			limits                          *clv1alpha1.QueueLimits
			requests, used                  corev1.ResourceList
			resource, usedQ, requested, max string
		}
		DescribeTable("Should fit while what the member uses plus the job is within the limit, for each resource the job requests",
			func(c fitsCase) {
				result := forge.CrownJobPerUserFits(c.limits, c.requests, c.used)
				if c.resource == "" {
					Expect(result.Fits).To(BeTrue())
					return
				}
				Expect(result.Fits).To(BeFalse())
				Expect(result.Resource).To(Equal(corev1.ResourceName(c.resource)))
				Expect(result.Used.String()).To(Equal(c.usedQ))
				Expect(result.Requested.String()).To(Equal(c.requested))
				Expect(result.Limit.String()).To(Equal(c.max))
			},
			Entry("nothing used", fitsCase{limits: limits, requests: list("cpu", "500m", "memory", "1Gi"), used: list()}),
			Entry("exactly the limit", fitsCase{limits: limits, requests: list("cpu", "500m"), used: list("cpu", "500m")}),
			Entry("above the limit", fitsCase{limits: limits, requests: list("cpu", "500m"), used: list("cpu", "1"),
				resource: "cpu", usedQ: "1", requested: "500m", max: "1"}),
			Entry("above in a resource the job does not request", fitsCase{limits: limits, requests: list("cpu", "500m"),
				used: list("cpu", "500m", "memory", "8Gi")}),
			Entry("a zero request of a resource the member is above", fitsCase{limits: limits, requests: list("cpu", "500m", "memory", "0"),
				used: list("memory", "8Gi")}),
			Entry("a resource without a limit", fitsCase{limits: limits, requests: list("cpu", "500m", gpu, "4"), used: list(gpu, "8")}),
			Entry("no per-user limit", fitsCase{limits: &clv1alpha1.QueueLimits{}, requests: list("cpu", "8"), used: list("cpu", "8")}),
			Entry("two resources above: the first by name", fitsCase{limits: limits, requests: list("memory", "2Gi", "cpu", "1"),
				used: list("cpu", "500m", "memory", "3Gi"), resource: "cpu", usedQ: "500m", requested: "1", max: "1"}),
		)

		It("Should tell whether the job alone fits (forge.CrownJobPerUserAllows)", func() {
			Expect(forge.CrownJobPerUserAllows(limits, list("cpu", "1"))).To(BeTrue())
			Expect(forge.CrownJobPerUserAllows(limits, list("cpu", "1001m"))).To(BeFalse())
		})
	})

	Describe("The forge.CrownJobPerUserNextAllowing function", func() {
		It("Should find the start of the window that allows the job alone", func() {
			s := spec(perUser("cpu", "1"), window("20:00", "24:00", perUser("cpu", "2")))
			next, inWindow, found := forge.CrownJobPerUserNextAllowing(s, rome, at("2026-11-02 12:00"), list("cpu", "1500m"))
			Expect(found).To(BeTrue())
			Expect(next).To(BeTemporally("==", at("2026-11-02 20:00")))
			Expect(inWindow).To(BeTrue())
		})

		It("Should find the end of a window that lowers the limit, with the defaults in force", func() {
			s := spec(perUser("cpu", "2"), window("20:00", "24:00", perUser("cpu", "1")))
			next, inWindow, found := forge.CrownJobPerUserNextAllowing(s, rome, at("2026-11-02 21:00"), list("cpu", "1500m"))
			Expect(found).To(BeTrue())
			Expect(next).To(BeTemporally("==", at("2026-11-03 00:00")))
			Expect(inWindow).To(BeFalse())
		})

		It("Should find nothing when no set of limits allows every resource at once", func() {
			// cpu fits only in the defaults, the GPU only in the window: each resource alone is allowed somewhere.
			s := spec(perUser("cpu", "2", gpu, "0"), window("20:00", "24:00", perUser("cpu", "1", gpu, "1")))
			Expect(forge.CrownJobCheckPerUserLimits("ws", s, rome, list("cpu", "1500m", gpu, "1"))).To(Succeed())
			_, _, found := forge.CrownJobPerUserNextAllowing(s, rome, at("2026-11-02 12:00"), list("cpu", "1500m", gpu, "1"))
			Expect(found).To(BeFalse())
		})

		It("Should find nothing without a time zone", func() {
			s := spec(perUser("cpu", "1"), window("20:00", "24:00", perUser("cpu", "2")))
			_, _, found := forge.CrownJobPerUserNextAllowing(s, nil, at("2026-11-02 12:00"), list("cpu", "1500m"))
			Expect(found).To(BeFalse())
		})
	})

	Describe("The order of the held jobs of a member", func() {
		crownJob := func(name, created string) clv1alpha2.CrownJob {
			return clv1alpha2.CrownJob{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.Time{Time: at(created)}}}
		}

		It("Should release the oldest first, then by name (forge.CrownJobSortHeld)", func() {
			held := []clv1alpha2.CrownJob{
				crownJob("d", "2026-11-02 12:03"), crownJob("c", "2026-11-02 12:01"),
				crownJob("b", "2026-11-02 12:02"), crownJob("a", "2026-11-02 12:02"),
			}
			forge.CrownJobSortHeld(held)
			names := []string{}
			for i := range held {
				names = append(names, held[i].Name)
			}
			Expect(names).To(Equal([]string{"c", "a", "b", "d"}))
		})

		It("Should compare two jobs (forge.CrownJobHeldBefore)", func() {
			older, younger := crownJob("z", "2026-11-02 12:00"), crownJob("a", "2026-11-02 12:01")
			Expect(forge.CrownJobHeldBefore(&older, &younger)).To(BeTrue())
			Expect(forge.CrownJobHeldBefore(&younger, &older)).To(BeFalse())
			same1, same2 := crownJob("a", "2026-11-02 12:00"), crownJob("b", "2026-11-02 12:00")
			Expect(forge.CrownJobHeldBefore(&same1, &same2)).To(BeTrue())
			Expect(forge.CrownJobHeldBefore(&same2, &same1)).To(BeFalse())
			Expect(forge.CrownJobHeldBefore(&same1, &same1)).To(BeFalse())
		})
	})
})
