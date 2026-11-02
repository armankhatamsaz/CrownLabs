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

package forge

import (
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
)

// The per-user limit (perUser.resources) is held by the operator, outside Kueue: the Job of a CrownJob is created only
// while the jobs of its member (the tenant namespace) in the queue of the Workspace, waiting or running, plus this one,
// fit in the limit in force. Kueue cannot do it with one queue per Workspace: it would admit two waiting jobs of the
// same member together. A job that does not fit is held, with no Job, and the jobs of a member are released oldest
// first; a released job enters the queue of the Workspace at the back, since its Job is created then.

// CrownJobPerUserLimit returns the per-user limit of the given resource in the given limits, or nil if there is none.
func CrownJobPerUserLimit(limits *clv1alpha1.QueueLimits, name corev1.ResourceName) *resource.Quantity {
	if limits.PerUser == nil {
		return nil
	}
	limit, found := limits.PerUser.Resources[name.String()]
	if !found {
		return nil
	}
	return &limit
}

// CrownJobPerUserUsage returns what a member uses in the queue of a Workspace, by resource: the sum of the effective
// requests of the given Jobs (those of the member with the queue of the Workspace), except those that are finished or
// being deleted, and the one named exclude (the Job of the job being checked, created by an earlier attempt).
func CrownJobPerUserUsage(jobs []batchv1.Job, exclude string) corev1.ResourceList {
	used := corev1.ResourceList{}
	for i := range jobs {
		job := &jobs[i]
		if job.Name == exclude || !job.DeletionTimestamp.IsZero() || jobFinished(job) {
			continue
		}
		used = addResourceList(used, PodEffectiveRequests(&job.Spec.Template.Spec))
	}
	return used
}

// jobFinished returns whether the given Job is complete or failed.
func jobFinished(job *batchv1.Job) bool {
	for i := range job.Status.Conditions {
		condition := &job.Status.Conditions[i]
		if (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// CrownJobCheckPerUserLimits returns a CannotStartError if the job could never start because of the per-user limit: it
// requests more of a resource than the largest per-user limit of that resource, over the defaults and every job window
// (none when loc is nil, as the windows then never apply). A resource with no per-user limit in some set of limits is
// not limited. Only the resources the job requests are checked.
func CrownJobCheckPerUserLimits(workspace string, spec *clv1alpha1.WorkspaceSpec, loc *time.Location, requests corev1.ResourceList) error {
	all := JobLimitsAll(spec, loc)
	for _, name := range sortedResourceNames(requests) {
		requested := requests[name]
		if requested.Sign() <= 0 {
			continue
		}
		var largest *resource.Quantity
		unlimited := false
		for i := range all {
			limit := CrownJobPerUserLimit(&all[i], name)
			if limit == nil {
				unlimited = true
				break
			}
			if largest == nil || limit.Cmp(*largest) > 0 {
				largest = limit
			}
		}
		if !unlimited && requested.Cmp(*largest) > 0 {
			return CrownJobPerUserCannotStart(workspace, *largest, name)
		}
	}
	return nil
}

// CrownJobPerUserCannotStart returns the CannotStartError of a job that the given per-user limit of a resource would
// never allow.
func CrownJobPerUserCannotStart(workspace string, limit resource.Quantity, name corev1.ResourceName) error {
	return NewCannotStartError("Your own limit in workspace %s is %s of %s: this job could never start.", workspace, limit.String(), name)
}

// CrownJobPerUserResult tells whether a job fits in the per-user limit, and, if not, the first resource by name that
// does not fit, with what the member uses, what the job requests and the limit.
type CrownJobPerUserResult struct {
	Fits      bool
	Resource  corev1.ResourceName
	Used      resource.Quantity
	Requested resource.Quantity
	Limit     resource.Quantity
}

// CrownJobPerUserFits returns whether a job fits in the per-user limit of the given limits, when its member already
// uses the given resources: for every resource that the job requests and that the limit names, what the member uses
// plus the request must not exceed the limit. A resource the job does not request is not checked.
func CrownJobPerUserFits(limits *clv1alpha1.QueueLimits, requests, used corev1.ResourceList) CrownJobPerUserResult {
	for _, name := range sortedResourceNames(requests) {
		requested := requests[name]
		limit := CrownJobPerUserLimit(limits, name)
		if requested.Sign() <= 0 || limit == nil {
			continue
		}
		current := used[name]
		if after := sum(current, requested); after.Cmp(*limit) > 0 {
			return CrownJobPerUserResult{Resource: name, Used: current.DeepCopy(), Requested: requested.DeepCopy(), Limit: limit.DeepCopy()}
		}
	}
	return CrownJobPerUserResult{Fits: true}
}

// CrownJobPerUserAllows returns whether the per-user limit of the given limits allows the job alone, when its member
// uses nothing else.
func CrownJobPerUserAllows(limits *clv1alpha1.QueueLimits, requests corev1.ResourceList) bool {
	return CrownJobPerUserFits(limits, requests, corev1.ResourceList{}).Fits
}

// CrownJobPerUserNextAllowing returns the first instant strictly after now at which the per-user limit in force allows
// the job alone, and whether a job window is in force then (false: the defaults are, at the end of a window). It
// returns false as last value if there is none within JobWindowHorizon.
func CrownJobPerUserNextAllowing(spec *clv1alpha1.WorkspaceSpec, loc *time.Location, now time.Time,
	requests corev1.ResourceList) (at time.Time, inWindow, found bool) {
	at, found = JobNextTimeAllowing(spec, loc, now, func(limits *clv1alpha1.QueueLimits) bool {
		return CrownJobPerUserAllows(limits, requests)
	})
	if !found {
		return time.Time{}, false, false
	}
	for i := range spec.Windows {
		if JobWindowCovers(&spec.Windows[i], loc, at) {
			return at, true, true
		}
	}
	return at, false, true
}

// CrownJobHeldBefore returns whether the held CrownJob a is released before the held CrownJob b of the same member:
// the one created first, then by name.
func CrownJobHeldBefore(a, b *clv1alpha2.CrownJob) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// CrownJobSortHeld sorts the held CrownJobs of a member in the order in which they are released.
func CrownJobSortHeld(crownJobs []clv1alpha2.CrownJob) {
	sort.SliceStable(crownJobs, func(i, j int) bool { return CrownJobHeldBefore(&crownJobs[i], &crownJobs[j]) })
}

// sortedResourceNames returns the names of the given resources, sorted.
func sortedResourceNames(list corev1.ResourceList) []corev1.ResourceName {
	names := make([]string, 0, len(list))
	for name := range list {
		names = append(names, name.String())
	}
	sort.Strings(names)
	sorted := make([]corev1.ResourceName, 0, len(names))
	for _, name := range names {
		sorted = append(sorted, corev1.ResourceName(name))
	}
	return sorted
}
