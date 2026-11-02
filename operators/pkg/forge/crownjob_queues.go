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
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
)

// The job queues of the Workspaces share one pool, the root Cohort of Kueue. For each resource in the job capacity:
//   - the Cohort's nominal quota is the shared pool: the job capacity, minus the guarantees in force;
//   - the ClusterQueue of each Workspace has the guarantee in force as nominal quota, and its maximum minus that
//     guarantee as borrowing limit (no limit when the maximum is unlimited). Its lending limit is unset: an idle
//     guarantee is lent to the other Workspaces, and comes back as their jobs end (no job is ever stopped).
//
// A resource outside the job capacity is not shared: the nominal quota of each ClusterQueue is its maximum, and it
// never borrows it. Kueue serves first the jobs that borrow nothing, in any resource they request: a resource that
// every job borrowed (e.g., memory, which every pod requests) would make every job a borrower, and the guarantees
// would no longer come first.
//
// Without a job capacity (the default), no resource is shared, so every Workspace can use up to its maxAtOnce and
// the guarantees have no effect: the same behavior as job queues without a Cohort.

const (
	// ReasonGuaranteeAboveMaximum -> the reason of the event recorded on a Workspace whose guarantee in force is above
	// its maximum: the maximum is used as the guarantee.
	ReasonGuaranteeAboveMaximum = "GuaranteeAboveMaximum"
	// ReasonGuaranteeCut -> the reason of the event recorded on a Workspace whose guarantee in force does not fit in
	// the job capacity: it gets what the Workspaces before it, in name order, left.
	ReasonGuaranteeCut = "GuaranteeCut"
)

var (
	// KueueNoLimitOther -> the quota that no job reaches, for a resource other than cpu and memory (see KueueNoLimitCPU).
	KueueNoLimitOther = resource.MustParse("10000")
)

// KueueResourceQuota is the quota of one resource in a ClusterQueue or in the root Cohort of the job queues.
type KueueResourceQuota struct {
	Name    corev1.ResourceName
	Nominal resource.Quantity
	// BorrowingLimit is nil when unset: no limit (the root Cohort never has one).
	BorrowingLimit *resource.Quantity
}

// KueueWorkspaceLimits are the limits in force of the job queue of a Workspace.
type KueueWorkspaceLimits struct {
	Name   string
	Limits clv1alpha1.QueueLimits
}

// KueueQueueEvent is an event to record on a Workspace, about the quotas of its job queue.
type KueueQueueEvent struct {
	Workspace string
	Reason    string
	Message   string
}

// KueueQueuePlan is the desired state of the job queues: the resources they cover, the quotas of the root Cohort (the
// shared pool) and those of the ClusterQueue of each Workspace, and the events to record.
type KueueQueuePlan struct {
	Covered []corev1.ResourceName
	Pool    []KueueResourceQuota
	Queues  map[string][]KueueResourceQuota
	Events  []KueueQueueEvent
}

// KueueNoLimit returns the quota that no job reaches, for the given resource.
func KueueNoLimit(name corev1.ResourceName) resource.Quantity {
	switch name {
	case corev1.ResourceCPU:
		return KueueNoLimitCPU.DeepCopy()
	case corev1.ResourceMemory:
		return KueueNoLimitMemory.DeepCopy()
	default:
		return KueueNoLimitOther.DeepCopy()
	}
}

// KueueMaximum returns the most the jobs of a Workspace may use of a resource at the same time, with the given limits:
// maxAtOnce of the resource; if absent, no limit (nil) for cpu and memory, and 0 (not allowed) for any other resource.
func KueueMaximum(limits *clv1alpha1.QueueLimits, name corev1.ResourceName) *resource.Quantity {
	if quantity, found := limits.MaxAtOnce[name.String()]; found {
		return ptrQuantity(quantity)
	}
	if name == corev1.ResourceCPU || name == corev1.ResourceMemory {
		return nil
	}
	return ptrQuantity(resource.Quantity{})
}

// KueueCoveredResources returns the resources covered by the root Cohort and by every job queue: cpu and memory, then,
// sorted, every resource named in the job capacity, in the given Workspace specs (maxAtOnce or guaranteed, of the
// defaults or of a window), or in previous (those covered so far). They never shrink by themselves: a resource that
// is no longer named stays covered, with no quota, so that a resource in use is never removed.
func KueueCoveredResources(previous []corev1.ResourceName, capacity corev1.ResourceList, specs []*clv1alpha1.WorkspaceSpec) []corev1.ResourceName {
	others := map[corev1.ResourceName]struct{}{}
	add := func(name corev1.ResourceName) {
		if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
			others[name] = struct{}{}
		}
	}
	for _, name := range previous {
		add(name)
	}
	for name := range capacity {
		add(name)
	}
	for _, spec := range specs {
		for _, limits := range JobLimitsAll(spec, timeZoneAny) {
			for name := range limits.MaxAtOnce {
				add(corev1.ResourceName(name))
			}
			for name := range limits.Guaranteed {
				add(corev1.ResourceName(name))
			}
		}
	}

	names := make([]string, 0, len(others))
	for name := range others {
		names = append(names, name.String())
	}
	sort.Strings(names)
	covered := []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}
	for _, name := range names {
		covered = append(covered, corev1.ResourceName(name))
	}
	return covered
}

// KueueQueuePlanFor returns the desired quotas of the root Cohort and of the job queues of the given Workspaces, with
// their limits in force, for the given job capacity (empty: unlimited) and covered resources. For each resource:
//   - the guarantee is guaranteed (0 if absent), or the maximum if it is above it (event GuaranteeAboveMaximum);
//   - for a resource in the capacity, the guarantees are served in Workspace-name order: one that does not fit gets
//     what is left (event GuaranteeCut), and the pool is the capacity minus the guarantees; the nominal quota of a
//     job queue is its guarantee, and its borrowing limit is its maximum minus its guarantee (unset when the maximum
//     is unlimited);
//   - for any other resource, the pool is unlimited, and the nominal quota of a job queue is its maximum (unlimited
//     if it has none), with a borrowing limit of 0.
func KueueQueuePlanFor(workspaces []KueueWorkspaceLimits, capacity corev1.ResourceList, covered []corev1.ResourceName) KueueQueuePlan {
	sorted := append([]KueueWorkspaceLimits(nil), workspaces...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	plan := KueueQueuePlan{Covered: append([]corev1.ResourceName(nil), covered...), Queues: map[string][]KueueResourceQuota{}}
	aboveMaximum := map[string][]string{}
	cut := map[string][]string{}
	for _, name := range covered {
		total, limited := capacity[name]
		left := total.DeepCopy()
		for i := range sorted {
			limits := &sorted[i].Limits
			maximum := KueueMaximum(limits, name)
			guarantee := limits.Guaranteed[name.String()]
			if maximum != nil && guarantee.Cmp(*maximum) > 0 {
				aboveMaximum[sorted[i].Name] = append(aboveMaximum[sorted[i].Name],
					fmt.Sprintf("%s %s (maximum %s)", name, guarantee.String(), maximum.String()))
				guarantee = maximum.DeepCopy()
			}
			nominal := guarantee.DeepCopy()
			if limited {
				if nominal.Cmp(left) > 0 {
					nominal = left.DeepCopy()
					cut[sorted[i].Name] = append(cut[sorted[i].Name],
						fmt.Sprintf("%s %s of the %s it is guaranteed", name, nominal.String(), guarantee.String()))
				}
				left.Sub(nominal)
			}
			quota := KueueResourceQuota{Name: name, Nominal: nominal}
			switch {
			case !limited:
				quota.Nominal = KueueNoLimit(name)
				if maximum != nil {
					quota.Nominal = maximum.DeepCopy()
				}
				quota.BorrowingLimit = ptrQuantity(resource.Quantity{})
			case maximum != nil:
				borrowing := maximum.DeepCopy()
				borrowing.Sub(nominal)
				quota.BorrowingLimit = &borrowing
			}
			plan.Queues[sorted[i].Name] = append(plan.Queues[sorted[i].Name], quota)
		}
		pool := KueueNoLimit(name)
		if limited {
			pool = left
		}
		plan.Pool = append(plan.Pool, KueueResourceQuota{Name: name, Nominal: pool})
	}

	for i := range sorted {
		ws := sorted[i].Name
		if len(aboveMaximum[ws]) > 0 {
			plan.Events = append(plan.Events, KueueQueueEvent{Workspace: ws, Reason: ReasonGuaranteeAboveMaximum,
				Message: fmt.Sprintf("The guarantee of the jobs of this workspace is above their maximum (%s): the maximum is guaranteed instead.",
					strings.Join(aboveMaximum[ws], ", "))})
		}
		if len(cut[ws]) > 0 {
			plan.Events = append(plan.Events, KueueQueueEvent{Workspace: ws, Reason: ReasonGuaranteeCut,
				Message: fmt.Sprintf("The job capacity (%s) cannot hold every guarantee, served in workspace name order: this workspace gets %s.",
					formatResourceList(capacity), strings.Join(cut[ws], ", "))})
		}
	}
	return plan
}

// CrownJobWaiting tells why a job waits in the queue of its Workspace. The values are in the order in which the
// rules are checked: the first one that any requested resource gives is the reason of the job.
type CrownJobWaiting int

const (
	// CrownJobWaitingQuotaReached -> the Workspace uses so much that the job would take it above its maximum.
	CrownJobWaitingQuotaReached CrownJobWaiting = iota
	// CrownJobWaitingGuaranteeInUse -> the job fits in the guarantee of the Workspace, which other Workspaces are using.
	CrownJobWaitingGuaranteeInUse
	// CrownJobWaitingCapacityReached -> nothing is free in the shared pool.
	CrownJobWaitingCapacityReached
	// CrownJobWaitingLargerThanFree -> something is free, but less than the job needs.
	CrownJobWaitingLargerThanFree
	// CrownJobWaitingInQueue -> the job fits: it waits for its turn.
	CrownJobWaitingInQueue
)

// KueueQueueState is what the waiting reason of a job is computed from: the quotas of the ClusterQueue of its
// Workspace (nominal n, borrowing limit b), what that ClusterQueue uses (u, its reservation), and what is free in the
// root Cohort (the pool, plus the nominal quotas of its ClusterQueues, minus what they use).
type KueueQueueState struct {
	Quotas []KueueResourceQuota
	Used   corev1.ResourceList
	Free   corev1.ResourceList
}

// CrownJobWaitingResult is the waiting reason of a job, with the resource that gave it and the numbers to show.
type CrownJobWaitingResult struct {
	Reason    CrownJobWaiting
	Resource  corev1.ResourceName
	Requested resource.Quantity
	Free      resource.Quantity
	// AboveMaximum tells, for CrownJobWaitingQuotaReached, that the job alone is above the maximum in force (q > n + b):
	// it can start only when the limits change (e.g., in a job window).
	AboveMaximum bool
}

// KueueCohortFree returns what is free in the root Cohort, by resource: its pool, plus the nominal quotas of its
// ClusterQueues, minus what they use. It can be negative, when the quotas were lowered below what running jobs use.
func KueueCohortFree(pool []KueueResourceQuota, members []KueueQueueState) corev1.ResourceList {
	free := corev1.ResourceList{}
	for i := range pool {
		free[pool[i].Name] = pool[i].Nominal.DeepCopy()
	}
	for i := range members {
		for j := range members[i].Quotas {
			sum := free[members[i].Quotas[j].Name]
			sum.Add(members[i].Quotas[j].Nominal)
			free[members[i].Quotas[j].Name] = sum
		}
		for name, used := range members[i].Used {
			sum := free[name]
			sum.Sub(used)
			free[name] = sum
		}
	}
	return free
}

// CrownJobWaitingReason returns why a job with the given requests waits in the queue of its Workspace, from the state
// of the queue alone (Kueue gives one message for several causes, and no condition at all to identical waiting jobs).
// For each requested resource (q > 0), with u, n, b and free as in KueueQueueState (b unset: no limit):
//  1. u + q > n + b -> QuotaReached;
//  2. else, if free < q and u + q <= n -> GuaranteeInUse;
//  3. else, if free < q and free <= 0 -> CapacityReached;
//  4. else, if free < q -> LargerThanFree;
//  5. else -> InQueue.
//
// The reason of the job is the first of 1-5 that any resource gives (the first resource by name, among equals).
func CrownJobWaitingReason(requests corev1.ResourceList, state *KueueQueueState) CrownJobWaitingResult {
	quotas := map[corev1.ResourceName]KueueResourceQuota{}
	for i := range state.Quotas {
		quotas[state.Quotas[i].Name] = state.Quotas[i]
	}
	names := make([]string, 0, len(requests))
	for name := range requests {
		names = append(names, name.String())
	}
	sort.Strings(names)

	result := CrownJobWaitingResult{Reason: CrownJobWaitingInQueue}
	for _, name := range names {
		resourceName := corev1.ResourceName(name)
		requested := requests[resourceName]
		if requested.Sign() <= 0 {
			continue
		}
		quota, covered := quotas[resourceName]
		if !covered {
			quota = KueueResourceQuota{Name: resourceName, BorrowingLimit: ptrQuantity(resource.Quantity{})}
		}
		used, free := state.Used[resourceName], state.Free[resourceName]
		after := used.DeepCopy()
		after.Add(requested)

		current := CrownJobWaitingResult{Reason: CrownJobWaitingInQueue, Resource: resourceName, Requested: requested.DeepCopy(), Free: free.DeepCopy()}
		switch {
		case quota.BorrowingLimit != nil && after.Cmp(sum(quota.Nominal, *quota.BorrowingLimit)) > 0:
			current.Reason = CrownJobWaitingQuotaReached
			current.AboveMaximum = requested.Cmp(sum(quota.Nominal, *quota.BorrowingLimit)) > 0
		case free.Cmp(requested) < 0 && after.Cmp(quota.Nominal) <= 0:
			current.Reason = CrownJobWaitingGuaranteeInUse
		case free.Cmp(requested) < 0 && free.Sign() <= 0:
			current.Reason = CrownJobWaitingCapacityReached
		case free.Cmp(requested) < 0:
			current.Reason = CrownJobWaitingLargerThanFree
		}
		if current.Reason < result.Reason {
			result = current
		}
	}
	return result
}

// sum returns the sum of two quantities.
func sum(a, b resource.Quantity) resource.Quantity {
	total := a.DeepCopy()
	total.Add(b)
	return total
}

// KueuePhase1 returns the quotas to apply first when the current quotas of a ClusterQueue or of the root Cohort change
// to the desired ones: for each desired resource, the smallest of the current and desired nominal quotas, and the same
// for the borrowing limits, where an unset one counts as no limit. A resource with no current quota counts as 0 for
// both. Applying them never raises what any job queue can use, so no job is admitted between the two phases.
func KueuePhase1(current, desired []KueueResourceQuota) []KueueResourceQuota {
	byName := map[corev1.ResourceName]KueueResourceQuota{}
	for i := range current {
		byName[current[i].Name] = current[i]
	}
	phase1 := make([]KueueResourceQuota, 0, len(desired))
	for i := range desired {
		now, found := byName[desired[i].Name]
		if !found {
			now = KueueResourceQuota{Name: desired[i].Name, BorrowingLimit: ptrQuantity(resource.Quantity{})}
		}
		quota := KueueResourceQuota{Name: desired[i].Name, Nominal: minQuantity(now.Nominal, desired[i].Nominal)}
		switch {
		case now.BorrowingLimit == nil && desired[i].BorrowingLimit == nil:
		case now.BorrowingLimit == nil:
			quota.BorrowingLimit = ptrQuantity(*desired[i].BorrowingLimit)
		case desired[i].BorrowingLimit == nil:
			quota.BorrowingLimit = ptrQuantity(*now.BorrowingLimit)
		default:
			quota.BorrowingLimit = ptrQuantity(minQuantity(*now.BorrowingLimit, *desired[i].BorrowingLimit))
		}
		phase1 = append(phase1, quota)
	}
	return phase1
}

// KueueQuotasEqual returns whether two lists of quotas are the same, in the same order.
func KueueQuotasEqual(a, b []KueueResourceQuota) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Nominal.Cmp(b[i].Nominal) != 0 || (a[i].BorrowingLimit == nil) != (b[i].BorrowingLimit == nil) ||
			(a[i].BorrowingLimit != nil && a[i].BorrowingLimit.Cmp(*b[i].BorrowingLimit) != 0) {
			return false
		}
	}
	return true
}

// FormatKueueQuotas returns the given quotas as text, for the logs (e.g., "cpu=1/+2 memory=0/-": nominal/borrowing
// limit, "-" when unset).
func FormatKueueQuotas(quotas []KueueResourceQuota) string {
	parts := make([]string, 0, len(quotas))
	for i := range quotas {
		borrowing := "-"
		if quotas[i].BorrowingLimit != nil {
			borrowing = "+" + quotas[i].BorrowingLimit.String()
		}
		parts = append(parts, fmt.Sprintf("%s=%s/%s", quotas[i].Name, quotas[i].Nominal.String(), borrowing))
	}
	return strings.Join(parts, " ")
}

// timeZoneAny is any valid time zone: the resources named by the windows do not depend on when they apply.
var timeZoneAny = time.UTC

// minQuantity returns a copy of the smallest of the two quantities.
func minQuantity(a, b resource.Quantity) resource.Quantity {
	if a.Cmp(b) <= 0 {
		return a.DeepCopy()
	}
	return b.DeepCopy()
}

// ptrQuantity returns a pointer to a copy of the given quantity.
func ptrQuantity(quantity resource.Quantity) *resource.Quantity {
	copied := quantity.DeepCopy()
	return &copied
}

// formatResourceList returns a resource list as text, sorted by name (e.g., "cpu=4, nvidia.com/gpu=2").
func formatResourceList(list corev1.ResourceList) string {
	names := make([]string, 0, len(list))
	for name := range list {
		names = append(names, name.String())
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		quantity := list[corev1.ResourceName(name)]
		parts = append(parts, fmt.Sprintf("%s=%s", name, quantity.String()))
	}
	return strings.Join(parts, ", ")
}
