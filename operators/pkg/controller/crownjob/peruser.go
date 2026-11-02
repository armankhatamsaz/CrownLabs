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

package crownjob

import (
	"context"
	"fmt"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// keyedMutex holds one mutex per key.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// lock locks the mutex of the given key, and returns the function that unlocks it.
func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*sync.Mutex{}
	}
	m, found := k.locks[key]
	if !found {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	k.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// memberLocks serializes, for each member of each Workspace (a tenant namespace), the count of its jobs and the
// creation of a Job: two CrownJobs of the same member reconciled at the same time must not both take the same free
// part of the limit. One operator runs the CrownJob controller at a time (leader election).
var memberLocks keyedMutex

// perUserHold is the status of a job that the per-user limit holds, with no Job.
type perUserHold struct {
	reason, message string
	requeue         time.Duration
}

// checkPerUser checks the per-user limit in force of the Workspace for the given job, which requests the given resources;
// the caller holds the lock of the member. It returns how the job is held, or nil if its Job can be created: the
// member's jobs in the queue of the Workspace, waiting or running, plus this one, fit in the limit, and no older job of
// the member in the Workspace is held. A job that the limit in force never allows alone waits for the job window that
// allows it, if any (a forge.CannotStartError otherwise).
func (r *Reconciler) checkPerUser(ctx context.Context, crownJob *clv1alpha2.CrownJob, inputs *jobInputs,
	requests corev1.ResourceList) (*perUserHold, error) {
	workspace := inputs.workspace
	now := r.Clock.Now()
	limits := forge.JobLimitsAt(&workspace.Spec, inputs.timeZone, now)

	if !forge.CrownJobPerUserAllows(&limits, requests) {
		alone := forge.CrownJobPerUserFits(&limits, requests, nil)
		at, inWindow, found := forge.CrownJobPerUserNextAllowing(&workspace.Spec, inputs.timeZone, now, requests)
		if !found {
			return nil, forge.CrownJobPerUserCannotStart(workspace.Name, alone.Limit, alone.Resource)
		}
		message := msgWindowPerUser
		if !inWindow {
			message = msgWindowPerUserEnd
		}
		return &perUserHold{reason: ReasonWaitingForWindow,
			message: fmt.Sprintf(message, alone.Requested.String(), alone.Resource, workspace.Name, forge.FormatJobWindowTime(at, inputs.timeZone)),
			requeue: max(at.Sub(now), 0) + queueRequeueMargin}, nil
	}

	requeue := requeueHeld
	if boundary, found := forge.JobNextBoundary(&workspace.Spec, inputs.timeZone, now); found {
		requeue = min(requeue, max(boundary.Sub(now), 0)+queueRequeueMargin)
	}

	if limits.PerUser != nil && len(limits.PerUser.Resources) > 0 {
		// The Jobs of the member in the queue of the Workspace, read without cache: the Job created by the last holder
		// of the lock must be counted.
		var jobs batchv1.JobList
		if err := r.APIReader.List(ctx, &jobs, client.InNamespace(crownJob.Namespace),
			client.MatchingLabels{forge.KueueQueueNameLabel: forge.CrownJobQueueName(workspace.Name)}); err != nil {
			return nil, fmt.Errorf("failed listing the jobs of the member: %w", err)
		}
		used := forge.CrownJobPerUserUsage(jobs.Items, crownJob.Name)
		if fit := forge.CrownJobPerUserFits(&limits, requests, used); !fit.Fits {
			return &perUserHold{reason: ReasonUserLimitReached, requeue: requeue,
				message: fmt.Sprintf(msgUserLimitReached, workspace.Name, fit.Used.String(), fit.Resource)}, nil
		}
	}

	older, err := r.olderHeldJob(ctx, crownJob, workspace.Name)
	if err != nil {
		return nil, err
	}
	if older != "" {
		return &perUserHold{reason: ReasonUserLimitReached, requeue: requeue, message: fmt.Sprintf(msgUserLimitOlder, older, workspace.Name)}, nil
	}
	return nil, nil
}

// olderHeldJob returns the name of the oldest CrownJob of the same member that is held by the per-user limit of the
// given Workspace and goes before the given one (empty if none): the jobs of a member are released oldest first.
// A CrownJob whose Job exists was released, even if the cache still shows it held.
func (r *Reconciler) olderHeldJob(ctx context.Context, crownJob *clv1alpha2.CrownJob, workspace string) (string, error) {
	var crownJobs clv1alpha2.CrownJobList
	if err := r.List(ctx, &crownJobs, client.InNamespace(crownJob.Namespace)); err != nil {
		return "", fmt.Errorf("failed listing the crownjobs of the member: %w", err)
	}
	var held []clv1alpha2.CrownJob
	for i := range crownJobs.Items {
		other := &crownJobs.Items[i]
		if other.Name != crownJob.Name && isHeld(other) && other.DeletionTimestamp.IsZero() && forge.CrownJobHeldBefore(other, crownJob) {
			held = append(held, *other)
		}
	}
	forge.CrownJobSortHeld(held)

	workspaces := map[string]string{}
	for i := range held {
		var job batchv1.Job
		if err := r.Get(ctx, client.ObjectKeyFromObject(&held[i]), &job); err == nil {
			continue
		} else if !kerrors.IsNotFound(err) {
			return "", fmt.Errorf("failed retrieving the job of a held crownjob: %w", err)
		}
		heldWorkspace, err := r.crownJobWorkspace(ctx, &held[i], workspaces)
		if err != nil {
			return "", err
		}
		if heldWorkspace == workspace {
			return held[i].Name, nil
		}
	}
	return "", nil
}

// crownJobWorkspace returns the Workspace of the given CrownJob, through its Instance and Template (empty if one of
// them does not exist), keeping in memo the Workspace of each Instance read.
func (r *Reconciler) crownJobWorkspace(ctx context.Context, crownJob *clv1alpha2.CrownJob, memo map[string]string) (string, error) {
	name := crownJob.Spec.Instance.Name
	if workspace, found := memo[name]; found {
		return workspace, nil
	}
	var instance clv1alpha2.Instance
	var template clv1alpha2.Template
	workspace := ""
	if found, err := r.read(ctx, types.NamespacedName{Namespace: crownJob.Namespace, Name: name}, &instance); err != nil {
		return "", err
	} else if found {
		if found, err := r.read(ctx, forge.NamespacedNameFromGenericRef(instance.Spec.Template), &template); err != nil {
			return "", err
		} else if found {
			workspace = template.Spec.WorkspaceRef.Name
		}
	}
	memo[name] = workspace
	return workspace, nil
}

// isHeld returns whether the given CrownJob is held by the per-user limit.
func isHeld(crownJob *clv1alpha2.CrownJob) bool {
	return crownJob.Status.Phase == clv1alpha2.CrownJobPhaseWaiting && crownJob.Status.Reason == ReasonUserLimitReached
}

// heldCrownJobs maps a Job of a job queue to the held CrownJobs of its namespace: when one of the Jobs of a member ends
// or is deleted, their per-user limit may allow the next one; when one is created (a held job released), the next one
// of the member checks again whether it fits too.
func (r *Reconciler) heldCrownJobs(ctx context.Context, obj client.Object) []reconcile.Request {
	var crownJobs clv1alpha2.CrownJobList
	if err := r.List(ctx, &crownJobs, client.InNamespace(obj.GetNamespace())); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed listing the crownjobs of a namespace", "namespace", obj.GetNamespace())
		return nil
	}
	var requests []reconcile.Request
	for i := range crownJobs.Items {
		if isHeld(&crownJobs.Items[i]) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&crownJobs.Items[i])})
		}
	}
	return requests
}

// memberJobChanged selects the Jobs of a job queue that are created, end or are deleted.
var memberJobChanged = predicate.Funcs{
	CreateFunc: func(e event.CreateEvent) bool { return inJobQueue(e.Object) },
	DeleteFunc: func(e event.DeleteEvent) bool { return inJobQueue(e.Object) },
	UpdateFunc: func(e event.UpdateEvent) bool {
		return inJobQueue(e.ObjectNew) && !jobEnded(e.ObjectOld) && jobEnded(e.ObjectNew)
	},
	GenericFunc: func(event.GenericEvent) bool { return false },
}
