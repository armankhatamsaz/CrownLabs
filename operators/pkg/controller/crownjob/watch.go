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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// waitingCrownJobs maps a change of a job queue (its status included) to every CrownJob whose Job waits in a job
// queue: what is free is shared, so a change of one job queue can change the waiting reason of a job in another. The
// state of the job queues read so far is forgotten first.
func (r *Reconciler) waitingCrownJobs(ctx context.Context, _ client.Object) []reconcile.Request {
	r.QueueCache.Invalidate()
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.HasLabels{forge.KueueQueueNameLabel}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed listing the jobs of the job queues")
		return nil
	}
	var requests []reconcile.Request
	for i := range jobs.Items {
		job := &jobs.Items[i]
		owner := metav1.GetControllerOf(job)
		if !inJobQueue(job) || !ptr.Deref(job.Spec.Suspend, false) || jobEnded(job) || owner == nil ||
			owner.Kind != crownJobKind || owner.APIVersion != clv1alpha2.GroupVersion.String() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: job.Namespace, Name: owner.Name}})
	}
	return requests
}

// crownJobKind -> the kind of the CrownJob, as in the owner references of its Jobs.
const crownJobKind = "CrownJob"

// inJobQueue returns whether the given object is a Job of a job queue (log helpers have no queue).
func inJobQueue(obj client.Object) bool {
	return strings.HasPrefix(obj.GetLabels()[forge.KueueQueueNameLabel], forge.CrownJobQueuePrefix)
}

// jobEnded returns whether the given object is a Job that is complete or failed.
func jobEnded(obj client.Object) bool {
	job, ok := obj.(*batchv1.Job)
	return ok && (jobConditionTrue(job, batchv1.JobComplete) != nil || jobConditionTrue(job, batchv1.JobFailed) != nil)
}
