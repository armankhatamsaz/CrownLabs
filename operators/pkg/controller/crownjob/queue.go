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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	ctrlcommon "github.com/netgroup-polito/CrownLabs/operators/pkg/controller/common"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/utils"
)

// QueueReconciler reconciles the job queues of all the Workspaces together, under a single key: the root Cohort (the
// shared pool), the ResourceFlavor shared by every job queue, and the ClusterQueue of each job-enabled Workspace,
// with the limits in force now. The LocalQueues are applied by the CrownJob controller, right before each Job.
//
// The quotas are changed in two phases: first every decrease (each quota to the smallest of its current and desired
// values), then, once every write of the first phase has landed, the desired values. Kueue admits waiting jobs about
// a second after any quota change, so the other order would admit jobs above the capacity, which no later change
// takes back: no job is ever stopped. Lowering a quota below what running jobs use stops nothing either.
type QueueReconciler struct {
	client.Client
	// APIReader reads the current Cohort and ClusterQueues without cache: the values of the first phase are the
	// smallest of these and the desired ones, so they must be those the API server holds.
	APIReader      client.Reader
	TargetLabel    ctrlcommon.KVLabel
	EventsRecorder record.EventRecorder
	// Capacity is the job capacity: the most all the jobs may use at the same time, by resource. Without it (empty),
	// every Workspace can use up to its maxAtOnce, and the guarantees have no effect.
	Capacity corev1.ResourceList
	// DefaultTimeZone is the time zone of the job windows of the Workspaces whose jobPolicy sets none.
	DefaultTimeZone *time.Location
	// Clock returns the current time: the limits in force depend on it.
	Clock clock.PassiveClock

	// reported holds the message of the last event recorded on each Workspace for each reason, so that an event is
	// recorded once per change, not at every reconcile.
	reported   map[string]string
	reportedMu sync.Mutex

	// This function, if configured, is deferred at the beginning of the Reconcile.
	// Specifically, it is meant to be set to GinkgoRecover during the tests,
	// in order to lead to a controlled failure in case the Reconcile panics.
	ReconcileDeferHook func()
}

// queueRequeueMax -> the longest time between two reconciles of the job queues.
const queueRequeueMax = 10 * time.Minute

// queueRequeueMargin -> the time added to the next start or end of a job window, so that it is in the past when the
// job queues are reconciled.
const queueRequeueMargin = time.Second

// queueReconcileRequest is the single key of the job queues.
var queueReconcileRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: forge.CrownJobRootCohort}}

// SetupWithManager registers a new controller for the job queues of the Workspaces, named explicitly since the
// Workspace controller already takes the default name derived from the Workspace kind. Every change of a Workspace
// (spec or labels), and every change of the spec of a job queue or of the root Cohort (to restore them), enqueues
// the single key; the status updates written by Kueue do not.
func (r *QueueReconciler) SetupWithManager(mgr ctrl.Manager, concurrency int) error {
	clusterQueue := &unstructured.Unstructured{}
	clusterQueue.SetGroupVersionKind(forge.KueueClusterQueueGVK)
	cohort := &unstructured.Unstructured{}
	cohort.SetGroupVersionKind(forge.KueueCohortGVK)

	toQueues := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{queueReconcileRequest}
	})
	isJobQueue := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return strings.HasPrefix(obj.GetName(), forge.CrownJobQueuePrefix)
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("crownjob-queue").
		Watches(&clv1alpha1.Workspace{}, toQueues,
			builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{}))).
		Watches(clusterQueue, toQueues, builder.WithPredicates(predicate.GenerationChangedPredicate{}, isJobQueue)).
		Watches(cohort, toQueues, builder.WithPredicates(predicate.GenerationChangedPredicate{}, isJobQueue)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: concurrency,
		}).
		WithLogConstructor(utils.LogConstructor(mgr.GetLogger(), "CrownJobQueue")).
		Complete(r)
}

// queueObject is a job queue or the root Cohort, with its current quotas and those to apply.
type queueObject struct {
	kind    string
	name    string
	current *unstructured.Unstructured
	// effective are the current quotas as Kueue uses them (nil if the object does not exist).
	effective []forge.KueueResourceQuota
	desired   []forge.KueueResourceQuota
	forge     func(quotas []forge.KueueResourceQuota) *unstructured.Unstructured
	// remove tells that the job queue is to be deleted once its quotas are 0.
	remove bool
	// deleting tells that the job queue is already being deleted (e.g., by the garbage collector, when its Workspace
	// is deleted): Kueue keeps it while its jobs run, and still counts its quotas.
	deleting bool
}

// Reconcile reconciles the job queues of all the Workspaces (the request is always the single key).
func (r *QueueReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	if r.ReconcileDeferHook != nil {
		defer r.ReconcileDeferHook()
	}
	log := ctrl.LoggerFrom(ctx)
	now := r.Clock.Now()

	// The job-enabled Workspaces, with the limits in force now, and the next start or end of one of their windows.
	var workspaces clv1alpha1.WorkspaceList
	if err := r.List(ctx, &workspaces); err != nil {
		log.Error(err, "failed listing the workspaces")
		return ctrl.Result{}, err
	}
	enabled := map[string]*clv1alpha1.Workspace{}
	var limits []forge.KueueWorkspaceLimits
	var specs []*clv1alpha1.WorkspaceSpec
	next := time.Time{}
	for i := range workspaces.Items {
		workspace := &workspaces.Items[i]
		if !r.TargetLabel.IsIncluded(workspace.Labels) || workspace.Spec.QueueLimits == nil || !workspace.DeletionTimestamp.IsZero() {
			continue
		}
		loc := r.timeZone(workspace)
		enabled[workspace.Name] = workspace
		limits = append(limits, forge.KueueWorkspaceLimits{Name: workspace.Name, Limits: forge.JobLimitsAt(&workspace.Spec, loc, now)})
		specs = append(specs, &workspace.Spec)
		if boundary, found := forge.JobNextBoundary(&workspace.Spec, loc, now); found && (next.IsZero() || boundary.Before(next)) {
			next = boundary
		}
	}

	objects, err := r.queueObjects(ctx, enabled, limits, specs)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(enabled) > 0 {
		if err := r.enforceResourceFlavor(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Phase 1: every decrease. Phase 2, once every write of phase 1 has landed: the desired values.
	for _, obj := range objects {
		if err := r.applyPhase1(ctx, obj); err != nil {
			return ctrl.Result{}, err
		}
	}
	for _, obj := range objects {
		if err := r.applyPhase2(ctx, obj); err != nil {
			return ctrl.Result{}, err
		}
	}

	requeue := queueRequeueMax
	if !next.IsZero() {
		requeue = min(requeue, max(next.Sub(now), 0)+queueRequeueMargin)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// queueObjects returns the root Cohort and the job queues to reconcile, in this order: the Cohort, the job queues of the
// enabled Workspaces by name, then those to remove. Their current state is read with the API reader.
func (r *QueueReconciler) queueObjects(ctx context.Context, enabled map[string]*clv1alpha1.Workspace,
	limits []forge.KueueWorkspaceLimits, specs []*clv1alpha1.WorkspaceSpec) ([]*queueObject, error) {
	log := ctrl.LoggerFrom(ctx)

	cohort, err := r.readKueueObject(ctx, forge.KueueCohortGVK, forge.CrownJobRootCohort)
	if err != nil {
		return nil, err
	}
	var previous []corev1.ResourceName
	var cohortQuotas []forge.KueueResourceQuota
	if cohort != nil {
		if cohortQuotas, previous, err = forge.KueueQuotas(cohort); err != nil {
			log.Error(err, "failed reading the quotas of the root cohort")
			return nil, err
		}
	}

	queues := &unstructured.UnstructuredList{}
	queues.SetGroupVersionKind(forge.KueueClusterQueueGVK.GroupVersion().WithKind(forge.KueueClusterQueueGVK.Kind + "List"))
	if err := r.APIReader.List(ctx, queues, client.MatchingLabels{forge.LabelManagedByKey: forge.LabelManagedByCrownJobValue}); err != nil {
		log.Error(err, "failed listing the cluster queues")
		return nil, err
	}
	current := map[string]*unstructured.Unstructured{}
	for i := range queues.Items {
		current[queues.Items[i].GetName()] = &queues.Items[i]
	}

	covered := forge.KueueCoveredResources(previous, r.Capacity, specs)
	plan := forge.KueueQueuePlanFor(limits, r.Capacity, covered)
	r.recordPlanEvents(enabled, plan.Events)

	objects := []*queueObject{{
		kind: forge.KueueCohortGVK.Kind, name: forge.CrownJobRootCohort, current: cohort, effective: cohortQuotas, desired: plan.Pool,
		forge: func(quotas []forge.KueueResourceQuota) *unstructured.Unstructured {
			return forge.KueueCohort(covered, quotas)
		},
	}}
	for i := range limits {
		workspace := enabled[limits[i].Name]
		name := forge.CrownJobQueueName(workspace.Name)
		queue, found := current[name]
		if !found {
			// A ClusterQueue with the name of the job queue, without the managed-by label, is taken over too.
			if queue, err = r.readKueueObject(ctx, forge.KueueClusterQueueGVK, name); err != nil {
				return nil, err
			}
		}
		delete(current, name)
		effective, err := effectiveQuotas(queue)
		if err != nil {
			log.Error(err, "failed reading the quotas of the cluster queue", "clusterqueue", name)
			return nil, err
		}
		objects = append(objects, &queueObject{
			kind: forge.KueueClusterQueueGVK.Kind, name: name, current: queue, effective: effective, desired: plan.Queues[workspace.Name],
			forge: func(quotas []forge.KueueResourceQuota) *unstructured.Unstructured {
				return forge.KueueClusterQueue(workspace, covered, quotas)
			},
		})
	}

	// The job queues no longer wanted: those controlled by a Workspace that lost its queueLimits or the target label
	// (or was deleted). A ClusterQueue that no Workspace controls is never touched. One already being deleted is
	// zeroed too: Kueue still counts its quotas until its jobs end, and the pool may grow in the same reconcile.
	for name, queue := range current {
		owner := metav1.GetControllerOf(queue)
		if owner == nil || owner.Kind != "Workspace" || !strings.HasPrefix(name, forge.CrownJobQueuePrefix) {
			continue
		}
		effective, err := effectiveQuotas(queue)
		if err != nil {
			log.Error(err, "failed reading the quotas of the cluster queue", "clusterqueue", name)
			return nil, err
		}
		zero := make([]forge.KueueResourceQuota, 0, len(effective))
		for i := range effective {
			zero = append(zero, forge.KueueResourceQuota{Name: effective[i].Name, BorrowingLimit: &resource.Quantity{}})
		}
		objects = append(objects, &queueObject{kind: forge.KueueClusterQueueGVK.Kind, name: name, current: queue,
			effective: effective, desired: zero, remove: true, deleting: !queue.GetDeletionTimestamp().IsZero()})
	}
	return objects, nil
}

// applyPhase1 applies the decreases of the given object: each quota to the smallest of its current and desired values.
// A new object is created with every quota at 0. An object already at those values is not written.
func (r *QueueReconciler) applyPhase1(ctx context.Context, obj *queueObject) error {
	phase1 := forge.KueuePhase1(obj.effective, obj.desired)
	if obj.current != nil && !r.phase1Needed(obj, phase1) {
		return nil
	}
	if obj.remove {
		return r.patchQuotas(ctx, obj, phase1, "phase 1 (decreases): quotas to 0 before the deletion")
	}
	return r.apply(ctx, obj, obj.forge(phase1), "phase 1 (decreases)")
}

// applyPhase2 applies the desired values of the given object, or deletes a job queue to remove (Kueue keeps it while
// its jobs run).
func (r *QueueReconciler) applyPhase2(ctx context.Context, obj *queueObject) error {
	if !obj.remove {
		return r.apply(ctx, obj, obj.forge(obj.desired), "phase 2 (desired values)")
	}
	if obj.deleting {
		return nil
	}
	queue := &unstructured.Unstructured{}
	queue.SetGroupVersionKind(forge.KueueClusterQueueGVK)
	queue.SetName(obj.name)
	if err := r.Delete(ctx, queue); err != nil && !kerrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Error(err, "failed to delete the cluster queue", "clusterqueue", obj.name)
		return err
	}
	ctrl.LoggerFrom(ctx).Info("job queues, phase 2: cluster queue deleted", "object", obj.kind+"/"+obj.name)
	return nil
}

// phase1Needed returns whether the first phase must write the given object: its quotas, as the API server holds them,
// differ from those of the first phase, or it is not yet in the root Cohort.
func (r *QueueReconciler) phase1Needed(obj *queueObject, phase1 []forge.KueueResourceQuota) bool {
	written, _, err := forge.KueueQuotas(obj.current)
	if err != nil || !forge.KueueQuotasEqual(written, phase1) {
		return true
	}
	if obj.kind == forge.KueueClusterQueueGVK.Kind && !obj.remove {
		cohort, _, _ := unstructured.NestedString(obj.current.Object, "spec", "cohortName")
		return cohort != forge.CrownJobRootCohort
	}
	return false
}

// apply applies the given object with server-side apply, and checks that the response holds the quotas sent: a write
// that is accepted but changed does not count as done.
func (r *QueueReconciler) apply(ctx context.Context, obj *queueObject, desired *unstructured.Unstructured, phase string) error {
	log := ctrl.LoggerFrom(ctx)
	sent, _, err := forge.KueueQuotas(desired)
	if err != nil {
		return err
	}
	// Whether the write changed the spec: the generation moves with the spec only (Kueue writes the status meanwhile).
	previousGeneration := int64(0)
	if obj.current != nil {
		previousGeneration = obj.current.GetGeneration()
	}
	if err := r.Patch(ctx, desired, client.Apply, client.FieldOwner(forge.CrownJobFieldManager), client.ForceOwnership); err != nil {
		log.Error(err, "failed to apply the job queue object", "phase", phase, "object", obj.kind+"/"+obj.name)
		return err
	}
	if err := checkResponse(desired, sent); err != nil {
		log.Error(err, "the job queue object was not written as sent", "phase", phase, "object", obj.kind+"/"+obj.name)
		return err
	}
	changed := desired.GetGeneration() != previousGeneration
	obj.current, obj.effective = desired, sent
	logger := log
	if !changed {
		logger = log.V(utils.LogDebugLevel)
	}
	logger.Info("job queues, "+phase, "object", obj.kind+"/"+obj.name, "quotas", forge.FormatKueueQuotas(sent),
		"changed", changed, "generation", desired.GetGeneration())
	return nil
}

// patchQuotas sets the quotas of a job queue to remove with a merge patch of its resource groups only, refused if the
// queue changed since it was read.
func (r *QueueReconciler) patchQuotas(ctx context.Context, obj *queueObject, quotas []forge.KueueResourceQuota, phase string) error {
	log := ctrl.LoggerFrom(ctx)
	_, covered, err := forge.KueueQuotas(obj.current)
	if err != nil {
		return err
	}
	patched := obj.current.DeepCopy()
	if err := unstructured.SetNestedSlice(patched.Object, forge.KueueResourceGroups(covered, quotas), "spec", "resourceGroups"); err != nil {
		return err
	}
	if err := r.Patch(ctx, patched, client.MergeFromWithOptions(obj.current, client.MergeFromWithOptimisticLock{})); err != nil {
		if kerrors.IsNotFound(err) {
			// Gone meanwhile: it no longer counts.
			log.Info("job queues, "+phase+": the cluster queue is gone", "object", obj.kind+"/"+obj.name)
			obj.deleting = true
			return nil
		}
		log.Error(err, "failed to patch the quotas of the cluster queue", "phase", phase, "object", obj.kind+"/"+obj.name)
		return err
	}
	if err := checkResponse(patched, quotas); err != nil {
		log.Error(err, "the cluster queue was not written as sent", "phase", phase, "object", obj.kind+"/"+obj.name)
		return err
	}
	obj.current, obj.effective = patched, quotas
	log.Info("job queues, "+phase, "object", obj.kind+"/"+obj.name, "quotas", forge.FormatKueueQuotas(quotas),
		"changed", true, "generation", patched.GetGeneration())
	return nil
}

// checkResponse returns an error if the given object, as returned by the API server, does not hold the given quotas.
func checkResponse(response *unstructured.Unstructured, sent []forge.KueueResourceQuota) error {
	got, _, err := forge.KueueQuotas(response)
	if err != nil {
		return err
	}
	if !forge.KueueQuotasEqual(got, sent) {
		return fmt.Errorf("%s %s: sent %s, the API server holds %s", response.GetKind(), response.GetName(),
			forge.FormatKueueQuotas(sent), forge.FormatKueueQuotas(got))
	}
	return nil
}

// effectiveQuotas returns the quotas of the given ClusterQueue as Kueue uses them: those of its spec, except that a
// ClusterQueue outside any Cohort cannot borrow, whatever its (unset) borrowing limit says. It returns nil for a
// ClusterQueue that does not exist.
func effectiveQuotas(queue *unstructured.Unstructured) ([]forge.KueueResourceQuota, error) {
	if queue == nil {
		return nil, nil
	}
	quotas, _, err := forge.KueueQuotas(queue)
	if err != nil {
		return nil, err
	}
	if cohort, _, _ := unstructured.NestedString(queue.Object, "spec", "cohortName"); cohort == "" {
		for i := range quotas {
			quotas[i].BorrowingLimit = &resource.Quantity{}
		}
	}
	return quotas, nil
}

// readKueueObject reads the Kueue object of the given kind and name with the API reader (nil if it does not exist).
func (r *QueueReconciler) readKueueObject(ctx context.Context, gvk schema.GroupVersionKind, name string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: name}, obj); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		ctrl.LoggerFrom(ctx).Error(err, "failed retrieving the kueue object", "kind", gvk.Kind, "name", name)
		return nil, err
	}
	return obj, nil
}

// timeZone returns the time zone of the job windows of the given Workspace, or nil (the windows are ignored) if it is
// not valid, in which case an event is recorded on the Workspace (once per change).
func (r *QueueReconciler) timeZone(workspace *clv1alpha1.Workspace) *time.Location {
	loc, err := forge.JobTimeZone(workspace.Spec.JobPolicy, r.DefaultTimeZone)
	message := ""
	if err != nil {
		message = fmt.Sprintf(msgInvalidTimeZone, workspace.Spec.JobPolicy.TimeZone)
		loc = nil
	}
	r.reportOnce(workspace, ReasonInvalidTimeZone, message)
	return loc
}

// recordPlanEvents records the events of the quota plan on their Workspaces, once per change of each.
func (r *QueueReconciler) recordPlanEvents(enabled map[string]*clv1alpha1.Workspace, events []forge.KueueQueueEvent) {
	messages := map[string]string{}
	for _, event := range events {
		messages[event.Workspace+"/"+event.Reason] = event.Message
	}
	for name, workspace := range enabled {
		for _, reason := range []string{forge.ReasonGuaranteeAboveMaximum, forge.ReasonGuaranteeCut} {
			r.reportOnce(workspace, reason, messages[name+"/"+reason])
		}
	}
}

// reportOnce records a warning event with the given reason and message on the Workspace, unless the last one recorded
// with that reason had the same message. An empty message records nothing, and forgets the last one.
func (r *QueueReconciler) reportOnce(workspace *clv1alpha1.Workspace, reason, message string) {
	r.reportedMu.Lock()
	defer r.reportedMu.Unlock()
	if r.reported == nil {
		r.reported = map[string]string{}
	}
	key := workspace.Name + "/" + reason
	if message == "" {
		delete(r.reported, key)
		return
	}
	if r.reported[key] == message {
		return
	}
	r.reported[key] = message
	r.EventsRecorder.Event(workspace, corev1.EventTypeWarning, reason, message)
}

// enforceResourceFlavor creates the ResourceFlavor used by every job queue, if it is missing. It is never deleted.
func (r *QueueReconciler) enforceResourceFlavor(ctx context.Context) error {
	flavor := forge.KueueResourceFlavor()
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(forge.KueueResourceFlavorGVK)
	err := r.Get(ctx, types.NamespacedName{Name: flavor.GetName()}, existing)
	if err == nil || !kerrors.IsNotFound(err) {
		return err
	}
	if err := r.Create(ctx, flavor, client.FieldOwner(forge.CrownJobFieldManager)); err != nil && !kerrors.IsAlreadyExists(err) {
		ctrl.LoggerFrom(ctx).Error(err, "failed to create the resource flavor", "resourceflavor", flavor.GetName())
		return err
	}
	ctrl.LoggerFrom(ctx).Info("resource flavor created", "resourceflavor", flavor.GetName())
	return nil
}
