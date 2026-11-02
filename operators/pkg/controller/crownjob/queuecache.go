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
	"sync"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/utils"
)

// QueueCache keeps, for a short time, what the CrownJob reconciler reads about the job queues: the root Cohort and the
// ClusterQueues of the Workspaces (for the waiting reasons), and the waiting workloads of each ClusterQueue (for the
// queue positions). A change of a job queue reconciles every waiting job, and the reconciles that follow it share one
// read. Every change of a ClusterQueue (its status included) invalidates what was read before it, so that no
// reconcile triggered by a change reads the state from before the change. A nil QueueCache reads at every call.
type QueueCache struct {
	ttl   time.Duration
	clock clock.PassiveClock

	queuesMu sync.Mutex
	queues   *queueSnapshot
	queuesAt time.Time

	pendingMu sync.Mutex
	pending   map[string]*pendingEntry
	// failureLogged is when the last failure to read the waiting workloads of each Workspace was logged.
	failureLogged map[string]time.Time
}

// queueSnapshot is the state of the job queues: the root Cohort (nil if it does not exist) and the ClusterQueues with
// the managed-by label of the job queues, by name.
type queueSnapshot struct {
	cohort *unstructured.Unstructured
	queues map[string]*unstructured.Unstructured
}

// pendingEntry is the result of a read of the waiting workloads of the ClusterQueue of a Workspace: the queue position
// of each waiting Job, by UID, or the error of the read.
type pendingEntry struct {
	at        time.Time
	positions map[types.UID]int32
	err       error
}

const (
	// QueueCacheTTL -> how long the state of the job queues is kept, unless a ClusterQueue changes meanwhile.
	QueueCacheTTL = time.Second
	// pendingFailureTTL -> how long a failure to read the waiting workloads of a ClusterQueue is kept: the visibility
	// API is not called again by every reconcile while it is unavailable. A change of a ClusterQueue does not clear it.
	pendingFailureTTL = 10 * time.Second
	// pendingFailureLogInterval -> how often a failure to read the waiting workloads of a Workspace is logged at most.
	pendingFailureLogInterval = time.Minute
	// VisibilityTimeout -> the longest a call to the visibility API of Kueue may take.
	VisibilityTimeout = 5 * time.Second
)

// NewQueueCache returns a QueueCache keeping what it reads for the given time, as told by the given clock.
func NewQueueCache(ttl time.Duration, clk clock.PassiveClock) *QueueCache {
	return &QueueCache{ttl: ttl, clock: clk, pending: map[string]*pendingEntry{}, failureLogged: map[string]time.Time{}}
}

// Invalidate forgets the state of the job queues and the waiting workloads read so far (not the failures).
func (c *QueueCache) Invalidate() {
	if c == nil {
		return
	}
	c.queuesMu.Lock()
	c.queues = nil
	c.queuesMu.Unlock()

	c.pendingMu.Lock()
	for workspace, entry := range c.pending {
		if entry.err == nil {
			delete(c.pending, workspace)
		}
	}
	c.pendingMu.Unlock()
}

// snapshot returns the state of the job queues, read with the given reader unless it was read less than the TTL ago.
func (c *QueueCache) snapshot(ctx context.Context, reader client.Reader) (*queueSnapshot, error) {
	if c == nil {
		return readQueueSnapshot(ctx, reader)
	}
	c.queuesMu.Lock()
	defer c.queuesMu.Unlock()
	if now := c.clock.Now(); c.queues != nil && now.Sub(c.queuesAt) < c.ttl {
		return c.queues, nil
	}
	snapshot, err := readQueueSnapshot(ctx, reader)
	if err != nil {
		return nil, err
	}
	c.queues, c.queuesAt = snapshot, c.clock.Now()
	return snapshot, nil
}

// readQueueSnapshot reads the root Cohort and the ClusterQueues with the managed-by label of the job queues.
func readQueueSnapshot(ctx context.Context, reader client.Reader) (*queueSnapshot, error) {
	cohort := &unstructured.Unstructured{}
	cohort.SetGroupVersionKind(forge.KueueCohortGVK)
	if err := reader.Get(ctx, types.NamespacedName{Name: forge.CrownJobRootCohort}, cohort); err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}
		cohort = nil
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(forge.KueueClusterQueueGVK.GroupVersion().WithKind(forge.KueueClusterQueueGVK.Kind + "List"))
	if err := reader.List(ctx, list, client.MatchingLabels{forge.LabelManagedByKey: forge.LabelManagedByCrownJobValue}); err != nil {
		return nil, err
	}
	snapshot := &queueSnapshot{cohort: cohort, queues: make(map[string]*unstructured.Unstructured, len(list.Items))}
	for i := range list.Items {
		snapshot.queues[list.Items[i].GetName()] = &list.Items[i]
	}
	ctrl.LoggerFrom(ctx).V(utils.LogDebugLevel).Info("job queues read", "read", "queue-state", "clusterqueues", len(list.Items))
	return snapshot, nil
}

// positions returns the queue position of each waiting Job of the given Workspace, by UID, read with the given reader
// unless it was read less than the TTL ago (a failure: less than pendingFailureTTL ago). A failure is logged at most
// once per pendingFailureLogInterval for each Workspace.
func (c *QueueCache) positions(ctx context.Context, workspace string, reader VisibilityReader) (map[types.UID]int32, error) {
	if c == nil {
		entry := readPositions(ctx, workspace, reader, time.Time{})
		if entry.err != nil {
			ctrl.LoggerFrom(ctx).Info("failed reading the waiting workloads of the job queue: no queue positions",
				"workspace", workspace, "error", entry.err.Error())
		}
		return entry.positions, entry.err
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	now := c.clock.Now()
	if entry, found := c.pending[workspace]; found {
		if age := now.Sub(entry.at); (entry.err == nil && age < c.ttl) || (entry.err != nil && age < pendingFailureTTL) {
			return entry.positions, entry.err
		}
	}
	entry := readPositions(ctx, workspace, reader, now)
	c.pending[workspace] = entry
	if entry.err != nil {
		if last, found := c.failureLogged[workspace]; !found || now.Sub(last) >= pendingFailureLogInterval {
			c.failureLogged[workspace] = now
			ctrl.LoggerFrom(ctx).Info("failed reading the waiting workloads of the job queue: no queue positions",
				"workspace", workspace, "error", entry.err.Error())
		}
	}
	return entry.positions, entry.err
}

// readPositions reads the waiting workloads of the ClusterQueue of the given Workspace, and returns the queue position
// of each waiting Job.
func readPositions(ctx context.Context, workspace string, reader VisibilityReader, now time.Time) *pendingEntry {
	workloads, err := reader.PendingWorkloads(ctx, forge.CrownJobQueueName(workspace))
	if err != nil {
		return &pendingEntry{at: now, err: err}
	}
	ctrl.LoggerFrom(ctx).V(utils.LogDebugLevel).Info("job queues read", "read", "pending-workloads", "workspace", workspace,
		"workloads", len(workloads))
	return &pendingEntry{at: now, positions: forge.KueueQueuePositions(workloads)}
}
