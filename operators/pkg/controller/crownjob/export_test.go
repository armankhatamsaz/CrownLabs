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

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// JobEnvironment exposes jobEnvironment to the tests, for the Templates that the CEL rules of the CRD refuse.
var JobEnvironment = jobEnvironment

// CrownJobToInstance exposes crownJobToInstance to the tests.
var CrownJobToInstance = crownJobToInstance

// InstanceDeploymentToCrownJobs exposes the map function of the watch on the Deployments of the instances to the tests.
var InstanceDeploymentToCrownJobs = (*Reconciler).instanceDeploymentToCrownJobs

// SetupWithManagerFor registers the watches of the CrownJob controller on the given manager, with another reconciler:
// the tests see which requests each change enqueues.
func (r *Reconciler) SetupWithManagerFor(mgr ctrl.Manager, rec reconcile.Reconciler) error {
	return r.controllerBuilder(mgr, 1).WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).Complete(rec)
}

// SetBeforeJobCreate sets the function called right before a Job is created, with the lock of the member held.
func (r *Reconciler) SetBeforeJobCreate(f func()) {
	r.beforeJobCreate = f
}

// Positions exposes the queue positions of a Workspace read through the cache.
func (c *QueueCache) Positions(ctx context.Context, workspace string, reader VisibilityReader) (map[types.UID]int32, error) {
	return c.positions(ctx, workspace, reader)
}
