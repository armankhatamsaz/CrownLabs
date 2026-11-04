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
	"time"

	"k8s.io/client-go/rest"

	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// VisibilityReader reads the waiting workloads of a ClusterQueue, in the order in which Kueue considers them.
type VisibilityReader interface {
	PendingWorkloads(ctx context.Context, clusterQueue string) ([]forge.KueuePendingWorkload, error)
}

// restVisibilityReader reads the visibility API of Kueue (an aggregated API) with plain REST calls, so that the
// operator has no Go dependency on Kueue.
type restVisibilityReader struct {
	client  rest.Interface
	timeout time.Duration
}

// NewVisibilityReader returns a VisibilityReader that calls the visibility API of Kueue through the given REST client
// (any client of the API server: the path is absolute), giving up after the given timeout. A call is never retried:
// the queue positions are read again at the next reconcile.
func NewVisibilityReader(client rest.Interface, timeout time.Duration) VisibilityReader {
	return &restVisibilityReader{client: client, timeout: timeout}
}

// PendingWorkloads returns the waiting workloads of the given ClusterQueue.
func (v *restVisibilityReader) PendingWorkloads(ctx context.Context, clusterQueue string) ([]forge.KueuePendingWorkload, error) {
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()
	result := v.client.Get().AbsPath(forge.KueuePendingWorkloadsPath(clusterQueue)).MaxRetries(0).Do(ctx)
	// Error decodes the Status sent by the API server (e.g., the permission that is missing), whereas the error of
	// DoRaw has "unknown" as its message.
	if err := result.Error(); err != nil {
		return nil, err
	}
	data, err := result.Raw()
	if err != nil {
		return nil, err
	}
	return forge.KueueDecodePendingWorkloads(data)
}
