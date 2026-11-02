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
	"encoding/json"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// KueueVisibilityGroup -> the API group of the visibility API of Kueue (an aggregated API).
	KueueVisibilityGroup = "visibility.kueue.x-k8s.io"
	// KueueVisibilityVersion -> the version of the visibility API of Kueue used by the CrownJob controller.
	KueueVisibilityVersion = "v1beta2"
)

// KueuePendingWorkloadsPath returns the path of the pending workloads of the given ClusterQueue, in the visibility API
// of Kueue: all the waiting workloads of the ClusterQueue, in the order in which Kueue considers them.
func KueuePendingWorkloadsPath(clusterQueue string) string {
	return fmt.Sprintf("/apis/%s/%s/clusterqueues/%s/pendingworkloads", KueueVisibilityGroup, KueueVisibilityVersion, clusterQueue)
}

// KueuePendingWorkload is a waiting workload of a ClusterQueue, as listed by the visibility API of Kueue: only the
// fields the CrownJob controller reads.
type KueuePendingWorkload struct {
	Namespace string
	Name      string
	// JobUID and JobName identify the Job that owns the workload (empty if it is not owned by a Job).
	JobUID  types.UID
	JobName string
	// PositionInClusterQueue is the position of the workload among all the waiting workloads of the ClusterQueue,
	// starting from 0. (positionInLocalQueue is not read: in Kueue v0.18 the ClusterQueue endpoint counts it per
	// LocalQueue name, ignoring the namespace, and every tenant namespace has a LocalQueue with the same name.)
	PositionInClusterQueue int32
}

// kueuePendingWorkloadsSummary is the part of a PendingWorkloadsSummary of Kueue that is decoded.
type kueuePendingWorkloadsSummary struct {
	Items []struct {
		Metadata struct {
			Name            string `json:"name"`
			Namespace       string `json:"namespace"`
			OwnerReferences []struct {
				APIVersion string    `json:"apiVersion"`
				Kind       string    `json:"kind"`
				Name       string    `json:"name"`
				UID        types.UID `json:"uid"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
		PositionInClusterQueue *int32 `json:"positionInClusterQueue"`
	} `json:"items"`
}

// KueueDecodePendingWorkloads decodes the response of the pending workloads endpoint of a ClusterQueue. A workload
// without a position is an error: the response is not what is expected.
func KueueDecodePendingWorkloads(data []byte) ([]KueuePendingWorkload, error) {
	var summary kueuePendingWorkloadsSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, fmt.Errorf("failed decoding the pending workloads: %w", err)
	}
	workloads := make([]KueuePendingWorkload, 0, len(summary.Items))
	for i := range summary.Items {
		item := &summary.Items[i]
		if item.PositionInClusterQueue == nil {
			return nil, fmt.Errorf("the pending workload %s/%s has no positionInClusterQueue", item.Metadata.Namespace, item.Metadata.Name)
		}
		workload := KueuePendingWorkload{
			Namespace:              item.Metadata.Namespace,
			Name:                   item.Metadata.Name,
			PositionInClusterQueue: *item.PositionInClusterQueue,
		}
		for j := range item.Metadata.OwnerReferences {
			owner := &item.Metadata.OwnerReferences[j]
			if owner.Kind == "Job" && owner.APIVersion == batchv1.SchemeGroupVersion.String() {
				workload.JobUID, workload.JobName = owner.UID, owner.Name
				break
			}
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

// KueueQueuePositions returns the queue position of each waiting Job, by its UID, as shown to the user: the position
// of its workload among all the waiting workloads of the ClusterQueue, starting from 1.
func KueueQueuePositions(workloads []KueuePendingWorkload) map[types.UID]int32 {
	positions := make(map[types.UID]int32, len(workloads))
	for i := range workloads {
		if workloads[i].JobUID != "" {
			positions[workloads[i].JobUID] = workloads[i].PositionInClusterQueue + 1
		}
	}
	return positions
}
