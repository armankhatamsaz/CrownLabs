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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
)

// The Kueue objects are handled as unstructured objects, to avoid a Go dependency on Kueue
// (whose current versions require newer Go, Kubernetes and controller-runtime versions).

const (
	// KueueGroup -> the API group of Kueue.
	KueueGroup = "kueue.x-k8s.io"
	// KueueVersion -> the version of the Kueue API used by CrownLabs.
	KueueVersion = "v1beta2"

	// KueueQueueNameLabel -> the label of a Job naming the LocalQueue it is submitted to.
	KueueQueueNameLabel = "kueue.x-k8s.io/queue-name"
	// KueuePriorityClassLabel -> the label of a Job naming its WorkloadPriorityClass.
	KueuePriorityClassLabel = "kueue.x-k8s.io/priority-class"

	// CrownJobFieldManager -> the field manager used to apply the Kueue objects.
	CrownJobFieldManager = "crownlabs-crownjob"
	// CrownJobQueuePrefix -> the prefix of the name of the ClusterQueue and LocalQueue of a Workspace.
	CrownJobQueuePrefix = "crownlabs-"
	// CrownJobRootCohort -> the name of the root Cohort of the job queues, whose nominal quota is the shared pool.
	CrownJobRootCohort = "crownlabs-jobs"
	// CrownJobResourceFlavor -> the name of the ResourceFlavor used by every job queue.
	CrownJobResourceFlavor = "crownlabs-default"
	// CrownJobTenantNamespaceSelectorValue -> the value of the type label of the namespaces that can use a job queue.
	CrownJobTenantNamespaceSelectorValue = labelTypeTenantValue

	// LabelManagedByCrownJobValue -> the value of the managed-by label of the Kueue objects of the job queues.
	LabelManagedByCrownJobValue = "crownjob"

	kueuePreemptionNever = "Never"
)

var (
	// KueueClusterQueueGVK -> the GroupVersionKind of a Kueue ClusterQueue.
	KueueClusterQueueGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "ClusterQueue"}
	// KueueLocalQueueGVK -> the GroupVersionKind of a Kueue LocalQueue.
	KueueLocalQueueGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "LocalQueue"}
	// KueueResourceFlavorGVK -> the GroupVersionKind of a Kueue ResourceFlavor.
	KueueResourceFlavorGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "ResourceFlavor"}
	// KueueWorkloadPriorityClassGVK -> the GroupVersionKind of a Kueue WorkloadPriorityClass.
	KueueWorkloadPriorityClassGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "WorkloadPriorityClass"}
	// KueueCohortGVK -> the GroupVersionKind of a Kueue Cohort.
	KueueCohortGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "Cohort"}
	// KueueWorkloadGVK -> the GroupVersionKind of a Kueue Workload.
	KueueWorkloadGVK = schema.GroupVersionKind{Group: KueueGroup, Version: KueueVersion, Kind: "Workload"}

	// KueueNoLimitCPU -> the nominal quota of cpu in a job queue whose maxAtOnce does not limit it.
	// Kueue holds for ever a workload that requests a resource its ClusterQueue does not cover,
	// so cpu and memory are always covered, with a value that no job reaches.
	KueueNoLimitCPU = resource.MustParse("10000")
	// KueueNoLimitMemory -> the nominal quota of memory in a job queue whose maxAtOnce does not limit it (see KueueNoLimitCPU).
	KueueNoLimitMemory = resource.MustParse("100Ti")
)

// CrownJobQueueName returns the name of the ClusterQueue and of the LocalQueues of the given Workspace.
func CrownJobQueueName(workspace string) string {
	return CrownJobQueuePrefix + workspace
}

// KueueResourceFlavor forges the ResourceFlavor used by every job queue (with an empty spec).
func KueueResourceFlavor() *unstructured.Unstructured {
	flavor := kueueObject(KueueResourceFlavorGVK, CrownJobResourceFlavor, "")
	flavor.Object["spec"] = map[string]interface{}{}
	return flavor
}

// KueueClusterQueue forges the ClusterQueue of the job queue of the given Workspace, with the given covered resources
// and quotas (see KueueQueuePlanFor). It admits the jobs of the tenant namespaces, in BestEffortFIFO order, in the root
// Cohort, without preemption: a job that started is never stopped to make room for another.
func KueueClusterQueue(workspace *clv1alpha1.Workspace, covered []corev1.ResourceName, quotas []KueueResourceQuota) *unstructured.Unstructured {
	queue := kueueObject(KueueClusterQueueGVK, CrownJobQueueName(workspace.Name), "")
	queue.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(workspace, clv1alpha1.GroupVersion.WithKind("Workspace"))})
	queue.Object["spec"] = map[string]interface{}{
		"cohortName": CrownJobRootCohort,
		"namespaceSelector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				LabelTypeKey: CrownJobTenantNamespaceSelectorValue,
			},
		},
		"queueingStrategy": "BestEffortFIFO",
		"preemption": map[string]interface{}{
			"reclaimWithinCohort": kueuePreemptionNever,
			"withinClusterQueue":  kueuePreemptionNever,
			"borrowWithinCohort":  map[string]interface{}{"policy": kueuePreemptionNever},
		},
		"resourceGroups": KueueResourceGroups(covered, quotas),
	}
	return queue
}

// KueueCohort forges the root Cohort of the job queues, with the given covered resources and nominal quotas (the shared
// pool). A root Cohort cannot have a borrowing limit (Kueue refuses one without a parent): any is dropped.
// It has no owner: it is kept by the operator for as long as it runs.
func KueueCohort(covered []corev1.ResourceName, quotas []KueueResourceQuota) *unstructured.Unstructured {
	pool := make([]KueueResourceQuota, 0, len(quotas))
	for i := range quotas {
		pool = append(pool, KueueResourceQuota{Name: quotas[i].Name, Nominal: quotas[i].Nominal})
	}
	cohort := kueueObject(KueueCohortGVK, CrownJobRootCohort, "")
	cohort.Object["spec"] = map[string]interface{}{
		"resourceGroups": KueueResourceGroups(covered, pool),
	}
	return cohort
}

// KueueQuotas returns the quotas of the given ClusterQueue or Cohort in the ResourceFlavor of the job queues, in their
// order, and the resources its resource group covers.
func KueueQuotas(obj *unstructured.Unstructured) (quotas []KueueResourceQuota, covered []corev1.ResourceName, err error) {
	groups, _, err := unstructured.NestedSlice(obj.Object, "spec", "resourceGroups")
	if err != nil {
		return nil, nil, err
	}
	for _, g := range groups {
		group, _ := g.(map[string]interface{})
		names, _, _ := unstructured.NestedStringSlice(group, "coveredResources")
		for _, name := range names {
			covered = append(covered, corev1.ResourceName(name))
		}
		flavors, _, _ := unstructured.NestedSlice(group, "flavors")
		for _, f := range flavors {
			flavor, _ := f.(map[string]interface{})
			if flavor["name"] != CrownJobResourceFlavor {
				continue
			}
			resources, _, _ := unstructured.NestedSlice(flavor, "resources")
			for _, r := range resources {
				res, _ := r.(map[string]interface{})
				quota := KueueResourceQuota{Name: corev1.ResourceName(fmt.Sprint(res["name"]))}
				if quota.Nominal, err = parseKueueQuantity(res["nominalQuota"]); err != nil {
					return nil, nil, err
				}
				if value, found := res["borrowingLimit"]; found && value != nil {
					limit, err := parseKueueQuantity(value)
					if err != nil {
						return nil, nil, err
					}
					quota.BorrowingLimit = &limit
				}
				quotas = append(quotas, quota)
			}
		}
	}
	return quotas, covered, nil
}

// KueueReservation returns the quota reserved by the workloads of the given ClusterQueue in the ResourceFlavor of the
// job queues (status.flavorsReservation), by resource. It counts a workload from the moment its quota is reserved,
// before its pods run.
func KueueReservation(clusterQueue *unstructured.Unstructured) (corev1.ResourceList, error) {
	reserved := corev1.ResourceList{}
	flavors, _, err := unstructured.NestedSlice(clusterQueue.Object, "status", "flavorsReservation")
	if err != nil {
		return nil, err
	}
	for _, f := range flavors {
		flavor, _ := f.(map[string]interface{})
		if flavor["name"] != CrownJobResourceFlavor {
			continue
		}
		resources, _, _ := unstructured.NestedSlice(flavor, "resources")
		for _, r := range resources {
			res, _ := r.(map[string]interface{})
			total, err := parseKueueQuantity(res["total"])
			if err != nil {
				return nil, err
			}
			reserved[corev1.ResourceName(fmt.Sprint(res["name"]))] = total
		}
	}
	return reserved, nil
}

// KueueResourceGroups returns the single resource group of the job queues and of the root Cohort: the given covered
// resources, all in the ResourceFlavor of the job queues, with the given quotas.
func KueueResourceGroups(covered []corev1.ResourceName, quotas []KueueResourceQuota) []interface{} {
	names := make([]interface{}, 0, len(covered))
	for _, name := range covered {
		names = append(names, name.String())
	}
	resources := make([]interface{}, 0, len(quotas))
	for i := range quotas {
		res := map[string]interface{}{
			"name":         quotas[i].Name.String(),
			"nominalQuota": quotas[i].Nominal.String(),
		}
		if quotas[i].BorrowingLimit != nil {
			res["borrowingLimit"] = quotas[i].BorrowingLimit.String()
		}
		resources = append(resources, res)
	}
	return []interface{}{
		map[string]interface{}{
			"coveredResources": names,
			"flavors": []interface{}{
				map[string]interface{}{
					"name":      CrownJobResourceFlavor,
					"resources": resources,
				},
			},
		},
	}
}

// parseKueueQuantity parses a quantity of a Kueue object, written as a string or as a number.
func parseKueueQuantity(value interface{}) (resource.Quantity, error) {
	if value == nil {
		return resource.Quantity{}, nil
	}
	quantity, err := resource.ParseQuantity(fmt.Sprint(value))
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid quantity %v: %w", value, err)
	}
	return quantity, nil
}

// KueueLocalQueue forges the LocalQueue of the job queue of the given Workspace, in the given (tenant) namespace.
func KueueLocalQueue(workspace *clv1alpha1.Workspace, namespace string) *unstructured.Unstructured {
	queue := kueueObject(KueueLocalQueueGVK, CrownJobQueueName(workspace.Name), namespace)
	queue.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(workspace, clv1alpha1.GroupVersion.WithKind("Workspace"))})
	queue.Object["spec"] = map[string]interface{}{
		"clusterQueue": CrownJobQueueName(workspace.Name),
	}
	return queue
}

// kueueObject forges an unstructured Kueue object of the given kind, with the managed-by label.
func kueueObject(gvk schema.GroupVersionKind, name, namespace string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetLabels(map[string]string{LabelManagedByKey: LabelManagedByCrownJobValue})
	return obj
}
