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

package forge_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"

	clv1alpha1 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha1"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

var _ = Describe("Kueue objects forging", func() {
	var workspace clv1alpha1.Workspace

	BeforeEach(func() {
		workspace = clv1alpha1.Workspace{
			ObjectMeta: metav1.ObjectMeta{Name: "ws", UID: "ws-uid"},
			Spec: clv1alpha1.WorkspaceSpec{QueueLimits: &clv1alpha1.QueueLimits{
				MaxAtOnce: map[string]resource.Quantity{
					"nvidia.com/gpu":   resource.MustParse("4"),
					"cpu":              resource.MustParse("8"),
					"amd.com/gpu":      resource.MustParse("1"),
					"example.com/fpga": resource.MustParse("2"),
				},
			}},
		}
	})

	workspaceOwner := metav1.OwnerReference{
		APIVersion: "crownlabs.polito.it/v1alpha1", Kind: "Workspace", Name: "ws", UID: "ws-uid",
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
	}

	resourceGroup := func(queue *unstructured.Unstructured) map[string]interface{} {
		groups, found, err := unstructured.NestedSlice(queue.Object, "spec", "resourceGroups")
		Expect(err).ToNot(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(groups).To(HaveLen(1))
		return groups[0].(map[string]interface{})
	}

	flavorResources := func(group map[string]interface{}) []interface{} {
		flavors := group["flavors"].([]interface{})
		Expect(flavors).To(HaveLen(1))
		flavor := flavors[0].(map[string]interface{})
		Expect(flavor["name"]).To(Equal("crownlabs-default"))
		return flavor["resources"].([]interface{})
	}

	quota := func(name, nominal string, borrowing ...string) forge.KueueResourceQuota {
		q := forge.KueueResourceQuota{Name: corev1.ResourceName(name), Nominal: resource.MustParse(nominal)}
		if len(borrowing) > 0 {
			q.BorrowingLimit = ptr.To(resource.MustParse(borrowing[0]))
		}
		return q
	}
	covered := []corev1.ResourceName{"cpu", "memory", "nvidia.com/gpu"}

	Describe("The forge.KueueClusterQueue function", func() {
		It("Should forge the ClusterQueue of the workspace in the root Cohort, with the given covered resources and quotas", func() {
			queue := forge.KueueClusterQueue(&workspace, covered,
				[]forge.KueueResourceQuota{quota("cpu", "1", "3"), quota("memory", "0"), quota("nvidia.com/gpu", "0", "0")})
			Expect(queue.GroupVersionKind()).To(Equal(forge.KueueClusterQueueGVK))
			Expect(queue.GetAPIVersion()).To(Equal("kueue.x-k8s.io/v1beta2"))
			Expect(queue.GetName()).To(Equal("crownlabs-ws"))
			Expect(queue.GetNamespace()).To(BeEmpty())
			Expect(queue.GetOwnerReferences()).To(ConsistOf(workspaceOwner))
			Expect(queue.GetLabels()).To(Equal(map[string]string{"crownlabs.polito.it/managed-by": "crownjob"}))

			group := resourceGroup(queue)
			Expect(group["coveredResources"]).To(Equal([]interface{}{"cpu", "memory", "nvidia.com/gpu"}))
			Expect(flavorResources(group)).To(Equal([]interface{}{
				map[string]interface{}{"name": "cpu", "nominalQuota": "1", "borrowingLimit": "3"},
				map[string]interface{}{"name": "memory", "nominalQuota": "0"},
				map[string]interface{}{"name": "nvidia.com/gpu", "nominalQuota": "0", "borrowingLimit": "0"},
			}))
		})

		It("Should select the tenant namespaces, use BestEffortFIFO, join the root Cohort and never preempt", func() {
			spec, _, err := unstructured.NestedMap(forge.KueueClusterQueue(&workspace, covered, nil).Object, "spec")
			Expect(err).ToNot(HaveOccurred())
			Expect(spec).To(HaveKeyWithValue("namespaceSelector", map[string]interface{}{
				"matchLabels": map[string]interface{}{"crownlabs.polito.it/type": "tenant"},
			}))
			Expect(spec).To(HaveKeyWithValue("queueingStrategy", "BestEffortFIFO"))
			Expect(spec).To(HaveKeyWithValue("cohortName", "crownlabs-jobs"))
			Expect(spec).To(HaveKeyWithValue("preemption", map[string]interface{}{
				"reclaimWithinCohort": "Never", "withinClusterQueue": "Never",
				"borrowWithinCohort": map[string]interface{}{"policy": "Never"},
			}))
			Expect(spec).ToNot(HaveKey("lendingLimit"))
			Expect(spec).To(HaveLen(5))
		})
	})

	Describe("The forge.KueueCohort function", func() {
		It("Should forge the root Cohort with the pool as nominal quota, no borrowing limit, no parent and no owner", func() {
			// A borrowing limit (as KueuePhase1 gives for a Cohort that does not exist yet) is never written.
			cohort := forge.KueueCohort(covered, []forge.KueueResourceQuota{quota("cpu", "2", "0"), quota("memory", "100Ti"), quota("nvidia.com/gpu", "10k", "0")})
			Expect(cohort.GroupVersionKind()).To(Equal(forge.KueueCohortGVK))
			Expect(cohort.GetName()).To(Equal("crownlabs-jobs"))
			Expect(cohort.GetNamespace()).To(BeEmpty())
			Expect(cohort.GetOwnerReferences()).To(BeEmpty())
			Expect(cohort.GetLabels()).To(Equal(map[string]string{"crownlabs.polito.it/managed-by": "crownjob"}))
			spec := cohort.Object["spec"].(map[string]interface{})
			Expect(spec).To(HaveLen(1))
			group := resourceGroup(cohort)
			Expect(group["coveredResources"]).To(Equal([]interface{}{"cpu", "memory", "nvidia.com/gpu"}))
			Expect(flavorResources(group)).To(Equal([]interface{}{
				map[string]interface{}{"name": "cpu", "nominalQuota": "2"},
				map[string]interface{}{"name": "memory", "nominalQuota": "100Ti"},
				map[string]interface{}{"name": "nvidia.com/gpu", "nominalQuota": "10k"},
			}))
		})
	})

	Describe("The forge.KueueQuotas function", func() {
		It("Should read back the quotas and the covered resources that KueueClusterQueue writes", func() {
			written := []forge.KueueResourceQuota{quota("cpu", "1500m", "3"), quota("memory", "0"), quota("nvidia.com/gpu", "0", "0")}
			quotas, coveredRead, err := forge.KueueQuotas(forge.KueueClusterQueue(&workspace, covered, written))
			Expect(err).ToNot(HaveOccurred())
			Expect(coveredRead).To(Equal(covered))
			Expect(forge.KueueQuotasEqual(quotas, written)).To(BeTrue())
			Expect(quotas[1].BorrowingLimit).To(BeNil())
		})

		It("Should read numbers, and ignore the resources of other flavors", func() {
			obj := &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{
				"resourceGroups": []interface{}{map[string]interface{}{
					"coveredResources": []interface{}{"cpu"},
					"flavors": []interface{}{
						map[string]interface{}{"name": "other", "resources": []interface{}{map[string]interface{}{"name": "cpu", "nominalQuota": "9"}}},
						map[string]interface{}{"name": "crownlabs-default", "resources": []interface{}{
							map[string]interface{}{"name": "cpu", "nominalQuota": int64(2), "borrowingLimit": int64(1)}}},
					},
				}},
			}}}
			quotas, _, err := forge.KueueQuotas(obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(forge.KueueQuotasEqual(quotas, []forge.KueueResourceQuota{quota("cpu", "2", "1")})).To(BeTrue())
		})
	})

	Describe("The forge.KueueReservation function", func() {
		It("Should read flavorsReservation in the flavor of the job queues", func() {
			obj := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{
				"flavorsReservation": []interface{}{
					map[string]interface{}{"name": "crownlabs-default", "resources": []interface{}{
						map[string]interface{}{"name": "cpu", "total": "1500m", "borrowed": "500m"},
						map[string]interface{}{"name": "memory", "total": "2Gi", "borrowed": "0"},
					}},
					map[string]interface{}{"name": "other", "resources": []interface{}{map[string]interface{}{"name": "cpu", "total": "9"}}},
				},
				"flavorsUsage": []interface{}{map[string]interface{}{"name": "crownlabs-default", "resources": []interface{}{
					map[string]interface{}{"name": "cpu", "total": "7"}}}},
			}}}
			reserved, err := forge.KueueReservation(obj)
			Expect(err).ToNot(HaveOccurred())
			Expect(reserved).To(HaveLen(2))
			Expect(reserved.Cpu().String()).To(Equal("1500m"))
			Expect(reserved.Memory().String()).To(Equal("2Gi"))
		})

		It("Should return nothing for a ClusterQueue without status", func() {
			reserved, err := forge.KueueReservation(&unstructured.Unstructured{Object: map[string]interface{}{}})
			Expect(err).ToNot(HaveOccurred())
			Expect(reserved).To(BeEmpty())
		})
	})

	Describe("The forge.KueueLocalQueue function", func() {
		It("Should forge the LocalQueue in the given namespace, pointing to the ClusterQueue of the workspace", func() {
			queue := forge.KueueLocalQueue(&workspace, "tenant-student")
			Expect(queue.GroupVersionKind()).To(Equal(forge.KueueLocalQueueGVK))
			Expect(queue.GetName()).To(Equal("crownlabs-ws"))
			Expect(queue.GetNamespace()).To(Equal("tenant-student"))
			Expect(queue.GetOwnerReferences()).To(ConsistOf(workspaceOwner))
			Expect(queue.Object["spec"]).To(Equal(map[string]interface{}{"clusterQueue": "crownlabs-ws"}))
		})
	})

	Describe("The forge.KueueResourceFlavor function", func() {
		It("Should forge the crownlabs-default ResourceFlavor with an empty spec and no owner", func() {
			flavor := forge.KueueResourceFlavor()
			Expect(flavor.GroupVersionKind()).To(Equal(forge.KueueResourceFlavorGVK))
			Expect(flavor.GetName()).To(Equal("crownlabs-default"))
			Expect(flavor.GetOwnerReferences()).To(BeEmpty())
			Expect(flavor.Object["spec"]).To(Equal(map[string]interface{}{}))
		})
	})
})
