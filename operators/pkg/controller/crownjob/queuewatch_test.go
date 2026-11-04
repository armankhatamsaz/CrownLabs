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

package crownjob_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clv1alpha2 "github.com/netgroup-polito/CrownLabs/operators/api/v1alpha2"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/controller/crownjob"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
)

// fakeVisibility is a VisibilityReader that returns the workloads set by the test, or its error, and counts its calls.
type fakeVisibility struct {
	mu        sync.Mutex
	workloads map[string][]forge.KueuePendingWorkload
	err       error
	calls     int
}

func (f *fakeVisibility) PendingWorkloads(_ context.Context, clusterQueue string) ([]forge.KueuePendingWorkload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.workloads[clusterQueue], nil
}

func (f *fakeVisibility) set(clusterQueue string, jobs ...*batchv1.Job) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.workloads == nil {
		f.workloads = map[string][]forge.KueuePendingWorkload{}
	}
	workloads := make([]forge.KueuePendingWorkload, 0, len(jobs))
	for i, job := range jobs {
		workloads = append(workloads, forge.KueuePendingWorkload{Namespace: job.Namespace, Name: "job-" + job.Name,
			JobUID: job.UID, JobName: job.Name, PositionInClusterQueue: int32(i)})
	}
	f.workloads[clusterQueue] = workloads
}

func (f *fakeVisibility) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// countingReader is an API reader that counts the reads of the root Cohort and of the lists of ClusterQueues.
type countingReader struct {
	client.Reader
	cohortGets, queueLists atomic.Int32
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == forge.KueueCohortGVK {
		c.cohortGets.Add(1)
	}
	return c.Reader.Get(ctx, key, obj, opts...)
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind().Kind == forge.KueueClusterQueueGVK.Kind+"List" {
		c.queueLists.Add(1)
	}
	return c.Reader.List(ctx, list, opts...)
}

// waitingJob creates a CrownJob of the given instance and its Job, waiting in the queue.
func waitingJob(instanceName string) (*clv1alpha2.CrownJob, *batchv1.Job) {
	cj := newCrownJob(instanceName, []string{"sh", "-c", "sleep 1"}, "")
	reconcileCrownJob(cj)
	job, err := getJob(cj.Name)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
	ExpectWithOffset(1, ptr.Deref(job.Spec.Suspend, false)).To(BeTrue())
	return cj, job
}

var _ = Describe("Queue positions and fresh waiting reasons (P12c)", func() {
	BeforeEach(func() { drainEvents() })

	Describe("The queue position of a waiting job", func() {
		var visibility *fakeVisibility
		var withPositions crownjob.Reconciler

		BeforeEach(func() {
			visibility = &fakeVisibility{}
			withPositions = crownJobReconciler
			withPositions.Visibility = visibility
		})

		reconcileWith := func(r *crownjob.Reconciler, cj *clv1alpha2.CrownJob) {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
			ExpectWithOffset(1, err).ToNot(HaveOccurred())
		}

		It("Should be the position of its workload in the ClusterQueue plus 1, and nil once admitted", func() {
			cjA, jobA := waitingJob("inst-rb")
			cjB, jobB := waitingJob("inst-rb")
			visibility.set(forge.CrownJobQueueName(reasonWorkspaceB), jobB, jobA)
			reconcileWith(&withPositions, cjA)
			reconcileWith(&withPositions, cjB)
			Expect(crownJobStatus(cjA).QueuePosition).To(Equal(ptr.To[int32](2)))
			Expect(crownJobStatus(cjB).QueuePosition).To(Equal(ptr.To[int32](1)))
			Expect(crownJobStatus(cjA).Reason).To(Equal(crownjob.ReasonInQueue), "the message does not change")

			By("Admitted: no position")
			admit(jobB)
			visibility.set(forge.CrownJobQueueName(reasonWorkspaceB), jobA)
			reconcileWith(&withPositions, cjB)
			Expect(crownJobStatus(cjB).Phase).To(Equal(clv1alpha2.CrownJobPhaseStarting))
			Expect(crownJobStatus(cjB).QueuePosition).To(BeNil())
			reconcileWith(&withPositions, cjA)
			Expect(crownJobStatus(cjA).QueuePosition).To(Equal(ptr.To[int32](1)))

			By("Not among the waiting workloads (e.g., Kueue has not seen it yet): no position")
			visibility.set(forge.CrownJobQueueName(reasonWorkspaceB))
			reconcileWith(&withPositions, cjA)
			Expect(crownJobStatus(cjA).QueuePosition).To(BeNil())
		})

		It("Should be nil, with no error, when the visibility API cannot be read, and come back when it can", func() {
			cj, job := waitingJob("inst-rb")
			visibility.set(forge.CrownJobQueueName(reasonWorkspaceB), job)
			reconcileWith(&withPositions, cj)
			Expect(crownJobStatus(cj).QueuePosition).To(Equal(ptr.To[int32](1)))

			for _, err := range []error{
				kerrors.NewServiceUnavailable("the server is currently unable to handle the request"),
				kerrors.NewNotFound(visibilityResource("clusterqueue"), "crownlabs-ws-rb"),
				kerrors.NewForbidden(visibilityResource("clusterqueues/pendingworkloads"), "crownlabs-ws-rb", nil),
				context.DeadlineExceeded,
			} {
				visibility.err = err
				result, rerr := withPositions.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
				Expect(rerr).ToNot(HaveOccurred())
				Expect(result).To(Equal(ctrl.Result{RequeueAfter: 30 * time.Second}))
				Expect(crownJobStatus(cj).QueuePosition).To(BeNil(), err.Error())
				Expect(crownJobStatus(cj).Reason).To(Equal(crownjob.ReasonInQueue))
			}
			visibility.err = nil
			reconcileWith(&withPositions, cj)
			Expect(crownJobStatus(cj).QueuePosition).To(Equal(ptr.To[int32](1)))
		})
	})

	Describe("The cache of the job queues", func() {
		It("Should read once for many reconciles, again after a change of a ClusterQueue or after its time to live", func() {
			reader := &countingReader{Reader: k8sClient}
			visibility := &fakeVisibility{}
			cached := crownJobReconciler
			cached.APIReader = reader
			cached.Visibility = visibility
			cached.QueueCache = crownjob.NewQueueCache(time.Second, testClock)

			var cjs []*clv1alpha2.CrownJob
			var jobs []*batchv1.Job
			for range 3 {
				cj, job := waitingJob("inst-rb")
				cjs, jobs = append(cjs, cj), append(jobs, job)
			}
			visibility.set(forge.CrownJobQueueName(reasonWorkspaceB), jobs...)
			reconcileAll := func() {
				for _, cj := range cjs {
					_, err := cached.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cj)})
					ExpectWithOffset(1, err).ToNot(HaveOccurred())
				}
			}
			reads := func() []int {
				return []int{int(reader.cohortGets.Load()), int(reader.queueLists.Load()), visibility.count()}
			}

			reconcileAll()
			Expect(reads()).To(Equal([]int{1, 1, 1}), "three reconciles, one read of each")
			for i, cj := range cjs {
				Expect(crownJobStatus(cj).QueuePosition).To(Equal(ptr.To(int32(i + 1))))
			}

			By("A change of a ClusterQueue: read again")
			cached.QueueCache.Invalidate()
			reconcileAll()
			Expect(reads()).To(Equal([]int{2, 2, 2}))

			By("After the time to live: read again")
			testClock.Step(1100 * time.Millisecond)
			reconcileAll()
			Expect(reads()).To(Equal([]int{3, 3, 3}))

			By("A failure of the visibility API is kept 10 s, even across a change of a ClusterQueue")
			testClock.Step(1100 * time.Millisecond)
			visibility.err = kerrors.NewServiceUnavailable("unavailable")
			reconcileAll()
			Expect(reads()).To(Equal([]int{4, 4, 4}))
			cached.QueueCache.Invalidate()
			visibility.err = nil
			reconcileAll()
			Expect(reads()).To(Equal([]int{5, 5, 4}))
			Expect(crownJobStatus(cjs[0]).QueuePosition).To(BeNil())
			testClock.Step(10 * time.Second)
			reconcileAll()
			Expect(reads()).To(Equal([]int{6, 6, 5}))
			Expect(crownJobStatus(cjs[0]).QueuePosition).To(Equal(ptr.To[int32](1)))
		})
	})

	Describe("The REST reader of the visibility API", func() {
		var server *httptest.Server
		var hits atomic.Int32
		var handler http.HandlerFunc

		BeforeEach(func() {
			hits.Store(0)
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				hits.Add(1)
				handler(w, req)
			}))
		})
		AfterEach(func() { server.Close() })

		newReader := func(timeout time.Duration) crownjob.VisibilityReader {
			clientset, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			ExpectWithOffset(1, err).ToNot(HaveOccurred())
			return crownjob.NewVisibilityReader(clientset.CoreV1().RESTClient(), timeout)
		}
		status := func(code int, body string, header ...string) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				for i := 0; i+1 < len(header); i += 2 {
					w.Header().Set(header[i], header[i+1])
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(body))
			}
		}

		It("Should GET the pending workloads of the ClusterQueue and decode them", func() {
			var path string
			handler = func(w http.ResponseWriter, req *http.Request) {
				path = req.Method + " " + req.URL.Path
				status(http.StatusOK, `{"kind":"PendingWorkloadsSummary","apiVersion":"visibility.kueue.x-k8s.io/v1beta2","metadata":{},"items":[
					{"metadata":{"name":"job-a-1","namespace":"tenant-m1","ownerReferences":[{"apiVersion":"batch/v1","kind":"Job","name":"a","uid":"uid-a"}]},
					 "priority":0,"localQueueName":"crownlabs-ws","positionInClusterQueue":0,"positionInLocalQueue":0}]}`)(w, req)
			}
			workloads, err := newReader(time.Second).PendingWorkloads(ctx, "crownlabs-ws")
			Expect(err).ToNot(HaveOccurred())
			Expect(path).To(Equal("GET /apis/visibility.kueue.x-k8s.io/v1beta2/clusterqueues/crownlabs-ws/pendingworkloads"))
			Expect(workloads).To(Equal([]forge.KueuePendingWorkload{{Namespace: "tenant-m1", Name: "job-a-1", JobUID: "uid-a", JobName: "a"}}))
		})

		type failureCase struct {
			handler func() http.HandlerFunc
			check   func(error) bool
		}
		DescribeTable("Should return an error, once, when the API cannot be read",
			func(c failureCase) {
				handler = c.handler()
				started := time.Now()
				_, err := newReader(300*time.Millisecond).PendingWorkloads(ctx, "crownlabs-ws")
				Expect(err).To(HaveOccurred())
				Expect(c.check(err)).To(BeTrue(), err.Error())
				Expect(time.Since(started)).To(BeNumerically("<", 2*time.Second))
				Expect(hits.Load()).To(Equal(int32(1)), "never retried")
			},
			Entry("404: no such ClusterQueue, or no visibility API", failureCase{
				handler: func() http.HandlerFunc {
					return status(http.StatusNotFound, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
				},
				check: kerrors.IsNotFound}),
			Entry("403: no permission, with the message of the API server", failureCase{
				handler: func() http.HandlerFunc {
					return status(http.StatusForbidden, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,
						"message":"clusterqueues.visibility.kueue.x-k8s.io \"crownlabs-ws\" is forbidden: User \"system:serviceaccount:crownlabs:operator\" cannot get resource \"clusterqueues/pendingworkloads\""}`)
				},
				check: func(err error) bool {
					return kerrors.IsForbidden(err) && strings.Contains(err.Error(), `cannot get resource "clusterqueues/pendingworkloads"`)
				}}),
			Entry("503 with Retry-After: the visibility server is down", failureCase{
				handler: func() http.HandlerFunc {
					return status(http.StatusServiceUnavailable, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"ServiceUnavailable","code":503}`,
						"Retry-After", "1")
				},
				check: kerrors.IsServiceUnavailable}),
			Entry("a timeout", failureCase{
				handler: func() http.HandlerFunc {
					return func(w http.ResponseWriter, req *http.Request) {
						select {
						case <-time.After(3 * time.Second):
						case <-req.Context().Done():
						}
						status(http.StatusOK, `{"items":[]}`)(w, req)
					}
				},
				check: func(err error) bool { return strings.Contains(err.Error(), "context deadline exceeded") }}),
			Entry("a body that is not a list of pending workloads", failureCase{
				handler: func() http.HandlerFunc { return status(http.StatusOK, `{"items":{"a":1}}`) },
				check:   func(err error) bool { return strings.Contains(err.Error(), "failed decoding the pending workloads") }}),
		)
	})

	Describe("The watches of the CrownJob controller", func() {
		It("Should reconcile every waiting job when a job queue changes, its status included, and forget the cached queue state", func() {
			By("Jobs waiting in two workspaces, and one admitted")
			waitA, _ := waitingJob("inst-ra")
			waitB, _ := waitingJob("inst-rb")
			admitted, jobAdmitted := waitingJob("inst-rb")
			admit(jobAdmitted)
			reconcileCrownJob(admitted)

			watcher, collect := startWatches()
			name := func(cj *clv1alpha2.CrownJob) string { return cj.Namespace + "/" + cj.Name }
			Expect(collect()).To(HaveKey(name(waitA)), "the initial list enqueues everything")

			By("A status-only change of the ClusterQueue of ws-ra: every job waiting in any job queue, and the cache is invalidated")
			visibility := &fakeVisibility{}
			_, err := watcher.QueueCache.Positions(ctx, reasonWorkspaceA, visibility)
			Expect(err).ToNot(HaveOccurred())
			queue := &unstructured.Unstructured{}
			queue.SetGroupVersionKind(forge.KueueClusterQueueGVK)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: forge.CrownJobQueueName(reasonWorkspaceA)}, queue)).To(Succeed())
			generation := queue.GetGeneration()
			Expect(unstructured.SetNestedField(queue.Object, int64(7), "status", "pendingWorkloads")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, queue)).To(Succeed())
			Expect(queue.GetGeneration()).To(Equal(generation), "the spec did not change")
			got := collect()
			Expect(got).To(HaveKey(name(waitA)))
			Expect(got).To(HaveKey(name(waitB)), "a job of another workspace")
			Expect(got).ToNot(HaveKey(name(admitted)))
			for key := range got {
				var job batchv1.Job
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: tenantNamespace, Name: key[len(tenantNamespace)+1:]}, &job)).To(Succeed())
				Expect(ptr.Deref(job.Spec.Suspend, false)).To(BeTrue(), key)
			}
			_, err = watcher.QueueCache.Positions(ctx, reasonWorkspaceA, visibility)
			Expect(err).ToNot(HaveOccurred())
			Expect(visibility.count()).To(Equal(2), "read again after the change, though its time to live is 1 h")
		})
	})

})

// startWatches starts a manager with the watches of the CrownJob controller and a reconciler that records the requests
// (stopped when the spec ends). It returns the CrownJob reconciler of the watches (its cache lives 1 h) and a function
// returning the names of the requests received until none arrives for 1 s.
func startWatches() (watcher *crownjob.Reconciler, collect func() map[string]bool) {
	requests := make(chan reconcile.Request, 10000)
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{Scheme: scheme.Scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
	// The watch on the Deployments of the instances lists the CrownJobs through this field index, as in the operator.
	ExpectWithOffset(1, crownjob.IndexCrownJobsByInstance(ctx, mgr.GetFieldIndexer())).To(Succeed())
	copied := crownJobReconciler
	watcher = &copied
	watcher.Client = mgr.GetClient()
	watcher.QueueCache = crownjob.NewQueueCache(time.Hour, testClock)
	ExpectWithOffset(1, watcher.SetupWithManagerFor(mgr, reconcile.Func(func(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
		requests <- req
		return reconcile.Result{}, nil
	}))).To(Succeed())
	managerCtx, stop := context.WithCancel(ctx)
	DeferCleanup(stop)
	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(managerCtx)).To(Succeed())
	}()
	collect = func() map[string]bool {
		names := map[string]bool{}
		for {
			select {
			case req := <-requests:
				names[req.Namespace+"/"+req.Name] = true
			case <-time.After(time.Second):
				return names
			}
		}
	}
	return watcher, collect
}

// visibilityResource returns a resource of the visibility API of Kueue, for the errors of the fake reader.
func visibilityResource(resourceName string) schema.GroupResource {
	return schema.GroupResource{Group: forge.KueueVisibilityGroup, Resource: resourceName}
}
