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

package main

import (
	"flag"
	"fmt"
	"time"
	_ "time/tzdata" // The time zones of the job windows: the image of the operator has no time zone database.

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	ctrlcommon "github.com/netgroup-polito/CrownLabs/operators/pkg/controller/common"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/controller/crownjob"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/forge"
	"github.com/netgroup-polito/CrownLabs/operators/pkg/utils"
)

var (
	crownJobRuntimeClasses          string
	crownJobDefaultMaxRunTime       string
	crownJobLogHelperImage          string
	crownJobFinishedJobTTL          string
	crownJobCapacity                string
	crownJobTimeZone                string
	maxConcurrentCrownJobReconciles int
)

const (
	crownJobCtrl = "CrownJob"
)

func init() {
	flag.StringVar(&crownJobRuntimeClasses, "crownjob-runtime-classes", "",
		"The RuntimeClass of the jobs that request a given resource, as a comma-separated list of resource=runtimeClass (e.g., nvidia.com/gpu=nvidia)")
	flag.StringVar(&crownJobDefaultMaxRunTime, "crownjob-default-max-run-time", "2h",
		"The maximum run time of a job when neither its Template nor its Workspace set one, as a number followed by m, h or d")
	flag.StringVar(&crownJobLogHelperImage, "crownjob-log-helper-image", "registry.k8s.io/kubectl:v1.34.1",
		"The image of the Jobs that save the log of each job pod into its run folder (it must contain /go-runner and /bin/kubectl)")
	flag.StringVar(&crownJobFinishedJobTTL, "crownjob-finished-job-ttl", "10m",
		"How long the Job of a CrownJob, and each of its log helpers, is kept after it ends, with its pods, as a number followed by m, h or d (the CrownJob is kept)")
	flag.StringVar(&crownJobCapacity, "crownjob-capacity", "",
		"The most all the jobs may use at the same time, as a comma-separated list of resource=quantity (e.g., cpu=64,memory=256Gi,nvidia.com/gpu=8): "+
			"the guarantees of the workspaces are taken from it, and the rest is shared. If empty, it is unlimited: every workspace can use up to its maxAtOnce, "+
			"and the guarantees have no effect")
	flag.StringVar(&crownJobTimeZone, "crownjob-time-zone", "UTC",
		"The IANA time zone of the job windows of the workspaces whose jobPolicy sets none (e.g., Europe/Rome)")
	flag.IntVar(&maxConcurrentCrownJobReconciles, "crownjob-max-concurrent-reconciles", 1,
		"The maximum number of concurrent Reconciles which can be run for the CrownJob controllers")
}

// setupCrownJob configures the CrownJob controller, the controller keeping the Instances while their jobs run,
// and the controller of the job queues of the Workspaces.
func setupCrownJob(mgr manager.Manager, targetLabel ctrlcommon.KVLabel) error {
	runtimeClasses, err := forge.MapFromKVString(crownJobRuntimeClasses)
	if err != nil {
		return fmt.Errorf("invalid value of --crownjob-runtime-classes: %w", err)
	}
	defaultMaxRunTime, err := utils.ParseDurationWithDays(crownJobDefaultMaxRunTime)
	if err != nil {
		return fmt.Errorf("invalid value of --crownjob-default-max-run-time: %w", err)
	}
	finishedJobTTL, err := utils.ParseDurationWithDays(crownJobFinishedJobTTL)
	if err != nil {
		return fmt.Errorf("invalid value of --crownjob-finished-job-ttl: %w", err)
	}
	capacity, err := parseCapacity(crownJobCapacity)
	if err != nil {
		return fmt.Errorf("invalid value of --crownjob-capacity: %w", err)
	}
	timeZone, err := time.LoadLocation(crownJobTimeZone)
	if err != nil || crownJobTimeZone == "Local" {
		return fmt.Errorf("invalid value of --crownjob-time-zone: %q is not an IANA time zone (%w)", crownJobTimeZone, err)
	}
	if err := checkKueueKinds(mgr); err != nil {
		return err
	}
	// The queue positions are read from the visibility API of Kueue with plain REST calls.
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("failed creating the client of the visibility API of Kueue: %w", err)
	}

	if err := (&crownjob.Reconciler{
		Client:            mgr.GetClient(),
		APIReader:         mgr.GetAPIReader(),
		TargetLabel:       targetLabel,
		EventsRecorder:    mgr.GetEventRecorderFor(crownJobCtrl),
		RuntimeClasses:    runtimeClasses,
		DefaultMaxRunTime: defaultMaxRunTime,
		LogHelperImage:    crownJobLogHelperImage,
		FinishedJobTTL:    finishedJobTTL,
		Capacity:          capacity,
		DefaultTimeZone:   timeZone,
		Clock:             clock.RealClock{},
		Visibility:        crownjob.NewVisibilityReader(clientset.CoreV1().RESTClient(), crownjob.VisibilityTimeout),
		QueueCache:        crownjob.NewQueueCache(crownjob.QueueCacheTTL, clock.RealClock{}),
	}).SetupWithManager(mgr, maxConcurrentCrownJobReconciles); err != nil {
		return err
	}

	if err := (&crownjob.InstanceReconciler{
		Client:         mgr.GetClient(),
		APIReader:      mgr.GetAPIReader(),
		TargetLabel:    targetLabel,
		EventsRecorder: mgr.GetEventRecorderFor(crownJobCtrl),
	}).SetupWithManager(mgr, maxConcurrentCrownJobReconciles); err != nil {
		return err
	}

	return (&crownjob.QueueReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		TargetLabel:     targetLabel,
		EventsRecorder:  mgr.GetEventRecorderFor(crownJobCtrl),
		Capacity:        capacity,
		DefaultTimeZone: timeZone,
		Clock:           clock.RealClock{},
	}).SetupWithManager(mgr, maxConcurrentCrownJobReconciles)
}

// parseCapacity parses the job capacity, a comma-separated list of resource=quantity.
func parseCapacity(value string) (corev1.ResourceList, error) {
	pairs, err := forge.MapFromKVString(value)
	if err != nil {
		return nil, err
	}
	capacity := corev1.ResourceList{}
	for name, raw := range pairs {
		quantity, err := resource.ParseQuantity(raw)
		if err != nil {
			return nil, fmt.Errorf("the quantity of %s: %w", name, err)
		}
		if quantity.Sign() < 0 {
			return nil, fmt.Errorf("the quantity of %s is negative", name)
		}
		capacity[corev1.ResourceName(name)] = quantity
	}
	return capacity, nil
}

// checkKueueKinds returns an error if the cluster does not serve the Kueue kinds used by the CrownJob controllers.
func checkKueueKinds(mgr manager.Manager) error {
	for _, gvk := range []schema.GroupVersionKind{
		forge.KueueClusterQueueGVK, forge.KueueLocalQueueGVK, forge.KueueResourceFlavorGVK,
		forge.KueueWorkloadPriorityClassGVK, forge.KueueWorkloadGVK, forge.KueueCohortGVK,
	} {
		if _, err := mgr.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
			return fmt.Errorf("the CrownJob controller needs Kueue, but the cluster does not serve %s %s (%w): install Kueue, or set --enable-crownjob=false",
				gvk.Kind, gvk.GroupVersion(), err)
		}
	}
	return nil
}
