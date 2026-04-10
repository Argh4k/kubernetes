/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queueing

import (
	"context"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testutils "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

// blockingFilterPlugin blocks the Filter call until unblocked, then returns an error to trigger handleSchedulingFailure.
type blockingFilterPlugin struct {
	fh            fwk.Handle
	blockCh       chan struct{}
	reachedFilter chan struct{}
	once          sync.Once
	failedOnce    bool
	mu            sync.Mutex
}

func (*blockingFilterPlugin) Name() string {
	return "blockingFilterPlugin"
}

func (p *blockingFilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pod.Name != "victim-pod" {
		return fwk.NewStatus(fwk.Success)
	}

	p.mu.Lock()
	if p.failedOnce {
		p.mu.Unlock()
		return fwk.NewStatus(fwk.Success)
	}
	p.failedOnce = true
	p.mu.Unlock()

	p.once.Do(func() {
		close(p.reachedFilter)
	})
	<-p.blockCh
	return fwk.NewStatus(fwk.Unschedulable, "simulated precondition failure (UID mismatch) during filter")
}

func TestInFlightPodLeak(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerQueueingHints, true)

	blockCh := make(chan struct{})
	reachedFilter := make(chan struct{})
	defer close(blockCh)

	registry := frameworkruntime.Registry{
		"blockingFilterPlugin": func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
			return &blockingFilterPlugin{fh: fh, blockCh: blockCh, reachedFilter: reachedFilter}, nil
		},
	}

	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				Filter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: "blockingFilterPlugin"},
					},
				},
			},
		}}})

	// Start the test scheduler with 0 backoff to simplify test logic
	testCtx := testutils.InitTestSchedulerWithOptions(
		t,
		testutils.InitTestAPIServer(t, "in-flight-leak", nil),
		0,
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(0),
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry),
	)
	testutils.SyncSchedulerInformerFactory(testCtx)

	// Run scheduler asynchronously
	go testCtx.Scheduler.Run(testCtx.Ctx)

	cs, ns, ctx := testCtx.ClientSet, testCtx.NS.Name, testCtx.Ctx

	// 1. Create a Node
	node := st.MakeNode().Name("test-node").Obj()
	if _, err := cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Node %q: %v", node.Name, err)
	}

	// 2. Create the target Pod to schedule
	podName := "victim-pod"
	pod := st.MakePod().Namespace(ns).Name(podName).Container(imageutils.GetPauseImageName()).Obj()
	createdPod, err := cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Pod %q: %v", pod.Name, err)
	}

	// Keep track of our original attempt UID
	originalUID := createdPod.UID

	// 3. Wait for the pod to be popped and enter the in-flight list (reaching the Filter phase)
	t.Logf("Waiting for pod to enter Filter phase...")

	select {
	case <-reachedFilter:
		t.Logf("Pod successfully reached the Filter phase (in-flight)!")
	case <-time.After(15 * time.Second):
		t.Fatalf("Timed out waiting for pod to reach Filter phase!")
	}

	// 4. At this point the scheduling routine is blocked inside our custom Filter plugin.
	// We now delete the Pod from the API server:
	if err := cs.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Failed to delete Pod: %v", err)
	}

	// 5. Recreate a new Pod with the EXACT SAME name but unschedulable so it doesn't get assigned to a node immediately:
	newPod := st.MakePod().Namespace(ns).Name(podName).NodeSelector(map[string]string{"impossible": "node"}).Container(imageutils.GetPauseImageName()).Obj()
	createdNewPod, err := cs.CoreV1().Pods(ns).Create(ctx, newPod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to recreate Pod: %v", err)
	}

	// Make sure our new pod has a DIFFERENT UID and is seen by the system:
	if createdNewPod.UID == originalUID {
		t.Fatalf("Expected recreated pod to have a different UID")
	}

	// 6. Wait for the Informer cache specifically to observe the newly recreated pod
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, false, func(ctx context.Context) (bool, error) {
		podLister := testCtx.Scheduler.Profiles[v1.DefaultSchedulerName].SharedInformerFactory().Core().V1().Pods().Lister()
		cachedPod, err := podLister.Pods(ns).Get(podName)
		if err != nil {
			return false, nil
		}
		return cachedPod.UID == createdNewPod.UID, nil
	})
	if err != nil {
		t.Fatalf("Failed waiting for Informer cache to update to new pod UID: %v", err)
	}

	// 7. Release the blocked filter plugin. This will cause the framework to fail to plugin and execute handleSchedulingFailure().
	// Because of our race condition, handleSchedulingFailure() will look up by Name, retrieve the NEW pod (createdNewPod.UID),
	// and call Done(createdNewPod.UID) instead of Done(originalUID).
	blockCh <- struct{}{}

	// 8. Wait a short moment for the failure handling routine to complete
	time.Sleep(1 * time.Second)

	// 9. Create two additional pods to generate new events in the scheduling queue:
	extraPod1 := st.MakePod().Namespace(ns).Name("extra-pod-1").Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, extraPod1, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create first extra Pod: %v", err)
	}

	extraPod2 := st.MakePod().Namespace(ns).Name("extra-pod-2").Container(imageutils.GetPauseImageName()).Obj()
	if _, err := cs.CoreV1().Pods(ns).Create(ctx, extraPod2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create second extra Pod: %v", err)
	}

	// 10. Wait for both extra pods to be successfully scheduled onto the node:
	if err := testutils.WaitForPodToSchedule(ctx, cs, extraPod1); err != nil {
		t.Fatalf("Failed to schedule first extra pod: %v", err)
	}
	if err := testutils.WaitForPodToSchedule(ctx, cs, extraPod2); err != nil {
		t.Fatalf("Failed to schedule second extra pod: %v", err)
	}

	// Give the event tracker a brief moment to process the scheduled pod events (normally they should be removed by now!)
	time.Sleep(1 * time.Second)

	inFlightPods := testCtx.Scheduler.SchedulingQueue.InFlightPods()
	inFlightEvents := testCtx.Scheduler.SchedulingQueue.InFlightEvents()
	var actualUIDs []string
	foundLeakedUID := false
	for _, p := range inFlightPods {
		actualUIDs = append(actualUIDs, string(p.UID))
		if p.UID == originalUID {
			foundLeakedUID = true
			break
		}
	}

	t.Logf("Actual inFlightPods UIDs: %v, expected to contain originalUID: %s", actualUIDs, originalUID)

	if foundLeakedUID {
		t.Fatalf("Original pod UID %q was found in inFlightPods, but it should have been removed (Proving the memory leak), len(inFlightPods)=%d, len(inFlightEvents)=%d", originalUID, len(inFlightPods), len(inFlightEvents))
	}
}
