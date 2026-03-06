/*
Copyright The Kubernetes Authors.

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

package preemption_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	schedulingv1alpha1 "k8s.io/api/scheduling/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	apicache "k8s.io/kubernetes/pkg/scheduler/backend/api_cache"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

type mockCandidate struct {
	victims *extenderv1.Victims
	name    string
}

func (c *mockCandidate) Victims() *extenderv1.Victims {
	return c.victims
}

func (c *mockCandidate) Name() string {
	return c.name
}

type fakePodActivator struct {
	activatedPods map[string]*v1.Pod
	mu            *sync.RWMutex
}

func (f *fakePodActivator) Activate(logger klog.Logger, pods map[string]*v1.Pod) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, pod := range pods {
		f.activatedPods[name] = pod
	}
}

type fakePodNominator struct {
	// embed it so that we can only override NominatedPodsForNode
	internalqueue.SchedulingQueue

	// fakePodNominator doesn't respond to NominatedPodsForNode() until the channel is closed.
	requestStopper chan struct{}
}

func (f *fakePodNominator) NominatedPodsForNode(nodeName string) []fwk.PodInfo {
	<-f.requestStopper
	return nil
}

func TestPrepareCandidate(t *testing.T) {
	var (
		node1Name            = "node1"
		defaultSchedulerName = "default-scheduler"
	)
	condition := v1.PodCondition{
		Type:    v1.DisruptionTarget,
		Status:  v1.ConditionTrue,
		Reason:  v1.PodReasonPreemptionByScheduler,
		Message: fmt.Sprintf("%s: preempting to accommodate a higher priority pod", defaultSchedulerName),
	}

	var (
		victim1 = st.MakePod().Name("victim1").UID("victim1").
			Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()

		notFoundVictim1 = st.MakePod().Name("not-found-victim").UID("victim1").
				Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
				Obj()

		failVictim = st.MakePod().Name("fail-victim").UID("victim1").
				Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
				Obj()

		victim2 = st.MakePod().Name("victim2").UID("victim2").
			Node(node1Name).SchedulerName(defaultSchedulerName).Priority(50000).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()

		victim1WithMatchingCondition = st.MakePod().Name("victim1").UID("victim1").
						Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
						Conditions([]v1.PodCondition{condition}).
						Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
						Obj()

		failVictim1WithMatchingCondition = st.MakePod().Name("fail-victim").UID("victim1").
							Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
							Conditions([]v1.PodCondition{condition}).
							Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
							Obj()

		preemptor = st.MakePod().Name("preemptor").UID("preemptor").
				SchedulerName(defaultSchedulerName).Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
				Obj()

		errDeletePodFailed   = errors.New("delete pod failed")
		errPatchStatusFailed = errors.New("patch pod status failed")
	)

	victimWithDeletionTimestamp := victim1.DeepCopy()
	victimWithDeletionTimestamp.Name = "victim1-with-deletion-timestamp"
	victimWithDeletionTimestamp.UID = "victim1-with-deletion-timestamp"
	victimWithDeletionTimestamp.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-100 * time.Second)}
	victimWithDeletionTimestamp.Finalizers = []string{"test"}

	tests := []struct {
		name          string
		nodeNames     []string
		mockCandidate preemption.Candidate
		preemptor     *v1.Pod
		testPods      []*v1.Pod
		// expectedDeletedPod is the pod name that is expected to be deleted.
		//
		// You can set multiple pod name if there're multiple possibilities.
		// Both empty and "" means no pod is expected to be deleted.
		expectedDeletedPod    []string
		expectedDeletionError bool
		expectedPatchError    bool
		// Only compared when async preemption is disabled.
		expectedStatus *fwk.Status
		// Only compared when async preemption is enabled.
		expectedPreemptingMap sets.Set[types.UID]
		expectedActivatedPods map[string]*v1.Pod
	}{
		{
			name: "no victims",
			mockCandidate: &mockCandidate{
				victims: &extenderv1.Victims{},
			},
			preemptor: preemptor,
			testPods: []*v1.Pod{
				victim1,
			},
			nodeNames:      []string{node1Name},
			expectedStatus: nil,
		},
		{
			name: "one victim without condition",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
					},
				},
			},
			preemptor: preemptor,
			testPods: []*v1.Pod{
				victim1,
			},
			nodeNames:             []string{node1Name},
			expectedDeletedPod:    []string{"victim1"},
			expectedStatus:        nil,
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
		},
		{
			name: "one victim, but victim is already being deleted",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victimWithDeletionTimestamp,
					},
				},
			},
			preemptor: preemptor,
			testPods: []*v1.Pod{
				victimWithDeletionTimestamp,
			},
			nodeNames:      []string{node1Name},
			expectedStatus: nil,
		},
		{
			name: "one victim, but victim is already deleted",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						notFoundVictim1,
					},
				},
			},
			preemptor:             preemptor,
			testPods:              []*v1.Pod{},
			nodeNames:             []string{node1Name},
			expectedStatus:        nil,
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
		},
		{
			name: "one victim with same condition",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1WithMatchingCondition,
					},
				},
			},
			preemptor: preemptor,
			testPods: []*v1.Pod{
				victim1WithMatchingCondition,
			},
			nodeNames:             []string{node1Name},
			expectedDeletedPod:    []string{"victim1"},
			expectedStatus:        nil,
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
		},
		{
			name: "one victim, not-found victim error is ignored when patching",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1WithMatchingCondition,
					},
				},
			},
			preemptor:             preemptor,
			testPods:              []*v1.Pod{},
			nodeNames:             []string{node1Name},
			expectedDeletedPod:    []string{"victim1"},
			expectedStatus:        nil,
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
		},
		{
			name: "one victim, but pod deletion failed",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						failVictim1WithMatchingCondition,
					},
				},
			},
			preemptor:             preemptor,
			testPods:              []*v1.Pod{},
			expectedDeletionError: true,
			nodeNames:             []string{node1Name},
			expectedStatus:        fwk.AsStatus(errDeletePodFailed),
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
			expectedActivatedPods: map[string]*v1.Pod{preemptor.Name: preemptor},
		},
		{
			name: "one victim, not-found victim error is ignored when deleting",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
					},
				},
			},
			preemptor:             preemptor,
			testPods:              []*v1.Pod{},
			nodeNames:             []string{node1Name},
			expectedDeletedPod:    []string{"victim1"},
			expectedStatus:        nil,
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
		},
		{
			name: "one victim, but patch pod failed",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						failVictim,
					},
				},
			},
			preemptor:             preemptor,
			testPods:              []*v1.Pod{},
			expectedPatchError:    true,
			nodeNames:             []string{node1Name},
			expectedStatus:        fwk.AsStatus(errPatchStatusFailed),
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
			expectedActivatedPods: map[string]*v1.Pod{preemptor.Name: preemptor},
		},
		{
			name: "two victims without condition, one passes successfully and the second fails",

			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						failVictim,
						victim2,
					},
				},
			},
			preemptor: preemptor,
			testPods: []*v1.Pod{
				victim1,
			},
			nodeNames:          []string{node1Name},
			expectedPatchError: true,
			expectedDeletedPod: []string{
				"victim2",
				// The first victim could fail before the deletion of the second victim happens,
				// which results in the second victim not being deleted.
				"",
			},
			expectedStatus:        fwk.AsStatus(errPatchStatusFailed),
			expectedPreemptingMap: sets.New(types.UID("preemptor")),
			expectedActivatedPods: map[string]*v1.Pod{preemptor.Name: preemptor},
		},
	}

	for _, asyncPreemptionEnabled := range []bool{true, false} {
		for _, asyncAPICallsEnabled := range []bool{true, false} {
			for _, tt := range tests {
				t.Run(fmt.Sprintf("%v (Async preemption enabled: %v, Async API calls enabled: %v)", tt.name, asyncPreemptionEnabled, asyncAPICallsEnabled), func(t *testing.T) {
					metrics.Register()
					logger, ctx := ktesting.NewTestContext(t)
					ctx, cancel := context.WithCancel(ctx)
					defer cancel()

					nodes := make([]*v1.Node, len(tt.nodeNames))
					for i, nodeName := range tt.nodeNames {
						nodes[i] = st.MakeNode().Name(nodeName).Capacity(veryLargeRes).Obj()
					}
					registeredPlugins := append([]tf.RegisterPluginFunc{
						tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New)},
						tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
					)
					var objs []runtime.Object
					for _, pod := range tt.testPods {
						objs = append(objs, pod)
					}

					mu := &sync.RWMutex{}
					deletedPods := sets.New[string]()
					deletionFailure := false // whether any request to delete pod failed
					patchFailure := false    // whether any request to patch pod status failed

					cs := clientsetfake.NewClientset(objs...)
					cs.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
						mu.Lock()
						defer mu.Unlock()
						name := action.(clienttesting.DeleteAction).GetName()
						if name == "fail-victim" {
							deletionFailure = true
							return true, nil, errDeletePodFailed
						}
						// fake clientset does not return an error for not-found pods, so we simulate it here.
						if name == "not-found-victim" {
							// Simulate a not-found error.
							return true, nil, apierrors.NewNotFound(v1.Resource("pods"), name)
						}

						deletedPods.Insert(name)
						return true, nil, nil
					})

					cs.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
						mu.Lock()
						defer mu.Unlock()
						if action.(clienttesting.PatchAction).GetName() == "fail-victim" {
							patchFailure = true
							return true, nil, errPatchStatusFailed
						}
						// fake clientset does not return an error for not-found pods, so we simulate it here.
						if action.(clienttesting.PatchAction).GetName() == "not-found-victim" {
							return true, nil, apierrors.NewNotFound(v1.Resource("pods"), "not-found-victim")
						}
						return true, nil, nil
					})

					informerFactory := informers.NewSharedInformerFactory(cs, 0)
					eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
					fakeActivator := &fakePodActivator{activatedPods: make(map[string]*v1.Pod), mu: mu}

					// Note: NominatedPodsForNode is called at the beginning of the goroutine in any case.
					// fakePodNominator can delay the response of NominatedPodsForNode until the channel is closed,
					// which allows us to test the preempting map before the goroutine does nothing yet.
					requestStopper := make(chan struct{})
					nominator := &fakePodNominator{
						SchedulingQueue: internalqueue.NewSchedulingQueue(nil, informerFactory),
						requestStopper:  requestStopper,
					}
					var apiDispatcher *apidispatcher.APIDispatcher
					if asyncAPICallsEnabled {
						apiDispatcher = apidispatcher.New(cs, 16, apicalls.Relevances)
						apiDispatcher.Run(logger)
						defer apiDispatcher.Close()
					}

					fwk, err := tf.NewFramework(
						ctx,
						registeredPlugins, "",
						frameworkruntime.WithClientSet(cs),
						frameworkruntime.WithAPIDispatcher(apiDispatcher),
						frameworkruntime.WithLogger(logger),
						frameworkruntime.WithInformerFactory(informerFactory),
						frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
						frameworkruntime.WithPodsInPreBind(frameworkruntime.NewPodsInPreBindMap()),
						frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(tt.testPods, nodes)),
						frameworkruntime.WithPodNominator(nominator),
						frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
						frameworkruntime.WithPodActivator(fakeActivator),
					)
					if err != nil {
						t.Fatal(err)
					}
					informerFactory.Start(ctx.Done())
					informerFactory.WaitForCacheSync(ctx.Done())
					if asyncAPICallsEnabled {
						cache := internalcache.New(ctx, apiDispatcher)
						fwk.SetAPICacher(apicache.New(nil, cache))
					}

					executor := preemption.NewExecutor(fwk, feature.Features{EnableAsyncPreemption: asyncPreemptionEnabled})

					if asyncPreemptionEnabled {
						executor.ActuatePodPreemption(ctx, tt.mockCandidate.Name(), tt.mockCandidate.Victims(), tt.preemptor, "test-plugin")
						expectedPreempting := tt.expectedPreemptingMap.UnsortedList()
						for _, k := range expectedPreempting {
							if !executor.IsPodRunningPreemption(k) {
								t.Errorf("expected pod %v to be running preemption, got %v", k, executor.IsPodRunningPreemption(k))
								close(requestStopper)
								return
							}
						}
						// make the requests complete
						close(requestStopper)
					} else {
						close(requestStopper) // no need to stop requests
						status := executor.ActuatePodPreemption(ctx, tt.mockCandidate.Name(), tt.mockCandidate.Victims(), tt.preemptor, "test-plugin")
						if tt.expectedStatus == nil {
							if status != nil {
								t.Errorf("expect nil status, but got %v", status)
							}
						} else {
							if !cmp.Equal(status, tt.expectedStatus) {
								t.Errorf("expect status %v, but got %v", tt.expectedStatus, status)
							}
						}
					}

					var lastErrMsg string
					if err := wait.PollUntilContextTimeout(ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
						mu.RLock()
						defer mu.RUnlock()

						if executor.IsRunningAnyPreemption() {
							// The preempting map should be empty after the goroutine in all test cases.
							lastErrMsg = fmt.Sprintf("expected no preempting pods")
							return false, nil
						}

						if tt.expectedDeletionError != deletionFailure {
							lastErrMsg = fmt.Sprintf("expected deletion error %v, got %v", tt.expectedDeletionError, deletionFailure)
							return false, nil
						}
						if tt.expectedPatchError != patchFailure {
							lastErrMsg = fmt.Sprintf("expected patch error %v, got %v", tt.expectedPatchError, patchFailure)
							return false, nil
						}

						if asyncPreemptionEnabled {
							if diff := cmp.Diff(tt.expectedActivatedPods, fakeActivator.activatedPods); tt.expectedActivatedPods != nil && diff != "" {
								lastErrMsg = fmt.Sprintf("Unexpected activated pods (-want,+got):\n%s", diff)
								return false, nil
							}
							if tt.expectedActivatedPods == nil && len(fakeActivator.activatedPods) != 0 {
								lastErrMsg = fmt.Sprintf("expected no activated pods, got %v", fakeActivator.activatedPods)
								return false, nil
							}
						}

						if deletedPods.Len() > 1 {
							// For now, we only expect at most one pod to be deleted in all test cases.
							// If we need to test multiple pods deletion, we need to update the test table definition.
							return false, fmt.Errorf("expected at most one pod to be deleted, got %v", deletedPods.UnsortedList())
						}

						if len(tt.expectedDeletedPod) == 0 {
							if deletedPods.Len() != 0 {
								// When tt.expectedDeletedPod is empty, we expect no pod to be deleted.
								return false, fmt.Errorf("expected no pod to be deleted, got %v", deletedPods.UnsortedList())
							}
							// nothing further to check.
							return true, nil
						}

						found := false
						for _, podName := range tt.expectedDeletedPod {
							if deletedPods.Has(podName) ||
								// If podName is empty, we expect no pod to be deleted.
								(deletedPods.Len() == 0 && podName == "") {
								found = true
							}
						}
						if !found {
							lastErrMsg = fmt.Sprintf("expected pod %v to be deleted, but %v is deleted", strings.Join(tt.expectedDeletedPod, " or "), deletedPods.UnsortedList())
							return false, nil
						}

						return true, nil
					}); err != nil {
						t.Fatal(lastErrMsg)
					}
				})
			}
		}
	}
}

func TestPrepareCandidateAsyncSetsPreemptingSets(t *testing.T) {
	var (
		node1Name            = "node1"
		defaultSchedulerName = "default-scheduler"
	)

	var (
		victim1 = st.MakePod().Name("victim1").UID("victim1").
			Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()

		victim2 = st.MakePod().Name("victim2").UID("victim2").
			Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()

		preemptor = st.MakePod().Name("preemptor").UID("preemptor").
				SchedulerName(defaultSchedulerName).Priority(highPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
				Obj()
		testPods = []*v1.Pod{
			victim1,
			victim2,
		}
		nodeNames = []string{node1Name}
	)

	tests := []struct {
		name          string
		mockCandidate preemption.Candidate
		lastVictim    *v1.Pod
		preemptor     *v1.Pod
	}{
		{
			name: "no victims",
			mockCandidate: &mockCandidate{
				victims: &extenderv1.Victims{},
			},
			lastVictim: nil,
			preemptor:  preemptor,
		},
		{
			name: "one victim",
			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
					},
				},
			},
			lastVictim: victim1,
			preemptor:  preemptor,
		},
		{
			name: "two victims",
			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
						victim2,
					},
				},
			},
			lastVictim: victim2,
			preemptor:  preemptor,
		},
	}

	for _, asyncAPICallsEnabled := range []bool{true, false} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%v (Async API calls enabled: %v)", tt.name, asyncAPICallsEnabled), func(t *testing.T) {
				metrics.Register()
				logger, ctx := ktesting.NewTestContext(t)
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				nodes := make([]*v1.Node, len(nodeNames))
				for i, nodeName := range nodeNames {
					nodes[i] = st.MakeNode().Name(nodeName).Capacity(veryLargeRes).Obj()
				}
				registeredPlugins := append([]tf.RegisterPluginFunc{
					tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New)},
					tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				)
				var objs []runtime.Object
				for _, pod := range testPods {
					objs = append(objs, pod)
				}

				cs := clientsetfake.NewClientset(objs...)

				informerFactory := informers.NewSharedInformerFactory(cs, 0)
				eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})

				var apiDispatcher *apidispatcher.APIDispatcher
				if asyncAPICallsEnabled {
					apiDispatcher = apidispatcher.New(cs, 16, apicalls.Relevances)
					apiDispatcher.Run(logger)
					defer apiDispatcher.Close()
				}

				fwk, err := tf.NewFramework(
					ctx,
					registeredPlugins, "",
					frameworkruntime.WithClientSet(cs),
					frameworkruntime.WithAPIDispatcher(apiDispatcher),
					frameworkruntime.WithLogger(logger),
					frameworkruntime.WithInformerFactory(informerFactory),
					frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
					frameworkruntime.WithPodsInPreBind(frameworkruntime.NewPodsInPreBindMap()),
					frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(testPods, nodes)),
					frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
					frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				)
				if err != nil {
					t.Fatal(err)
				}
				informerFactory.Start(ctx.Done())
				if asyncAPICallsEnabled {
					cache := internalcache.New(ctx, apiDispatcher)
					fwk.SetAPICacher(apicache.New(nil, cache))
				}

				executor := preemption.NewExecutor(fwk, feature.Features{EnableAsyncPreemption: true})
				// preemptPodCallsCounter helps verify if the last victim pod gets preempted after other victims.
				preemptPodCallsCounter := 0
				preemptFunc := executor.PreemptPod
				exLock := sync.Mutex{}
				executor.PreemptPod = func(ctx context.Context, c preemption.Candidate, preemptor preemption.ExecutorPreemptor, victim *v1.Pod, pluginName string) error {
					// Verify contents of the sets: preempting and lastVictimsPendingPreemption before preemption of subsequent pods.
					exLock.Lock()
					preemptPodCallsCounter++

					victimCount := len(tt.mockCandidate.Victims().Pods)
					if victim.Name == tt.lastVictim.Name {
						if victimCount != preemptPodCallsCounter {
							t.Errorf("Expected PreemptPod for last victim %v to be called last (call no. %v), but it was called as no. %v", victim.Name, victimCount, preemptPodCallsCounter)
						}
						lastVictimName, _ := executor.LastVictim(tt.preemptor.UID)
						if tt.lastVictim.Name != lastVictimName {
							t.Errorf("Expected lastVictimsPendingPreemption map to contain victim %v for preemptor UID %v when preempting the last victim", tt.lastVictim.Name, tt.preemptor.UID)
						}
					} else {
						if preemptPodCallsCounter >= victimCount {
							t.Errorf("Expected PreemptPod for victim %v to be called earlier, but it was called as last - no. %v", victim.Name, preemptPodCallsCounter)
						}
						lastVictimName, _ := executor.LastVictim(tt.preemptor.UID)
						if lastVictimName != "" {
							t.Errorf("Expected lastVictimsPendingPreemption map to not contain values for preemptor UID %v when not preempting the last victim", tt.preemptor.UID)
						}
					}
					exLock.Unlock()

					return preemptFunc(ctx, c, preemptor, victim, pluginName)
				}

				if executor.IsRunningAnyPreemption() {
					t.Errorf("Expected preempting set to be empty before prepareCandidateAsync")
				}

				executor.ActuatePodPreemption(ctx, tt.mockCandidate.Name(), tt.mockCandidate.Victims(), tt.preemptor, "test-plugin")

				// Perform the checks when there are no victims left to preempt.
				t.Log("Waiting for async preemption goroutine to finish cleanup...")
				err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 2*time.Second, false, func(ctx context.Context) (bool, error) {
					// Check if the preemptor is removed from the ev.preempting set.
					return !executor.IsRunningAnyPreemption(), nil
				})
				if err != nil {
					t.Errorf("Timed out waiting for preemptingSet to become empty. %v", err)
				}

				lastVictimName, _ := executor.LastVictim(tt.preemptor.UID)
				if lastVictimName != "" {
					t.Errorf("Expected lastVictimsPendingPreemption map to not contain values for %v after completing preemption", tt.preemptor.UID)
				}
				if victimCount := len(tt.mockCandidate.Victims().Pods); victimCount != preemptPodCallsCounter {
					t.Errorf("Expected PreemptPod to be called %v times during prepareCandidateAsync but got %v", victimCount, preemptPodCallsCounter)
				}
			})
		}
	}
}

func TestAsyncPreemptionFailure(t *testing.T) {
	metrics.Register()
	var (
		node1Name            = "node1"
		defaultSchedulerName = "default-scheduler"
		failVictimNamePrefix = "fail-victim"
	)

	makePod := func(name string, priority int32) *v1.Pod {
		return st.MakePod().Name(name).UID(name).
			Node(node1Name).SchedulerName(defaultSchedulerName).Priority(priority).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()
	}

	preemptor := makePod("preemptor", highPriority)

	makeVictim := func(name string) *v1.Pod {
		return makePod(name, midPriority)
	}

	tests := []struct {
		name                                 string
		victims                              []*v1.Pod
		expectSuccessfulPreemption           bool
		expectPreemptionAttemptForLastVictim bool
	}{
		{
			name: "Failure with a single victim",
			victims: []*v1.Pod{
				makeVictim(failVictimNamePrefix),
			},
			expectSuccessfulPreemption:           false,
			expectPreemptionAttemptForLastVictim: true,
		},
		{
			name: "Success with a single victim",
			victims: []*v1.Pod{
				makeVictim("victim1"),
			},
			expectSuccessfulPreemption:           true,
			expectPreemptionAttemptForLastVictim: true,
		},
		{
			name: "Failure in first of three victims",
			victims: []*v1.Pod{
				makeVictim(failVictimNamePrefix),
				makeVictim("victim2"),
				makeVictim("victim3"),
			},
			expectSuccessfulPreemption:           false,
			expectPreemptionAttemptForLastVictim: false,
		},
		{
			name: "Failure in second of three victims",
			victims: []*v1.Pod{
				makeVictim("victim1"),
				makeVictim(failVictimNamePrefix),
				makeVictim("victim3"),
			},
			expectSuccessfulPreemption:           false,
			expectPreemptionAttemptForLastVictim: false,
		},
		{
			name: "Failure in first two of three victims",
			victims: []*v1.Pod{
				makeVictim(failVictimNamePrefix + "1"),
				makeVictim(failVictimNamePrefix + "2"),
				makeVictim("victim3"),
			},
			expectSuccessfulPreemption:           false,
			expectPreemptionAttemptForLastVictim: false,
		},
		{
			name: "Failure in third of three victims",
			victims: []*v1.Pod{
				makeVictim("victim1"),
				makeVictim("victim2"),
				makeVictim(failVictimNamePrefix),
			},
			expectSuccessfulPreemption:           false,
			expectPreemptionAttemptForLastVictim: true,
		},
		{
			name: "Success with three victims",
			victims: []*v1.Pod{
				makeVictim("victim1"),
				makeVictim("victim2"),
				makeVictim("victim3"),
			},
			expectSuccessfulPreemption:           true,
			expectPreemptionAttemptForLastVictim: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			mockCandidate := &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: tt.victims,
				},
			}

			// Set up the fake clientset.
			preemptionAttemptedPods := sets.New[string]()
			deletedPods := sets.New[string]()
			mu := &sync.RWMutex{}
			objs := []runtime.Object{preemptor}
			for _, v := range tt.victims {
				objs = append(objs, v)
			}

			cs := clientsetfake.NewClientset(objs...)
			cs.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				mu.Lock()
				defer mu.Unlock()
				name := action.(clienttesting.DeleteAction).GetName()
				preemptionAttemptedPods.Insert(name)
				if strings.HasPrefix(name, failVictimNamePrefix) {
					return true, nil, errors.New("delete pod failed")
				}
				deletedPods.Insert(name)
				return true, nil, nil
			})

			// Set up the framework.
			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
			fakeActivator := &fakePodActivator{activatedPods: make(map[string]*v1.Pod), mu: mu}

			registeredPlugins := append([]tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New)},
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			)

			snapshotPods := append([]*v1.Pod{preemptor}, tt.victims...)
			fwk, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithClientSet(cs),
				frameworkruntime.WithLogger(logger),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
				frameworkruntime.WithPodActivator(fakeActivator),
				frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
				frameworkruntime.WithPodsInPreBind(frameworkruntime.NewPodsInPreBindMap()),
				frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(snapshotPods, []*v1.Node{st.MakeNode().Name(node1Name).Obj()})),
			)
			if err != nil {
				t.Fatal(err)
			}
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			executor := preemption.NewExecutor(fwk, feature.Features{EnableAsyncPreemption: true})

			// Run the actual preemption.
			executor.ActuatePodPreemption(ctx, mockCandidate.Name(), mockCandidate.Victims(), preemptor, "test-plugin")

			// Wait for the async preemption to finish.
			err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, false, func(ctx context.Context) (bool, error) {
				// Check if the preemptor is removed from the executor.Preempting() set.
				return !executor.IsRunningAnyPreemption(), nil
			})
			if err != nil {
				t.Fatalf("Timed out waiting for async preemption to finish: %v", err)
			}

			mu.RLock()
			defer mu.RUnlock()

			lastVictimName := tt.victims[len(tt.victims)-1].Name
			if tt.expectPreemptionAttemptForLastVictim != preemptionAttemptedPods.Has(lastVictimName) {
				t.Errorf("Last victim's preemption attempted - wanted: %v, got: %v", tt.expectPreemptionAttemptForLastVictim, preemptionAttemptedPods.Has(lastVictimName))
			}
			// Verify that the preemption of the last victim is attempted if and only if
			// the preemption of all of the preceding victims succeeds.
			precedingVictimsPreempted := true
			for _, victim := range tt.victims[:len(tt.victims)-1] {
				if !preemptionAttemptedPods.Has(victim.Name) || !deletedPods.Has(victim.Name) {
					precedingVictimsPreempted = false
				}
			}
			if precedingVictimsPreempted != preemptionAttemptedPods.Has(lastVictimName) {
				t.Errorf("Last victim's preemption attempted - wanted: %v, got: %v", precedingVictimsPreempted, preemptionAttemptedPods.Has(lastVictimName))
			}

			// Verify that the preemptor is activated if and only if the async preemption fails.
			if _, ok := fakeActivator.activatedPods[preemptor.Name]; ok != !tt.expectSuccessfulPreemption {
				t.Errorf("Preemptor activated - wanted: %v, got: %v", !tt.expectSuccessfulPreemption, ok)
			}

			// Verify if the last victim got deleted as expected.
			if tt.expectSuccessfulPreemption != deletedPods.Has(lastVictimName) {
				t.Errorf("Last victim deleted - wanted: %v, got: %v", tt.expectSuccessfulPreemption, deletedPods.Has(lastVictimName))
			}
		})
	}
}

func TestPreemptPod(t *testing.T) {
	preemptorPod := st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()
	victimPod := st.MakePod().Name("v").UID("v").Priority(midPriority).Obj()

	tests := []struct {
		name               string
		addVictimToPrebind bool
		addVictimToWaiting bool
		expectCancel       bool
		expectedActions    []string
	}{
		{
			name:               "victim is in preBind, context should be cancelled",
			addVictimToPrebind: true,
			addVictimToWaiting: false,
			expectCancel:       true,
			expectedActions:    []string{},
		},
		{
			name:               "victim is in waiting pods, it should be rejected (no calls to apiserver)",
			addVictimToPrebind: false,
			addVictimToWaiting: true,
			expectCancel:       false,
			expectedActions:    []string{},
		},
		{
			name:               "victim is not in waiting/preBind pods, pod should be deleted",
			addVictimToPrebind: false,
			addVictimToWaiting: false,
			expectCancel:       false,
			expectedActions:    []string{"patch", "delete"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podsInPreBind := frameworkruntime.NewPodsInPreBindMap()
			waitingPods := frameworkruntime.NewWaitingPodsMap()
			registeredPlugins := append([]tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New)},
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPermitPlugin(waitingPermitPluginName, newWaitingPermitPlugin),
			)
			objs := []runtime.Object{preemptorPod, victimPod}
			cs := clientsetfake.NewClientset(objs...)
			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
			logger, ctx := ktesting.NewTestContext(t)

			fwk, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithClientSet(cs),
				frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot([]*v1.Pod{}, []*v1.Node{})),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithWaitingPods(waitingPods),
				frameworkruntime.WithPodsInPreBind(podsInPreBind),
				frameworkruntime.WithLogger(logger),
				frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
			)
			if err != nil {
				t.Fatal(err)
			}
			var victimCtx context.Context
			var cancel context.CancelCauseFunc
			if tt.addVictimToPrebind {
				victimCtx, cancel = context.WithCancelCause(context.Background())
				fwk.AddPodInPreBind(victimPod.UID, cancel)
			}
			if tt.addVictimToWaiting {
				pluginsWaitTime, status := fwk.RunPermitPlugins(ctx, framework.NewCycleState(), victimPod, "fake-node")
				if !status.IsWait() {
					t.Fatalf("Failed to add a pod to waiting list")
				}
				fwk.AddWaitingPod(victimPod, pluginsWaitTime)
			}
			pe := preemption.NewExecutor(fwk, feature.Features{})

			err = pe.PreemptPod(ctx, &mockCandidate{}, preemption.NewPodExecutorPreemptor(preemptorPod), victimPod, "test-plugin")
			if err != nil {
				t.Fatal(err)
			}
			if tt.expectCancel {
				if victimCtx.Err() == nil {
					t.Errorf("Context of a binding pod should be cancelled")
				}
			} else {
				if victimCtx != nil && victimCtx.Err() != nil {
					t.Errorf("Context of a normal pod should not be cancelled")
				}
			}

			// check if the API call was made
			actions := cs.Actions()
			if len(actions) != len(tt.expectedActions) {
				t.Errorf("Expected %d actions, but got %d", len(tt.expectedActions), len(actions))
			}
			for i, action := range actions {
				if action.GetVerb() != tt.expectedActions[i] {
					t.Errorf("Expected action %s, but got %s", tt.expectedActions[i], action.GetVerb())
				}
			}
		})
	}
}

// waitingPermitPlugin is a PermitPlugin that always returns Wait.
type waitingPermitPlugin struct{}

var _ fwk.PermitPlugin = &waitingPermitPlugin{}

const waitingPermitPluginName = "waitingPermitPlugin"

func newWaitingPermitPlugin(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &waitingPermitPlugin{}, nil
}

func (pl *waitingPermitPlugin) Name() string {
	return waitingPermitPluginName
}

func (pl *waitingPermitPlugin) Permit(ctx context.Context, _ fwk.CycleState, _ *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	return fwk.NewStatus(fwk.Wait, ""), 10 * time.Second
}

func TestActuatePodGroupPreemption(t *testing.T) {
	metrics.Register()
	var (
		node1Name            = "node1"
		defaultSchedulerName = "default-scheduler"
	)

	victim1 := st.MakePod().Name("victim1").UID("victim1").
		Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
		Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
		Obj()

	victim2 := st.MakePod().Name("victim2").UID("victim2").
		Node(node1Name).SchedulerName(defaultSchedulerName).Priority(midPriority).
		Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
		Obj()

	preemptor := &schedulingv1alpha1.PodGroup{
		Name: "test-pod-group",
	}

	tests := []struct {
		name          string
		mockCandidate preemption.Candidate
		victims       []*v1.Pod
	}{
		{
			name: "no victims",
			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{},
				},
			},
			victims: []*v1.Pod{},
		},
		{
			name: "one victim",
			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
					},
				},
			},
			victims: []*v1.Pod{
				victim1,
			},
		},
		{
			name: "two victims",
			mockCandidate: &mockCandidate{
				name: node1Name,
				victims: &extenderv1.Victims{
					Pods: []*v1.Pod{
						victim1,
						victim2,
					},
				},
			},
			victims: []*v1.Pod{
				victim1,
				victim2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			var objs []runtime.Object
			for _, pod := range tt.victims {
				objs = append(objs, pod)
			}

			cs := clientsetfake.NewClientset(objs...)
			deletedPods := sets.New[string]()
			mu := &sync.RWMutex{}
			cs.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				mu.Lock()
				defer mu.Unlock()
				name := action.(clienttesting.DeleteAction).GetName()
				deletedPods.Insert(name)
				return true, nil, nil
			})

			informerFactory := informers.NewSharedInformerFactory(cs, 0)
			eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})

			registeredPlugins := append([]tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New)},
				tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			)

			fwk, err := tf.NewFramework(
				ctx,
				registeredPlugins, "",
				frameworkruntime.WithClientSet(cs),
				frameworkruntime.WithLogger(logger),
				frameworkruntime.WithInformerFactory(informerFactory),
				frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, informerFactory)),
				frameworkruntime.WithEventRecorder(eventBroadcaster.NewRecorder(scheme.Scheme, "test-scheduler")),
				frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
				frameworkruntime.WithPodsInPreBind(frameworkruntime.NewPodsInPreBindMap()),
				frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(tt.victims, []*v1.Node{st.MakeNode().Name(node1Name).Obj()})),
			)
			if err != nil {
				t.Fatal(err)
			}
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())

			executor := preemption.NewExecutor(fwk, feature.Features{EnableAsyncPreemption: true})

			executor.ActuatePodGroupPreemption(ctx, tt.mockCandidate.Victims(), preemptor, "test-plugin")

			err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, false, func(ctx context.Context) (bool, error) {
				mu.RLock()
				defer mu.RUnlock()
				if deletedPods.Len() == len(tt.victims) {
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Fatalf("Timed out waiting for async preemption to finish: %v", err)
			}

			mu.RLock()
			defer mu.RUnlock()
			for _, victim := range tt.victims {
				if !deletedPods.Has(victim.Name) {
					t.Errorf("Victim %s not deleted", victim.Name)
				}
			}
		})
	}
}
