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

package preemption

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	apicache "k8s.io/kubernetes/pkg/scheduler/backend/api_cache"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// fakePodLister helps test IsPodRunningPreemption logic without worrying about cache synchronization issues.
// Current list of pods is set using field pods.
type fakePodLister struct {
	corelisters.PodLister
	pods map[string]*v1.Pod
}

func (m *fakePodLister) Pods(namespace string) corelisters.PodNamespaceLister {
	return &fakePodNamespaceLister{pods: m.pods}
}

// fakePodNamespaceLister helps test IsPodRunningPreemption logic without worrying about cache synchronization issues.
// Current list of pods is set using field pods.
type fakePodNamespaceLister struct {
	corelisters.PodNamespaceLister
	pods map[string]*v1.Pod
}

func (m *fakePodNamespaceLister) Get(name string) (*v1.Pod, error) {
	if pod, ok := m.pods[name]; ok {
		return pod, nil
	}
	// Important: Return the standard IsNotFound error for a fake cache miss.
	return nil, apierrors.NewNotFound(v1.Resource("pods"), name)
}

func TestIsPodRunningPreemption(t *testing.T) {
	var (
		midPriority = int32(100)
		victim1     = st.MakePod().Name("victim1").UID("victim1").
				Node("node").SchedulerName("sch").Priority(midPriority).
				Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
				Obj()

		victim2 = st.MakePod().Name("victim2").UID("victim2").
			Node("node").SchedulerName("sch").Priority(midPriority).
			Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
			Obj()

		victimWithDeletionTimestamp = st.MakePod().Name("victim-deleted").UID("victim-deleted").
						Node("node").SchedulerName("sch").Priority(midPriority).
						Terminating().
						Containers([]v1.Container{st.MakeContainer().Name("container1").Obj()}).
						Obj()
	)

	tests := []struct {
		name            string
		preemptorUID    types.UID
		preemptingSet   sets.Set[types.UID]
		lastVictimSet   map[types.UID]pendingVictim
		podsInPodLister map[string]*v1.Pod
		expectedResult  bool
	}{
		{
			name:           "preemptor not in preemptingSet",
			preemptorUID:   "preemptor",
			preemptingSet:  sets.New[types.UID](),
			lastVictimSet:  map[types.UID]pendingVictim{},
			expectedResult: false,
		},
		{
			name:          "preemptor not in preemptingSet, lastVictimSet not empty",
			preemptorUID:  "preemptor",
			preemptingSet: sets.New[types.UID](),
			lastVictimSet: map[types.UID]pendingVictim{
				"preemptor": {
					namespace: "ns",
					name:      "victim1",
				},
			},
			expectedResult: false,
		},
		{
			name:          "preemptor in preemptingSet, no lastVictim for preemptor",
			preemptorUID:  "preemptor",
			preemptingSet: sets.New[types.UID]("preemptor"),
			lastVictimSet: map[types.UID]pendingVictim{
				"otherPod": {
					namespace: "ns",
					name:      "victim1",
				},
			},
			expectedResult: true,
		},
		{
			name:          "preemptor in preemptingSet, victim in lastVictimSet, not in PodLister",
			preemptorUID:  "preemptor",
			preemptingSet: sets.New[types.UID]("preemptor"),
			lastVictimSet: map[types.UID]pendingVictim{
				"preemptor": {
					namespace: "ns",
					name:      "victim1",
				},
			},
			podsInPodLister: map[string]*v1.Pod{},
			expectedResult:  false,
		},
		{
			name:          "preemptor in preemptingSet, victim in lastVictimSet and in PodLister",
			preemptorUID:  "preemptor",
			preemptingSet: sets.New[types.UID]("preemptor"),
			lastVictimSet: map[types.UID]pendingVictim{
				"preemptor": {
					namespace: "ns",
					name:      "victim1",
				},
			},
			podsInPodLister: map[string]*v1.Pod{
				"victim1": victim1,
				"victim2": victim2,
			},
			expectedResult: true,
		},
		{
			name:          "preemptor in preemptingSet, victim in lastVictimSet and in PodLister with deletion timestamp",
			preemptorUID:  "preemptor",
			preemptingSet: sets.New[types.UID]("preemptor"),
			lastVictimSet: map[types.UID]pendingVictim{
				"preemptor": {
					namespace: "ns",
					name:      "victim-deleted",
				},
			},
			podsInPodLister: map[string]*v1.Pod{
				"victim1":        victim1,
				"victim-deleted": victimWithDeletionTimestamp,
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.name), func(t *testing.T) {

			// fakeLister is not used, since the test does not test the logic that uses lister.
			_ = &fakePodLister{
				pods: tt.podsInPodLister,
			}
			client := clientsetfake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(client, 0)
			for _, pod := range tt.podsInPodLister {
				informerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod)
			}
			a := &Executor{
				fh:                           &fakeHandleForLister{informerFactory: informerFactory},
				preempting:                   tt.preemptingSet,
				lastVictimsPendingPreemption: tt.lastVictimSet,
			}

			if result := a.IsPodRunningPreemption(tt.preemptorUID); tt.expectedResult != result {
				t.Errorf("Expected IsPodRunningPreemption to return %v but got %v", tt.expectedResult, result)
			}
		})
	}
}

func TestRemoveNominatedNodeName(t *testing.T) {
	tests := []struct {
		name                     string
		currentNominatedNodeName string
		newNominatedNodeName     string
		expectPatchRequest       bool
		expectedPatchData        string
	}{
		{
			name:                     "Should make patch request to clear node name",
			currentNominatedNodeName: "node1",
			expectPatchRequest:       true,
			expectedPatchData:        `{"status":{"nominatedNodeName":null}}`,
		},
		{
			name:                     "Should not make patch request if nominated node is already cleared",
			currentNominatedNodeName: "",
			expectPatchRequest:       false,
		},
	}
	for _, asyncAPICallsEnabled := range []bool{true, false} {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				logger, ctx := ktesting.NewTestContext(t)
				actualPatchRequests := 0
				var actualPatchData string
				cs := &clientsetfake.Clientset{}
				patchCalled := make(chan struct{}, 1)
				cs.AddReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					actualPatchRequests++
					patch := action.(clienttesting.PatchAction)
					actualPatchData = string(patch.GetPatch())
					patchCalled <- struct{}{}
					// For this test, we don't care about the result of the patched pod, just that we got the expected
					// patch request, so just returning &v1.Pod{} here is OK because scheduler doesn't use the response.
					return true, &v1.Pod{}, nil
				})

				pod := &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "foo"},
					Status:     v1.PodStatus{NominatedNodeName: test.currentNominatedNodeName},
				}

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				var apiCacher fwk.APICacher
				if asyncAPICallsEnabled {
					apiDispatcher := apidispatcher.New(cs, 16, apicalls.Relevances)
					apiDispatcher.Run(logger)
					defer apiDispatcher.Close()

					informerFactory := informers.NewSharedInformerFactory(cs, 0)
					queue := internalqueue.NewSchedulingQueue(nil, informerFactory, internalqueue.WithAPIDispatcher(apiDispatcher))
					apiCacher = apicache.New(queue, nil)
				}

				if err := clearNominatedNodeName(ctx, cs, apiCacher, pod); err != nil {
					t.Fatalf("Error calling removeNominatedNodeName: %v", err)
				}

				if test.expectPatchRequest {
					select {
					case <-patchCalled:
					case <-time.After(time.Second):
						t.Fatalf("Timed out while waiting for patch to be called")
					}
					if actualPatchData != test.expectedPatchData {
						t.Fatalf("Patch data mismatch: Actual was %v, but expected %v", actualPatchData, test.expectedPatchData)
					}
				} else {
					select {
					case <-patchCalled:
						t.Fatalf("Expected patch not to be called, actual patch data: %v", actualPatchData)
					case <-time.After(time.Second):
					}
				}
			})
		}
	}
}

type fakeHandleForLister struct {
	fwk.Handle
	informerFactory informers.SharedInformerFactory
}

func (f *fakeHandleForLister) SharedInformerFactory() informers.SharedInformerFactory {
	return f.informerFactory
}
