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
	"slices"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1alpha2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func TestWorkloadExecutor_SelectVictimsOnDomain(t *testing.T) {
	type blockingRule struct {
		nodeName        string
		capacity        int
		blockingVictims []string
	}

	tests := []struct {
		name                         string
		nodeNames                    []string
		initPods                     []*v1.Pod
		podGroupName                 string
		podGroupsWithGroupDisruption []string
		preemptor                    Preemptor
		pdbs                         []*policy.PodDisruptionBudget
		blockingRules                []blockingRule
		expectedPods                 [][]string
		expectedStatus               []*fwk.Status
	}{
		{
			name:      "Basic: Mix of no-group and single-pod-groups",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(lowPriority).PodGroupName("pg2").Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"p1"}},
				{nodeName: "node2", capacity: 1, blockingVictims: []string{"p2"}},
				{nodeName: "node3", capacity: 1, blockingVictims: []string{"p3"}},
			},
			expectedPods:   [][]string{{"p1"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Priority: Shared group vs no group",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(1, 0)).Obj(),
				st.MakePod().Name("p2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").StartTime(metav1.Unix(0, 0)).Obj(),
				st.MakePod().Name("p3").UID("v3").Node("node3").Priority(midPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"p1"}},
				{nodeName: "node2", capacity: 1, blockingVictims: []string{"p2"}},
				{nodeName: "node3", capacity: 1, blockingVictims: []string{"p3"}},
			},
			expectedPods:   [][]string{{"p1"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Basic: Preempt single lower priority pod",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"victim"}, capacity: 1},
			},
			expectedPods:   [][]string{{"victim"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Priority: Prefer lower priority victim",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("high-prio").UID("v3").Node("node1").Priority(highPriority).Obj(),
				st.MakePod().Name("mid-prio").UID("v2").Node("node1").Priority(midPriority).Obj(),
				st.MakePod().Name("low-prio").UID("v1").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"mid-prio"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"low-prio"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"high-prio"}, capacity: 1},
			},
			expectedPods:   [][]string{{"low-prio"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Efficiency: Preempt minimum number of victims",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("v3").UID("v3").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("v4").UID("v4").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"v1", "v2"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"v1"}, capacity: 1},
			},
			expectedPods:   [][]string{{"v1"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PDB: Prefer non-violating victim",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node1").Priority(lowPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"victim-pdb"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"victim-no-pdb"}, capacity: 1},
			},
			expectedPods:   [][]string{{"victim-no-pdb"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PDB: Prefer lower priority pod for preemption, when preemption without pdb violation is not possible",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node1").Label("app", "foo").Priority(midPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"v1"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"v2"}, capacity: 1},
			},
			expectedPods:   [][]string{{"v1"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PodGroup: Preempt group as a whole",
			nodeNames: []string{"node1", "node2"},
			initPods: []*v1.Pod{
				st.MakePod().Name("v1").UID("v1").Node("node1").Priority(lowPriority).PodGroupName("pg1").Obj(),
				st.MakePod().Name("v2").UID("v2").Node("node2").Priority(lowPriority).PodGroupName("pg1").Obj(),
			},
			podGroupsWithGroupDisruption: []string{"pg1"},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"v1"}},
			},
			expectedPods:   [][]string{{"v1", "v2"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PodGroup: Prefer single pod over podGroup for preemption candidate",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-1").UID("g1").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
			},
			podGroupsWithGroupDisruption: []string{"pg1"},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"g1-1", "g1-2"}, capacity: 1},
				{nodeName: "node1", blockingVictims: []string{"p1"}, capacity: 1},
			},
			expectedPods:   [][]string{{"p1"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PodGroup: Preempt group as a whole on single node",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("g1-1").UID("g1").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(midPriority).Obj(),
			},
			podGroupsWithGroupDisruption: []string{"pg1"},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"g1-1"}}, // Only g1-1 is blocking
			},
			expectedPods:   [][]string{{"g1-1", "g1-2"}}, // Both must be preempted
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PDB: Unit violation if any member violates",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("g1-1").UID("g1").Node("node1").Label("app", "foo").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(lowPriority).Obj(),
			},
			podGroupsWithGroupDisruption: []string{"pg1"},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"g1-1"}}, // Only g1-1 is blocking
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"p1"}},   // p1 is also blocking
			},
			expectedPods:   [][]string{{"p1"}}, // p1 is preferred because pg1 unit-violates PDB (via g1-1)
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PodGroup: Prefer preempting single pod over group of same priority",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("p1").UID("p1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-1").UID("g1").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
				st.MakePod().Name("g1-2").UID("g2").Node("node1").PodGroupName("pg1").Priority(lowPriority).Obj(),
			},
			podGroupsWithGroupDisruption: []string{"pg1"},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"g1-1", "g1-2"}},
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"p1"}},
			},
			expectedPods:   [][]string{{"p1"}}, // p1 is preempted because the PodGroup is "more important" at the same priority level
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Failure: Cannot preempt the victim with higher priority",
			nodeNames: []string{"node1"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(highPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			blockingRules: []blockingRule{
				{nodeName: "node1", blockingVictims: []string{"victim"}},
			},
			expectedPods:   [][]string{nil},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.UnschedulableAndUnresolvable)},
		},
		{
			name:      "Failure: Cannot preempt if node is empty",
			nodeNames: []string{"node1"},
			initPods:  []*v1.Pod{},
			preemptor: NewPodGroupPreemptor(
				&schedulingapi.PodGroup{ObjectMeta: metav1.ObjectMeta{Name: "preemptor-pg"}},
				[]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(midPriority).Obj()},
			),
			blockingRules:  []blockingRule{},
			expectedPods:   [][]string{nil},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.UnschedulableAndUnresolvable)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := make([]*v1.Node, len(tt.nodeNames))
			for i, nodeName := range tt.nodeNames {
				nodes[i] = st.MakeNode().Name(nodeName).Obj()
			}

			// Build nodes with pods
			nodeInfos := make(map[string]fwk.NodeInfo)
			for _, node := range nodes {
				nodeInfos[node.Name] = framework.NewNodeInfo()
				nodeInfos[node.Name].SetNode(node)
			}
			for _, p := range tt.initPods {
				podInfo, _ := framework.NewPodInfo(p)
				nodeInfos[p.Spec.NodeName].AddPodInfo(podInfo)
			}

			var domainNodes []fwk.NodeInfo
			for _, name := range tt.nodeNames {
				domainNodes = append(domainNodes, nodeInfos[name])
			}

			pgLister := &mockPodGroupLister{podGroupsWithGroupDisruption: sets.New(tt.podGroupsWithGroupDisruption...)}
			domain := NewDomainForWorkloadPreemption(domainNodes, pgLister, "test-domain")

			// Create a mock podGroupSchedulingFunc
			mockSchedulingFunc := func(ctx context.Context) *fwk.Status {
				neededSlots := len(tt.preemptor.Members())
				availableSlots := 0
				nodeMap := make(map[string]fwk.NodeInfo)
				for _, n := range domainNodes {
					nodeMap[n.Node().Name] = n
				}

				for _, rule := range tt.blockingRules {
					node, exists := nodeMap[rule.nodeName]
					if !exists {
						continue
					}

					isBlocked := false
					for _, pod := range node.GetPods() {
						if slices.Contains(rule.blockingVictims, pod.GetPod().Name) {
							isBlocked = true
							break
						}
					}

					if !isBlocked {
						availableSlots += rule.capacity
					}
				}

				if availableSlots >= neededSlots {
					return fwk.NewStatus(fwk.Success)
				}
				return fwk.NewStatus(fwk.Unschedulable)
			}

			executor := &mockPreemptionExecutor{}
			pl := &PodGroupEvaluator{
				Handler:                &mockHandle{executor: executor},
				podGroupSchedulingFunc: mockSchedulingFunc,
			}

			gotStatus := pl.selectVictimsOnDomain(context.Background(), tt.preemptor, domain, tt.pdbs)
			if gotStatus != nil && !gotStatus.IsSuccess() {
				t.Logf("SelectVictimsOnDomain failed: %v", gotStatus.Message())
			}

			gotPods := executor.gatheredVictims

			wantStatus := tt.expectedStatus[0]
			wantCode := fwk.Success
			if wantStatus != nil {
				wantCode = wantStatus.Code()
			}

			gotCode := fwk.Success
			if gotStatus != nil {
				gotCode = gotStatus.Code()
			}

			if gotCode != wantCode {
				t.Errorf("Status mismatch. Want %v, Got %v", wantCode, gotCode)
			}

			if wantCode != fwk.Success {
				return
			}

			var gotNames []string
			for _, p := range gotPods {
				gotNames = append(gotNames, p.Name)
			}
			sort.Strings(gotNames)

			wantNames := tt.expectedPods[0]
			sort.Strings(wantNames)

			if diff := cmp.Diff(wantNames, gotNames); diff != "" {
				t.Errorf("Victims mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type mockPreemptionExecutor struct {
	fwk.PreemptionExecutor
	gatheredVictims []*v1.Pod
}

func (m *mockPreemptionExecutor) ActuatePodGroupPreemption(ctx context.Context, victims *extenderv1.Victims, preemptorPods []*v1.Pod, preemptor *schedulingapi.PodGroup, pluginName string) *fwk.Status {
	m.gatheredVictims = victims.Pods
	return nil
}

type mockHandle struct {
	fwk.Handle
	executor fwk.PreemptionExecutor
}

func (m *mockHandle) PreemptionExecutor() fwk.PreemptionExecutor {
	return m.executor
}

type mockPodGroupLister struct {
	schedulinglisters.PodGroupLister
	podGroupsWithGroupDisruption sets.Set[string]
}

func (m *mockPodGroupLister) PodGroups(namespace string) schedulinglisters.PodGroupNamespaceLister {
	return &mockPodGroupNamespaceLister{podGroupsWithGroupDisruption: m.podGroupsWithGroupDisruption, namespace: namespace}
}

type mockPodGroupNamespaceLister struct {
	schedulinglisters.PodGroupNamespaceLister
	podGroupsWithGroupDisruption sets.Set[string]
	namespace                    string
}

func (m *mockPodGroupNamespaceLister) Get(name string) (*schedulingapi.PodGroup, error) {
	dm := schedulingapi.DisruptionModePod
	if m.podGroupsWithGroupDisruption.Has(name) {
		dm = schedulingapi.DisruptionModePodGroup
	}
	return &schedulingapi.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.namespace},
		Spec:       schedulingapi.PodGroupSpec{DisruptionMode: &dm},
	}, nil
}
