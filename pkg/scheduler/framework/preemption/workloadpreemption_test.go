package preemption

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

type mockPreemptionUnit struct {
	pods          []fwk.PodInfo
	priority      int32
	affectedNodes map[string]fwk.NodeInfo
}

var _ PreemptionUnit = &mockPreemptionUnit{}

func (m *mockPreemptionUnit) Pods() []fwk.PodInfo {
	return m.pods
}

func (m *mockPreemptionUnit) Priority() int32 {
	return m.priority
}

func (m *mockPreemptionUnit) AffectedNodes() map[string]fwk.NodeInfo {
	return m.affectedNodes
}

type mockDomain struct {
	nodes              []fwk.NodeInfo
	name               string
	allPossibleVictims []PreemptionUnit
}

var _ Domain = &mockDomain{}

func (d *mockDomain) GetName() string {
	return d.name
}

func (d *mockDomain) GetNodes() []fwk.NodeInfo {
	return d.nodes
}

func (d *mockDomain) GetAllPossibleVictims() []PreemptionUnit {
	return d.allPossibleVictims
}

func (d *mockDomain) Nodes() []fwk.NodeInfo {
	return d.nodes
}

func newPreemptionUnit(pods []fwk.PodInfo, priority int32, nodeInfos []fwk.NodeInfo) PreemptionUnit {
	nodes := make(map[string]fwk.NodeInfo)
	for _, node := range nodeInfos {
		nodes[node.Node().Name] = node
	}

	return &mockPreemptionUnit{
		affectedNodes: nodes,
		priority:      priority,
		pods:          pods,
	}
}

const (
	lowPriority  = 10
	midPriority  = 20
	highPriority = 30
)

func TestWorkloadExecutor_SelectVictimsOnDomain(t *testing.T) {
	type blockingRule struct {
		nodeName        string
		capacity        int
		blockingVictims []string
	}

	tests := []struct {
		name           string
		nodeNames      []string
		initPods       []*v1.Pod
		preemptor      Preemptor
		pdbs           []*policy.PodDisruptionBudget
		blockingRules  []blockingRule
		expectedPods   [][]string
		expectedStatus []*fwk.Status
	}{
		{
			name:      "Basic: Preempt single lower priority pod",
			nodeNames: []string{"node1", "node2"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim").UID("v1").Node("node1").Priority(lowPriority).Obj(),
				st.MakePod().Name("other").UID("v2").Node("node2").Priority(midPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor([]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()}, []fwk.CycleState{framework.NewCycleState()}),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"victim"}},
			},
			expectedPods:   [][]string{{"victim"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "Priority: Prefer lower priority victim",
			nodeNames: []string{"node1", "node2", "node3"},
			initPods: []*v1.Pod{
				st.MakePod().Name("high-prio").UID("v3").Node("node3").Priority(highPriority).Obj(),
				st.MakePod().Name("mid-prio").UID("v2").Node("node2").Priority(midPriority).Obj(),
				st.MakePod().Name("low-prio").UID("v1").Node("node1").Priority(lowPriority).Obj(),
			},
			preemptor: NewPodGroupPreemptor([]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()}, []fwk.CycleState{framework.NewCycleState()}),
			blockingRules: []blockingRule{
				{nodeName: "node2", capacity: 1, blockingVictims: []string{"mid-prio"}},
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"low-prio"}},
			},
			expectedPods:   [][]string{{"low-prio"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
		},
		{
			name:      "PDB: Prefer non-violating victim",
			nodeNames: []string{"node1", "node2"},
			initPods: []*v1.Pod{
				st.MakePod().Name("victim-pdb").UID("v1").Node("node1").Label("app", "foo").Priority(lowPriority).Obj(),
				st.MakePod().Name("victim-no-pdb").UID("v2").Node("node2").Priority(lowPriority).Obj(),
			},
			pdbs: []*policy.PodDisruptionBudget{
				{
					Spec:   policy.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "foo"}}},
					Status: policy.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
				},
			},
			preemptor: NewPodGroupPreemptor([]*v1.Pod{st.MakePod().Name("p").UID("p").Priority(highPriority).Obj()}, []fwk.CycleState{framework.NewCycleState()}),
			blockingRules: []blockingRule{
				{nodeName: "node1", capacity: 1, blockingVictims: []string{"victim-pdb"}},
				{nodeName: "node2", capacity: 1, blockingVictims: []string{"victim-no-pdb"}},
			},
			expectedPods:   [][]string{{"victim-no-pdb"}},
			expectedStatus: []*fwk.Status{fwk.NewStatus(fwk.Success)},
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
			var victims []PreemptionUnit
			for _, nodeInfo := range nodeInfos {
				domainNodes = append(domainNodes, nodeInfo)
				for _, pod := range nodeInfo.GetPods() {
					victims = append(victims, newPreemptionUnit(
						[]fwk.PodInfo{pod},
						corev1helpers.PodPriority(pod.GetPod()),
						[]fwk.NodeInfo{nodeInfo},
					))
				}
			}

			domain := &mockDomain{
				nodes:              domainNodes,
				name:               "test-domain",
				allPossibleVictims: victims,
			}

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
						for _, blockingVictim := range rule.blockingVictims {
							if pod.GetPod().Name == blockingVictim {
								isBlocked = true
								break
							}
						}
						if isBlocked {
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

			pl := &WorkloadExecutor{
				Name:                   "test-executor",
				podGroupSchedulingFunc: mockSchedulingFunc,
			}

			gotPods, gotStatus := pl.SelectVictimsOnDomain(context.Background(), tt.preemptor, domain, tt.pdbs)

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
