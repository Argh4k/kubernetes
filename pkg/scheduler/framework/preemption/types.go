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
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// fwk.Preemptor abstracts the entity that initiates preemption.
// It acts as a unified interface for both single Pods and collective Workloads PodGroups,
// allowing the preemption logic to treat them polymorphically.
type preemptor struct {
	fwk.Preemptor
	priority         int32
	pods             []*v1.Pod
	preemptionPolicy *v1.PreemptionPolicy
	isPodGroup       bool
	states           []fwk.CycleState
}

func NewPodPreemptor(p *v1.Pod, state fwk.CycleState) fwk.Preemptor {
	return &preemptor{
		priority:         corev1helpers.PodPriority(p),
		pods:             []*v1.Pod{p},
		preemptionPolicy: p.Spec.PreemptionPolicy,
		isPodGroup:       false,
		states:           []fwk.CycleState{state},
	}
}

func NewPodGroupPreemptor(pods []*v1.Pod, states []fwk.CycleState) fwk.Preemptor {
	return &preemptor{
		// TODO(Argh4k): Get priority and preemption policy from the podgroup once they are there.
		priority:         corev1helpers.PodPriority(pods[0]),
		pods:             pods,
		preemptionPolicy: pods[0].Spec.PreemptionPolicy,
		isPodGroup:       true,
		states:           states,
	}
}

func (p *preemptor) Priority() int32 {
	return p.priority
}

func (p *preemptor) IsPodGroup() bool {
	return p.isPodGroup
}

func (p *preemptor) Members() []*v1.Pod {
	return p.pods
}

func (p *preemptor) CycleStates() []fwk.CycleState {
	return p.states
}

func (p *preemptor) Snapshot() fwk.Preemptor {
	newStates := make([]fwk.CycleState, len(p.states))
	for i, state := range p.states {
		newStates[i] = state.Clone()
	}

	return &preemptor{
		priority:         p.priority,
		pods:             p.pods,
		preemptionPolicy: p.preemptionPolicy,
		isPodGroup:       p.isPodGroup,
		states:           newStates,
	}
}

func (p *preemptor) IsEligibleToPreemptOthers() bool {
	return p.preemptionPolicy == nil || *p.preemptionPolicy != v1.PreemptNever
}

func (p *preemptor) SupportExtenders() bool {
	return !p.isPodGroup
}

func (p *preemptor) GetNamespace() string {
	if len(p.pods) > 0 {
		return p.pods[0].Namespace
	}
	return ""
}

func (p *preemptor) GetName() string {
	if len(p.pods) == 0 {
		return "unknown"
	}

	firstPod := p.GetRepresentativePod()

	if p.isPodGroup {
		ref := firstPod.Spec.WorkloadRef

		// Start with the Workload Name (e.g., "my-job")
		name := ref.Name

		// Append PodGroup if distinct (e.g., "my-job/group-1")
		if ref.PodGroup != "" {
			name = name + "/" + ref.PodGroup
		}

		// Append ReplicaKey if present (e.g., "my-job/group-1/idx-0")
		// This is crucial for distinguishing between retries of the same job.
		if ref.PodGroupReplicaKey != "" {
			name = name + "/" + ref.PodGroupReplicaKey
		}

		return name
	}

	return firstPod.Name
}

func (p *preemptor) GetRepresentativePod() *v1.Pod {
	if len(p.pods) == 0 {
		return nil
	}

	return p.pods[0]
}

// fwk.Domain represents the boundary or scope within which the preemption logic is evaluated.
// It abstracts the scheduling domain, which can range from a single Node (for standard Pod preemption)
// to a group of Nodes or the entire Cluster (for Workload preemption).
type domain struct {
	fwk.Domain
	nodes              []fwk.NodeInfo
	name               string
	allPossibleVictims []fwk.PreemptionUnit
}

func (d *domain) Nodes() []fwk.NodeInfo {
	return d.nodes
}

func (d *domain) GetAllPossibleVictims(nodeInfoLister fwk.NodeInfoLister) []fwk.PreemptionUnit {
	return d.allPossibleVictims
}

func (d *domain) Snapshot() fwk.Domain {
	nodeSnapshotMap := make(map[string]fwk.NodeInfo, len(d.nodes))
	snapshotNodes := make([]fwk.NodeInfo, 0, len(d.nodes))

	for _, node := range d.nodes {
		copy := node.Snapshot()
		snapshotNodes = append(snapshotNodes, copy)
		nodeSnapshotMap[node.Node().Name] = copy
	}

	allPossibleVictims := make([]fwk.PreemptionUnit, 0, len(d.allPossibleVictims))
	for _, v := range d.allPossibleVictims {
		var victimNodeInfos []fwk.NodeInfo
		for _, node := range v.AffectedNodes() {
			nodeName := node.Node().Name

			if existingCopy, ok := nodeSnapshotMap[nodeName]; ok {
				// Point to the existing snapshot
				victimNodeInfos = append(victimNodeInfos, existingCopy)
			} else {
				// Edge Case: The victim resides on a node that isn't part of the
				// domain's primary node list (e.g., cross-node preemption scope).
				// We must snapshot it independently to ensure we don't mutate the original.
				outsideDomainNodeCopy := node.Snapshot()
				victimNodeInfos = append(victimNodeInfos, outsideDomainNodeCopy)

				// Cache it in case another victim needs this same orphan node
				nodeSnapshotMap[nodeName] = outsideDomainNodeCopy
			}
		}
		allPossibleVictims = append(allPossibleVictims, newPreemptionUnit(v.Pods(), v.Priority(), victimNodeInfos))
	}

	return &domain{
		nodes:              snapshotNodes,
		name:               d.name,
		allPossibleVictims: allPossibleVictims,
	}
}

func (d *domain) GetName() string {
	return d.name
}

// fwk.PreemptionUnit represents an atomic entity that can be preempted (a victim).
// It abstracts individual Pods and PodGroup, ensuring that
// atomic entities are treated as a single unit during eviction.
type preemptionUnit struct {
	fwk.PreemptionUnit
	pods          []fwk.PodInfo
	priority      int32
	affectedNodes map[string]fwk.NodeInfo //TODO: should I store that here?
	isPodGroup    bool
}

func newPreemptionUnit(pods []fwk.PodInfo, priority int32, nodeInfos []fwk.NodeInfo) fwk.PreemptionUnit {
	nodes := make(map[string]fwk.NodeInfo)
	for _, node := range nodeInfos {
		nodes[node.Node().Name] = node
	}

	return &preemptionUnit{
		affectedNodes: nodes,
		priority:      priority,
		isPodGroup:    pods[0].GetPod().Spec.WorkloadRef != nil,
		pods:          pods,
	}
}

func (pu *preemptionUnit) Pods() []fwk.PodInfo {
	return pu.pods
}

func (pu *preemptionUnit) Priority() int32 {
	return pu.priority
}

func (pu *preemptionUnit) IsPodGroup() bool {
	return pu.isPodGroup
}

func (pu *preemptionUnit) AffectedNodes() map[string]fwk.NodeInfo {
	return pu.affectedNodes
}

// fwk.Candidate represents a nominated node on which the preemptor can be scheduled,
// along with the list of victims that should be evicted for the preemptor to fit the node.
type candidate struct {
	victims *extenderv1.Victims
	name    string
	nodes   []string
}

// Victims returns s.victims.
func (s *candidate) Victims() *extenderv1.Victims {
	return s.victims
}

// Name returns s.name.
func (s *candidate) Name() string {
	return s.name
}

// GetNodes returns the unique names of all nodes where the victims of this candidate are currently running.
// This identifies the set of nodes that would be affected if this candidate is chosen for preemption.
func (s *candidate) GetNodes() []string {
	if s.nodes == nil {
		uniqueNames := make(map[string]bool)

		for _, pod := range s.Victims().Pods {
			uniqueNames[pod.Spec.NodeName] = true
		}

		res := make([]string, 0, len(uniqueNames))
		for name := range uniqueNames {
			res = append(res, name)
		}

		s.nodes = res
	}

	result := make([]string, len(s.nodes))
	copy(result, s.nodes)

	return result
}

type candidateList struct {
	idx   int32
	items []fwk.Candidate
}

// newCandidateList creates a new candidate list with the given capacity.
func newCandidateList(capacity int32) *candidateList {
	return &candidateList{idx: -1, items: make([]fwk.Candidate, capacity)}
}

// add adds a new candidate to the internal array atomically.
// Note: in case the list has reached its capacity, the candidate is disregarded
// and not added to the internal array.
func (cl *candidateList) add(c *candidate) {
	if idx := atomic.AddInt32(&cl.idx, 1); idx < int32(len(cl.items)) {
		cl.items[idx] = c
	}
}

// size returns the number of candidate stored. Note that some add() operations
// might still be executing when this is called, so care must be taken to
// ensure that all add() operations complete before accessing the elements of
// the list.
func (cl *candidateList) size() int32 {
	return min(atomic.LoadInt32(&cl.idx)+1, int32(len(cl.items)))
}

// get returns the internal candidate array. This function is NOT atomic and
// assumes that all add() operations have been completed.
func (cl *candidateList) get() []fwk.Candidate {
	return cl.items[:cl.size()]
}
