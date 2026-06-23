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
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/util/sets"
	policylisters "k8s.io/client-go/listers/policy/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1alpha3"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"

	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// PodGroupEvaluator is a preemption evaluator that knows how to run
// preemption where a preemptor is a pod group and the domain is the whole cluster.
type PodGroupEvaluator struct {
	Handle         fwk.Handle
	pdbLister      policylisters.PodDisruptionBudgetLister
	podGroupLister schedulinglisters.PodGroupLister

	Executor *Executor
}

// NewPodGroupEvaluator creates a new PodGroupEvaluator.
func NewPodGroupEvaluator(fh fwk.Handle, executor *Executor) *PodGroupEvaluator {
	return &PodGroupEvaluator{
		Handle:         fh,
		pdbLister:      fh.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister(),
		podGroupLister: fh.SharedInformerFactory().Scheduling().V1alpha3().PodGroups().Lister(),
		Executor:       executor,
	}
}

// Preempt implements the preemption logic where the preemptor is a pod group
// and the domain is the whole cluster. It preempts pod from the cluster
// in order to make enough room for the pod group to be scheduled.
// It returns PodGroupPreemptorResult which contains the mapping of nominated nodes
// for each pod in the pod group, and a status of the whole preemption process.
// The preemption logic modifies the NodeInfo provided by a Handle
// podGroupSchedulingFunc is a function that will be run to check feasibility of a pod group
// scheduling after modifying the node state.
// The caller is expected to backup the NodeInfo before calling this function
// And rollback the state to the backup after function is finished.
func (ev *PodGroupEvaluator) Preempt(ctx context.Context, pg *schedulingapi.PodGroup, pods []*v1.Pod, podGroupSchedulingFunc framework.PodGroupSchedulingFunc) (*framework.PodGroupPostFilterResult, *fwk.Status) {
	// In case of workload-aware preemption, the domain is whole cluster.
	// We do not make a snapshot of node info. Those nodes will be shared
	// with the PodGroup scheduling algorithm passed as podGroupSchedulingFunc.
	allNodes, err := ev.Handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("failed to list node infos: %w", err))
	}
	domain := newDomainForWorkloadPreemption(allNodes, ev.podGroupLister, "cluster-domain")
	preemptor := newPodGroupPreemptor(pg, pods)
	pdbs, err := getPodDisruptionBudgets(ev.pdbLister)
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("failed to get pod disruption budgets: %w", err))
	}

	res, status := ev.selectVictimsOnDomain(ctx, preemptor, domain, pdbs, podGroupSchedulingFunc)
	if !status.IsSuccess() {
		return nil, status
	}
	status = ev.Executor.actuatePodGroupPreemption(ctx, res.victims, preemptor.pods, preemptor.podGroup, names.DefaultPreemption)
	return &framework.PodGroupPostFilterResult{NominatedNodeNames: res.nominatedNodeNames}, status
}

type selectVictimsResult struct {
	nominatedNodeNames map[*v1.Pod]*fwk.NominatingInfo
	victims            *extenderv1.Victims
}

// selectVictimsOnDomain selects a set of victims that can be removed from the
// domain in order to make enough room for the preemptor to be scheduled.
// It prioritizes victims that are not protected by a PDB.
func (ev *PodGroupEvaluator) selectVictimsOnDomain(
	ctx context.Context,
	preemptor *podGroupPreemptor,
	domain *domain,
	pdbs []*policy.PodDisruptionBudget,
	podGroupSchedulingFunc framework.PodGroupSchedulingFunc) (*selectVictimsResult, *fwk.Status) {
	logger := klog.FromContext(ctx)

	nameToNode := make(map[string]fwk.NodeInfo)
	for _, nodeInfo := range domain.Nodes() {
		nameToNode[nodeInfo.Node().Name] = nodeInfo
	}

	// Ensure the preemptor is eligible to preempt other pods.
	if ok, msg := ev.preemptorEligibleToPreemptOthers(ctx, preemptor, nameToNode); !ok {
		logger.V(5).Info("Preemptor is not eligible for preemption", "preemptor", klog.KObj(preemptor.podGroup), "reason", msg)
		return nil, fwk.NewStatus(fwk.Unschedulable, msg)
	}

	removePods := func(v *victim) error {
		for _, pi := range v.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
				return err
			}
		}
		return nil
	}

	addPodsWithPreFilter := func(v *victim, preemptorAssignments *fwk.PodGroupAssignments) error {
		for _, pi := range v.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			nodeInfo.AddPodInfo(pi)
			for _, assignment := range preemptorAssignments.ProposedAssignments {
				status := ev.Handle.RunPreFilterExtensionAddPod(ctx, assignment.GetCycleState(), assignment.GetPod(), pi, nodeInfo)
				if !status.IsSuccess() {
					return status.AsError()
				}
			}
		}
		return nil
	}

	removePodsWithPreFilter := func(v *victim, preemptorAssignments *fwk.PodGroupAssignments) error {
		for _, pi := range v.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
				return err
			}
			for _, assignment := range preemptorAssignments.ProposedAssignments {
				status := ev.Handle.RunPreFilterExtensionRemovePod(ctx, assignment.GetCycleState(), assignment.GetPod(), pi, nodeInfo)
				if !status.IsSuccess() {
					return status.AsError()
				}
			}
		}
		return nil
	}

	var potentialVictims []*victim
	allPossiblyAffectedVictims := domain.GetAllPossibleVictims()
	for _, victim := range allPossiblyAffectedVictims {
		if ev.isPreemptionAllowed(victim, preemptor) {
			potentialVictims = append(potentialVictims, victim)
		}
	}

	// No preemption victims found for incoming preemptor.
	if len(potentialVictims) == 0 {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming preemptor")
	}

	// PodGroup being a victim can have pods spread between current domain and other domains
	// We need to pull data about nodes from outside of the current domain
	// if a pod group has a pod on it.
	for _, victim := range potentialVictims {
		for name, nodeInfo := range victim.AffectedNodes() {
			_, ok := nameToNode[name]
			if !ok {
				nameToNode[name] = nodeInfo
			}
		}

		if err := removePods(victim); err != nil {
			return nil, fwk.AsStatus(err)
		}
	}

	// If the scheduling failed after removing all potential victims, return the status.
	podGroupAssignments, status := podGroupSchedulingFunc(ctx)
	if !status.IsSuccess() {
		return nil, status
	}

	sort.Slice(potentialVictims, func(i, j int) bool {
		return moreImportantVictim(potentialVictims[i], potentialVictims[j])
	})

	violatingVictims, nonViolatingVictims := filterVictimsWithPDBViolation(potentialVictims, pdbs)
	numViolatingVictim := 0

	podInfoCache := make([]struct {
		pi fwk.PodInfo
		e  bool
	}, len(podGroupAssignments.ProposedAssignments))
	reprieveVictim := func(v *victim, preemptorAssignments *fwk.PodGroupAssignments) (bool, error) {
		victimNames := ""
		for _, pi := range v.Pods() {
			victimNames += pi.GetPod().Name + ", "
		}
		logger.Info("WAP: Reprieving for victim ", "victim", victimNames)
		if err := addPodsWithPreFilter(v, preemptorAssignments); err != nil {
			return false, err
		}
		cleanUpFn := []func(){}
		fits := true
		for i, assignment := range preemptorAssignments.ProposedAssignments {
			if assignment.GetNodeName() == "" {
				continue
			}
			nodeInfo := nameToNode[assignment.GetNodeName()]
			logger.Info("WAP: running filter for pod on node", "podName", assignment.GetPod().Name, "nodeName", nodeInfo.Node().Name)

			if s := ev.Handle.RunFilterPluginsWithNominatedPods(ctx, assignment.GetCycleState(), assignment.GetPod(), nodeInfo); s != nil {
				logger.Info("WAP: Reprieve failed because", "reasons", s.Reasons())
				fits = false
				break
			} else {
				// Do we need to also run Reserve/Unreserve Here?
				// Some plugins change their cache
				// For example:
				// VolumeBinding: pl.Binder.AssumePodVolumes
				// DRA: pl.draManager.ResourceClaims().SignalClaimPendingAllocation(claim.UID, claim)
				// But they cannot change (victim removal/addition does not change this cache)
				//
				// And what about victims. Should we ? No, their state is not affected when we remove them
				// via NodeInfo and we do not have mechanism to do so. Maybe we will need it, looking
				// at DRA KEP.
				if !podInfoCache[i].e {
					podInfo, err := framework.NewPodInfo(assignment.GetPod())
					if err != nil {
						return false, err
					}
					podInfoCache[i] = struct {
						pi fwk.PodInfo
						e  bool
					}{podInfo, true}
				}
				nodeInfo.AddPodInfo(podInfoCache[i].pi)
				cleanUpFn = append(cleanUpFn, func() {
					nodeInfo.RemovePod(logger, assignment.GetPod())
				})
			}
		}

		if !fits {
			if err := removePodsWithPreFilter(v, preemptorAssignments); err != nil {
				return false, err
			}
			var names []string
			for _, p := range v.Pods() {
				names = append(names, p.GetPod().Name)
			}
			pods := strings.Join(names, ",")
			logger.V(6).Info("Pods are potential preemption victims on domain", "pods", pods, "domain", domain.GetName())
		}

		for _, fn := range cleanUpFn {
			fn()
		}

		return fits, nil
	}

	// Try to reprieve as many pods as possible. We first try to reprieve the PDB
	// violating victims and then other non-violating ones. In both cases, we start
	// from the highest importance victims.
	var victimsToPreempt []*victim
	for _, v := range violatingVictims {
		if fits, err := reprieveVictim(v, podGroupAssignments); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			victimsToPreempt = append(victimsToPreempt, v)
			numViolatingVictim++
		}
	}

	for _, v := range nonViolatingVictims {
		if fits, err := reprieveVictim(v, podGroupAssignments); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			victimsToPreempt = append(victimsToPreempt, v)
		}
	}

	sort.Slice(victimsToPreempt, func(i, j int) bool {
		return moreImportantVictim(victimsToPreempt[i], victimsToPreempt[j])
	})
	var podsToPreempt []*v1.Pod
	for _, v := range victimsToPreempt {
		for _, pi := range v.Pods() {
			podsToPreempt = append(podsToPreempt, pi.GetPod())
		}
	}

	v := &extenderv1.Victims{
		Pods: podsToPreempt,
	}
	n := make(map[*v1.Pod]*fwk.NominatingInfo)
	for _, p := range podGroupAssignments.ProposedAssignments {
		if p.GetNodeName() != "" {
			n[p.GetPod()] = &fwk.NominatingInfo{
				NominatingMode:    fwk.ModeOverride,
				NominatedNodeName: p.GetNodeName(),
			}
		}
	}

	return &selectVictimsResult{nominatedNodeNames: n, victims: v}, nil
}

// isPreemptionAllowed returns whether the victim residing on nodeInfo can be preempted by the preemptor
func (ev *PodGroupEvaluator) isPreemptionAllowed(victim *victim, preemptor *podGroupPreemptor) bool {
	// The victim must have lower priority than the preemptor.
	return victim.Priority() < preemptor.Priority()
}

// preemptorEligibleToPreemptOthers returns one bool and one string. The bool
// indicates whether this preemptor should be considered for preempting other pods or
// not. The string includes the reason if this preemptor isn't eligible.
func (ev *PodGroupEvaluator) preemptorEligibleToPreemptOthers(_ context.Context, preemptor *podGroupPreemptor, nameToNode map[string]fwk.NodeInfo) (bool, string) {
	if preemptor.PreemptionPolicy() == v1.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}

	nominatedNodes := sets.New[string]()
	for _, pod := range preemptor.Members() {
		if len(pod.Status.NominatedNodeName) > 0 {
			nominatedNodes.Insert(pod.Status.NominatedNodeName)
		}
	}

	for nomNodeName := range nominatedNodes {
		if nodeInfo, exists := nameToNode[nomNodeName]; exists {
			for _, p := range nodeInfo.GetPods() {
				if ev.getPodPriority(p.GetPod()) < preemptor.Priority() && PodTerminatingByPreemption(p.GetPod()) {
					return false, "not eligible due to a terminating pod on the nominated node."
				}
			}
		}
	}

	return true, ""
}

// getPodPriority returns the effective preemption priority of a pod. If the pod belongs to
// a pod group, it returns the priority of the pod group.
// Otherwise, it returns the pod's own priority.
func (ev *PodGroupEvaluator) getPodPriority(p *v1.Pod) int32 {
	if pg := getPodGroup(p, ev.podGroupLister); pg != nil {
		return util.PodGroupPriority(pg)
	}
	return corev1helpers.PodPriority(p)
}

// moreImportantVictim decides which of two preemption units is considered more critical.
// This function is dedidcated only for PodGroup preemption.
//
// The comparison logic follows this strict hierarchy:
//
//  1. Priority: Higher priority units are always more important.
//
//  2. Workload Type (if WorkloadAwarePreemption is enabled):
//     Atomic workloads (PodGroups) are considered more important than individual Pods
//     of the same priority.
//
//  3. Start Time (for Single Pods):
//     If both units are single Pods, the one with the older StartTime is more important.
//     This honors "first-come, first-served".
//
//  4. Group Size (for PodGroups):
//     If both units are PodGroups, the one with more members (larger size) is considered
//     more important. This avoids the high cost of rescheduling massive jobs.
//
//  5. Start Time (Tie-breaker for PodGroups):
//     If sizes are equal, the group that started earlier (has the oldest pod)
//     is more important.
func moreImportantVictim(vi1, vi2 *victim) bool {
	if vi1.Priority() != vi2.Priority() {
		return vi1.Priority() > vi2.Priority()
	}

	if vi1.IsPodGroup() != vi2.IsPodGroup() {
		return vi1.IsPodGroup()
	}

	if !vi1.IsPodGroup() {
		return vi1.EarliestStartTime().Before(vi2.EarliestStartTime())
	}

	if len(vi1.Pods()) != len(vi2.Pods()) {
		return len(vi1.Pods()) > len(vi2.Pods())
	}

	t1 := vi1.EarliestStartTime()
	t2 := vi2.EarliestStartTime()
	return t1.Before(t2)
}
