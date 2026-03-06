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
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

type WorkloadExecutor struct {
	Name string

	Handler   fwk.Handle
	PodLister corelisters.PodLister
	PdbLister policylisters.PodDisruptionBudgetLister

	podGroupSchedulingFunc func(context.Context) *fwk.Status
}

func NewWorkloadExecutor(name string, fh fwk.Handle, podGroupSchedulingFunc func(context.Context) *fwk.Status) *WorkloadExecutor {
	return &WorkloadExecutor{
		Name:                   name,
		Handler:                fh,
		PodLister:              fh.SharedInformerFactory().Core().V1().Pods().Lister(),
		PdbLister:              fh.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister(),
		podGroupSchedulingFunc: podGroupSchedulingFunc,
	}
}

// Preempt implements the preemption logic where the preemptor is a pod group
// and the domain is the whole cluster. It returns a status together with the list of victims
// that should be preempted in order to make enough room for the pod group to be scheduled.
// The preemption logic actuates the NodeInfo provided by a Handler
// The caller is expected to snapshot the NodeInfo before calling this function
// And rollback the state to the snapshot after function is finished.
func (ev *WorkloadExecutor) Preempt(ctx context.Context, cycleStates []fwk.CycleState, podGroup []*v1.Pod) (*fwk.Status, []*v1.Pod) {
	// In case of workload-aware preemption, the domain is whole cluster.
	// We do not make a snapshot of node info. Those nodes will be shared
	// with the PodGroup scheduling algorithm passed as podGroupSchedulingFunc.
	allNodes, err := ev.Handler.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return fwk.AsStatus(err), nil
	}
	domain := NewDomainForWorkloadPreemption(allNodes, "cluster-domain")

	// TODO(Argh4k): use v1alpha2.PodGroup when creating the preemptor.
	preemptor := NewPodGroupPreemptor(podGroup, cycleStates)
	pdbs, err := getPodDisruptionBudgets(ev.PdbLister)
	if err != nil {
		return fwk.AsStatus(err), nil
	}

	victims, status := ev.SelectVictimsOnDomain(ctx, preemptor, domain, pdbs)

	return status, victims
}

func (ev *WorkloadExecutor) SelectVictimsOnDomain(
	ctx context.Context,
	preemptor Preemptor,
	domain Domain,
	pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, *fwk.Status) {
	logger := klog.FromContext(ctx)
	nameToNode := make(map[string]fwk.NodeInfo)
	for _, nodeInfo := range domain.Nodes() {
		nameToNode[nodeInfo.Node().Name] = nodeInfo
	}

	// Compared to the default preemption algorithm
	// do not run the runPreFilterExtensionRemovePod
	// as pod group scheduling does prefilter anyway.
	removePods := func(pu PreemptionUnit) error {
		for _, pi := range pu.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
				return err
			}
		}

		return nil
	}
	addPods := func(pu PreemptionUnit) error {
		for _, pi := range pu.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			nodeInfo.AddPodInfo(pi)
		}

		return nil
	}

	var potentialVictims []PreemptionUnit
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

	if status := ev.podGroupSchedulingFunc(ctx); !status.IsSuccess() {
		return nil, status
	}

	sort.Slice(potentialVictims, func(i, j int) bool {
		return ev.moreImportantVictim(potentialVictims[i], potentialVictims[j])
	})

	violatingVictims, nonViolatingVictims := FilterVictimsWithPDBViolation(potentialVictims, pdbs)
	numViolatingVictim := 0

	reprieveVictim := func(v PreemptionUnit) (bool, error) {
		if err := addPods(v); err != nil {
			return false, err
		}

		status := ev.podGroupSchedulingFunc(ctx)
		fits := status.IsSuccess()
		if !fits {
			if err := removePods(v); err != nil {
				return false, err
			}
			var names []string
			for _, p := range v.Pods() {
				names = append(names, p.GetPod().Name)
			}
			pods := strings.Join(names, ",")
			logger.V(5).Info("Pods are potential preemption victims on domain", "pods", pods, "domain", domain.GetName())
		}

		return fits, nil
	}

	var victimsToPreempt []PreemptionUnit
	for _, v := range violatingVictims {
		if fits, err := reprieveVictim(v); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			victimsToPreempt = append(victimsToPreempt, v)
			numViolatingVictim++
		}
	}

	victimsToPreempt = append(victimsToPreempt, nonViolatingVictims...)
	sort.Slice(victimsToPreempt, func(i, j int) bool {
		return ev.moreImportantVictim(victimsToPreempt[i], victimsToPreempt[j])
	})
	var podsToPreempt []*v1.Pod
	for _, v := range victimsToPreempt {
		if fits, err := reprieveVictim(v); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			for _, pi := range v.Pods() {
				podsToPreempt = append(podsToPreempt, pi.GetPod())
			}
		}
	}

	return podsToPreempt, nil
}

// isPreemptionAllowed returns whether the victim residing on nodeInfo can be preempted by the preemptor
func (pl *WorkloadExecutor) isPreemptionAllowed(victim PreemptionUnit, preemptor Preemptor) bool {
	// The victim must have lower priority than the preemptor, in addition to any filtering implemented by IsEligiblePreemptor
	return victim.Priority() < preemptor.Priority()
}

// moreImportantVictim resolves the pods from the PreemptionUnits and delegates to the priorityFunc
// to determine which victim is more important.
func (pl *WorkloadExecutor) moreImportantVictim(victim1, victim2 PreemptionUnit) bool {
	var pods1 []*v1.Pod
	var pods2 []*v1.Pod
	for _, pi := range victim1.Pods() {
		pods1 = append(pods1, pi.GetPod())
	}
	for _, pi := range victim2.Pods() {
		pods2 = append(pods2, pi.GetPod())
	}

	return util.MoreImportantVictim(pods1, pods2, true)
}
