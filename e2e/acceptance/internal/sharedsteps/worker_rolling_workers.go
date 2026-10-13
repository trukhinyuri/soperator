package sharedsteps

import (
	"context"
	"fmt"
	"slices"
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

func isEphemeralRollout(nodeSet kubeobjects.NodeSet) bool {
	return nodeSet.Spec.EphemeralNodes != nil && *nodeSet.Spec.EphemeralNodes
}

func (s *WorkerRollingUpdate) rolloutWorkerNames(ctx context.Context, nodeSet kubeobjects.NodeSet) ([]string, error) {
	var active []int32
	if isEphemeralRollout(nodeSet) {
		var powerState kubeobjects.NodeSetPowerState
		if err := s.kubectl.GetJSON(ctx, &powerState, "get", "nodesetpowerstate", nodeSet.Metadata.Name,
			"-n", nodeSet.Metadata.Namespace, "-o", "json"); err != nil {
			return nil, fmt.Errorf("read active ordinals for NodeSet %s: %w", nodeSet.Metadata.Name, err)
		}
		active = powerState.Spec.ActiveNodes
	}
	return rolloutWorkerNames(nodeSet, active)
}

func rolloutWorkerNames(nodeSet kubeobjects.NodeSet, active []int32) ([]string, error) {
	if nodeSet.Spec.Replicas < 0 {
		return nil, fmt.Errorf("validate NodeSet %s replicas: %d", nodeSet.Metadata.Name, nodeSet.Spec.Replicas)
	}
	if !isEphemeralRollout(nodeSet) {
		active = make([]int32, nodeSet.Spec.Replicas)
		for ordinal := range active {
			active[ordinal] = int32(ordinal)
		}
	}
	names := make([]string, 0, len(active))
	seen := make(map[int32]bool, len(active))
	for _, ordinal := range active {
		if ordinal < 0 || ordinal >= nodeSet.Spec.Replicas || seen[ordinal] {
			return nil, fmt.Errorf("validate NodeSet %s active ordinals: duplicate or out-of-range ordinal %d for replicas=%d",
				nodeSet.Metadata.Name, ordinal, nodeSet.Spec.Replicas)
		}
		seen[ordinal] = true
		names = append(names, fmt.Sprintf("%s-%d", nodeSet.Metadata.Name, ordinal))
	}
	sort.Strings(names)
	return names, nil
}

// Membership must stay fixed while its snapshot defines replacement and budget
// assertions. Idle power saving is excluded, but external power changes can still
// invalidate the experiment and must not silently reduce its coverage.
func (s *WorkerRollingUpdate) checkRolloutWorkerSet(ctx context.Context) error {
	if !isEphemeralRollout(s.nodeSet) || len(s.workerNames) == 0 {
		return nil
	}
	names, err := s.rolloutWorkerNames(ctx, s.nodeSet)
	if err != nil {
		return err
	}
	if !slices.Equal(names, s.workerNames) {
		return rolloutInvariant("NodeSet %s active workers changed during rollout: expected %v, found %v", s.nodeSet.Metadata.Name, s.workerNames, names)
	}
	return s.checkEphemeralWorkerHold(ctx)
}

func requireRolloutPodNames(names []string, pods []corev1.Pod) error {
	actual := make([]string, 0, len(pods))
	for _, pod := range pods {
		actual = append(actual, pod.Name)
	}
	sort.Strings(actual)
	if !slices.Equal(names, actual) {
		return fmt.Errorf("match active worker pods: expected %v, found %v", names, actual)
	}
	return nil
}

func selectRolloutWorkers(names []string, pool []framework.WorkerInfo) ([]framework.WorkerInfo, error) {
	byName := make(map[string]framework.WorkerInfo, len(pool))
	for _, worker := range pool {
		if _, duplicate := byName[worker.Name]; duplicate {
			return nil, fmt.Errorf("find unique Slurm worker %s", worker.Name)
		}
		byName[worker.Name] = worker
	}
	workers := make([]framework.WorkerInfo, 0, len(names))
	for _, name := range names {
		worker, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("find active worker %s in Slurm main partition", name)
		}
		workers = append(workers, worker)
	}
	return workers, nil
}
