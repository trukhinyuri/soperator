package sharedsteps

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

const (
	rolloutObservedOperationID    = "slurm.nebius.ai/worker-operation-id"
	rolloutObservedOperationPhase = "slurm.nebius.ai/worker-operation-phase"
)

type rolloutBudgetObservation struct {
	Time    time.Time                  `json:"time"`
	NodeSet string                     `json:"nodeset"`
	Budget  int                        `json:"budget"`
	Used    int                        `json:"observed_unavailable"`
	Workers []rolloutWorkerObservation `json:"workers"`
}

type rolloutWorkerObservation struct {
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resource_version,omitempty"`
	Node            string `json:"node,omitempty"`
	PodPhase        string `json:"pod_phase,omitempty"`
	Ready           bool   `json:"ready"`
	Deleting        bool   `json:"deleting,omitempty"`
	OperationID     string `json:"operation_id,omitempty"`
	OperationPhase  string `json:"operation_phase,omitempty"`
	SlurmState      string `json:"slurm_state,omitempty"`
	DrainReason     string `json:"drain_reason,omitempty"`
	BudgetEvidence  string `json:"budget_evidence,omitempty"`
}

// observeRollout samples a lower bound on unavailable workers and accepted
// handoffs. It deliberately does not reconstruct the controller's entire
// decision tree: a queued outdated pod, an ordinary drain, and a stale recovery
// label alone are not evidence that a rollout slot is occupied.
func (s *WorkerRollingUpdate) observeRollout(ctx context.Context) error {
	if err := s.checkRolloutWorkerSet(ctx); err != nil {
		return err
	}
	if len(s.initialPods) == 0 || s.rolloutBudget == 0 {
		return nil
	}
	if s.rolloutBudget < 1 {
		return fmt.Errorf("observe NodeSet %s rollout: invalid maxUnavailable budget %d", s.nodeSet.Metadata.Name, s.rolloutBudget)
	}
	observed, err := s.collectRolloutObservation(ctx)
	if err != nil {
		return err
	}
	s.recordRolloutObservation(ctx, observed)
	if observed.Used <= observed.Budget {
		return nil
	}

	// Kubernetes and Slurm are read separately. Never turn a mixture of an old
	// worker's handoff and its successor's handoff into a safety failure. Confirm
	// both the pod identities/resource versions and the evidence in a fresh read.
	confirmed, err := s.collectRolloutObservation(ctx)
	if err != nil {
		return err
	}
	s.recordRolloutObservation(ctx, confirmed)
	if confirmed.Used <= confirmed.Budget {
		return nil
	}
	if !sameRolloutBudgetObservation(observed, confirmed) {
		return fmt.Errorf("NodeSet %s rollout changed during budget observation (%d then %d occupied slots, budget %d); awaiting a coherent sample",
			s.nodeSet.Metadata.Name, observed.Used, confirmed.Used, observed.Budget)
	}
	return rolloutInvariant("NodeSet %s exceeded maxUnavailable=%d: %d workers unavailable or in accepted handoff: %s",
		s.nodeSet.Metadata.Name, confirmed.Budget, confirmed.Used, rolloutBudgetDiagnostic(confirmed))
}

func (s *WorkerRollingUpdate) collectRolloutObservation(ctx context.Context) (rolloutBudgetObservation, error) {
	pods, err := s.nodeSetPods(ctx)
	if err != nil {
		return rolloutBudgetObservation{}, err
	}
	byName := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		if _, expected := s.initialPods[pod.Name]; !expected {
			return rolloutBudgetObservation{}, rolloutInvariant("NodeSet %s gained unexpected worker pod %s during rollout", s.nodeSet.Metadata.Name, pod.Name)
		}
		if _, exists := byName[pod.Name]; exists {
			return rolloutBudgetObservation{}, fmt.Errorf("observe NodeSet %s rollout: duplicate pod %s", s.nodeSet.Metadata.Name, pod.Name)
		}
		byName[pod.Name] = pod
	}
	names := make([]string, 0, len(s.initialPods))
	for name := range s.initialPods {
		names = append(names, name)
	}
	sort.Strings(names)
	observation := rolloutBudgetObservation{Time: time.Now().UTC(), NodeSet: s.nodeSet.Metadata.Name, Budget: s.rolloutBudget}
	for _, name := range names {
		var pod *corev1.Pod
		if current, exists := byName[name]; exists {
			pod = &current
			if pod.UID == "" || pod.ResourceVersion == "" {
				return rolloutBudgetObservation{}, fmt.Errorf("observe worker %s: missing pod UID or resource version", name)
			}
		}
		var node *framework.SlurmNodeInfo
		if rolloutBudgetNeedsSlurm(pod) {
			current, err := s.slurm.NodeInfoOnce(ctx, name)
			if err != nil {
				return rolloutBudgetObservation{}, fmt.Errorf("observe handoff for worker %s: %w", name, err)
			}
			if strings.TrimSpace(current.State) == "" {
				return rolloutBudgetObservation{}, fmt.Errorf("observe handoff for worker %s: missing Slurm state", name)
			}
			node = &current
		}
		worker := observeRolloutWorker(name, pod, node)
		if worker.BudgetEvidence != "" {
			observation.Used++
		}
		observation.Workers = append(observation.Workers, worker)
	}
	return observation, nil
}

func rolloutBudgetNeedsSlurm(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || !kubeobjects.PodReady(*pod) || pod.Labels[rolloutObservedOperationID] == "" {
		return false
	}
	switch pod.Labels[rolloutObservedOperationPhase] {
	case "stopping", "recovering":
		return true
	default:
		return false
	}
}

func observeRolloutWorker(name string, pod *corev1.Pod, node *framework.SlurmNodeInfo) rolloutWorkerObservation {
	worker := rolloutWorkerObservation{Name: name}
	if pod == nil {
		worker.BudgetEvidence = "missing pod"
		return worker
	}
	worker.UID = string(pod.UID)
	worker.ResourceVersion = pod.ResourceVersion
	worker.Node = pod.Spec.NodeName
	worker.PodPhase = string(pod.Status.Phase)
	worker.Ready = kubeobjects.PodReady(*pod)
	worker.Deleting = pod.DeletionTimestamp != nil
	worker.OperationID = pod.Labels[rolloutObservedOperationID]
	worker.OperationPhase = pod.Labels[rolloutObservedOperationPhase]
	if node != nil {
		worker.SlurmState = node.State
		worker.DrainReason = node.Reason
	}
	// A worker occupies at most one slot, even if it is both unavailable and in
	// handoff. A Ready replacement with no operation identity can retain stale
	// Slurm flags while registering; those flags do not imply a new handoff.
	switch {
	case worker.Deleting:
		worker.BudgetEvidence = "terminating pod"
	case !worker.Ready:
		worker.BudgetEvidence = "pod not Ready"
	case worker.OperationID == "":
		return worker
	case worker.OperationPhase == "acknowledged" || worker.OperationPhase == "ready":
		worker.BudgetEvidence = "handoff " + worker.OperationPhase
	case (worker.OperationPhase == "stopping" || worker.OperationPhase == "recovering") && node != nil:
		switch {
		case node.HasStateFlag("REBOOT_REQUESTED") || node.HasStateFlag("REBOOT_ISSUED"):
			worker.BudgetEvidence = "Slurm reboot in progress"
		case worker.OperationPhase == "recovering" && rolloutObservedNodeFailureDrain(*node):
			worker.BudgetEvidence = "recovered worker awaiting node maintenance"
		}
	}
	return worker
}

// A Ready worker with an existing failure drain uses a rollout slot only after
// an accepted handoff. A recovering worker keeps that reservation while the
// documented node failure remains; user/software/manual drains alone do not.
func rolloutObservedNodeFailureDrain(node framework.SlurmNodeInfo) bool {
	if !node.HasStateFlag("DRAIN") {
		return false
	}
	for _, reason := range []string{"Kill task failed", "[compute_maintenance] node replacement process", "[compute_maintenance] node reboot process", "[node_problem]"} {
		if strings.Contains(node.Reason, reason) {
			return true
		}
	}
	// Health-check reasons can be appended to an existing reason; user problems
	// take precedence over a hardware reason, as in maintenance classification.
	return !strings.Contains(node.Reason, "[user_problem]") && strings.Contains(node.Reason, "[hardware_problem]")
}

func sameRolloutBudgetObservation(first, second rolloutBudgetObservation) bool {
	if first.NodeSet != second.NodeSet || first.Budget != second.Budget || first.Used != second.Used || len(first.Workers) != len(second.Workers) {
		return false
	}
	for i := range first.Workers {
		if first.Workers[i] != second.Workers[i] {
			return false
		}
	}
	return true
}

func rolloutBudgetDiagnostic(observation rolloutBudgetObservation) string {
	var workers []string
	for _, worker := range observation.Workers {
		if worker.BudgetEvidence != "" {
			workers = append(workers, fmt.Sprintf("%s uid=%s node=%s phase=%s slurm=%s (%s)",
				worker.Name, worker.UID, worker.Node, worker.OperationPhase, worker.SlurmState, worker.BudgetEvidence))
		}
	}
	return strings.Join(workers, "; ")
}

func (s *WorkerRollingUpdate) recordRolloutObservation(ctx context.Context, observation rolloutBudgetObservation) {
	artifacts, ok := framework.ScenarioArtifacts(ctx)
	if !ok || artifacts.RunnerDir == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(artifacts.RunnerDir, "worker-rollout-observations.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err == nil {
		err = json.NewEncoder(file).Encode(observation)
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		s.runtime.Logf("record rollout observation for NodeSet %s: %v", observation.NodeSet, err)
	}
}
