package sharedsteps

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

func TestObserveRolloutWorkerBudgetEvidence(t *testing.T) {
	tests := []struct {
		name      string
		phase     string
		state     string
		reason    string
		mutate    func(*corev1.Pod)
		wantUsed  bool
		wantSlurm bool
	}{
		{name: "ready untouched"},
		{name: "stopping requested", phase: "stopping", state: "ALLOCATED+DRAIN+REBOOT_REQUESTED", wantUsed: true, wantSlurm: true},
		{name: "stopping issued", phase: "stopping", state: "DOWN+DRAIN+REBOOT_ISSUED", wantUsed: true, wantSlurm: true},
		{name: "stopping before reboot accepted", phase: "stopping", state: "IDLE", wantSlurm: true},
		{name: "failure before reboot accepted", phase: "stopping", state: "IDLE+DRAIN", reason: "[hardware_problem] e2e", wantSlurm: true},
		{name: "acknowledged", phase: "acknowledged", wantUsed: true},
		{name: "released for eviction", phase: "ready", wantUsed: true},
		{name: "recovering with reboot", phase: "recovering", state: "DOWN+DRAIN+REBOOT_ISSUED", wantUsed: true, wantSlurm: true},
		{name: "recovered with hardware drain", phase: "recovering", state: "IDLE+DRAIN", reason: "[hardware_problem] e2e", wantUsed: true, wantSlurm: true},
		{name: "recovered with node drain", phase: "recovering", state: "IDLE+DRAIN", reason: "[node_problem] e2e", wantUsed: true, wantSlurm: true},
		{name: "recovered fault cleared", phase: "recovering", state: "IDLE", wantSlurm: true},
		{name: "recovered user drain", phase: "recovering", state: "IDLE+DRAIN", reason: "[user_problem] e2e", wantSlurm: true},
		{name: "recovered software drain", phase: "recovering", state: "IDLE+DRAIN", reason: "[software_problem] nvidia_libraries", wantSlurm: true},
		{name: "recovered manual drain", phase: "recovering", state: "IDLE+DRAIN", reason: "manual", wantSlurm: true},
		{name: "preexisting fault is not handoff", state: "IDLE+DRAIN", reason: "[hardware_problem] e2e"},
		{name: "new replacement with stale Slurm flags", state: "IDLE+DRAIN+REBOOT_REQUESTED"},
		{name: "unknown operation phase", phase: "unexpected", state: "IDLE+DRAIN+REBOOT_REQUESTED"},
		{name: "malformed reboot state", phase: "stopping", state: "IDLE+REBOOT_REQUESTED_bad", wantSlurm: true},
		{name: "unknown Slurm state", phase: "stopping", state: "unknown", wantSlurm: true},
		{name: "missing Slurm state", phase: "stopping", wantSlurm: true},
		{name: "missing operation identity", phase: "acknowledged", mutate: func(p *corev1.Pod) { delete(p.Labels, rolloutObservedOperationID) }},
		{name: "not ready", mutate: func(p *corev1.Pod) { p.Status.Conditions = nil }, wantUsed: true},
		{name: "not ready in handoff counted once", phase: "stopping", state: "DOWN+REBOOT_ISSUED", mutate: func(p *corev1.Pod) { p.Status.Conditions = nil }, wantUsed: true},
		{name: "terminating but ready", mutate: func(p *corev1.Pod) { p.DeletionTimestamp = new(metav1.Now()) }, wantUsed: true},
		{name: "terminating with handoff counted once", phase: "acknowledged", mutate: func(p *corev1.Pod) { p.DeletionTimestamp = new(metav1.Now()) }, wantUsed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := readyWorkerPod("worker-0", "uid-0", nil)
			pod.ResourceVersion = "15"
			pod.Spec.NodeName = "node-0"
			pod.Labels = make(map[string]string)
			if tt.phase != "" {
				pod.Labels[rolloutObservedOperationID] = "revision-2"
				pod.Labels[rolloutObservedOperationPhase] = tt.phase
			}
			if tt.mutate != nil {
				tt.mutate(&pod)
			}
			node := &framework.SlurmNodeInfo{Name: pod.Name, State: tt.state, Reason: tt.reason}
			observed := observeRolloutWorker(pod.Name, &pod, node)
			assert.Equal(t, tt.wantUsed, observed.BudgetEvidence != "", observed)
			assert.Equal(t, tt.wantSlurm, rolloutBudgetNeedsSlurm(&pod))
			assert.Equal(t, "uid-0", observed.UID)
			assert.Equal(t, "15", observed.ResourceVersion)
			assert.Equal(t, "node-0", observed.Node)
		})
	}
	missing := observeRolloutWorker("worker-0", nil, nil)
	assert.Equal(t, "missing pod", missing.BudgetEvidence)
	assert.False(t, rolloutBudgetNeedsSlurm(nil))
}

func TestRolloutObservedNodeFailureDrain(t *testing.T) {
	for _, reason := range []string{
		"Kill task failed: test", "[compute_maintenance] node replacement process",
		"[compute_maintenance] node reboot process", "[node_problem] check failed",
		"[hardware_problem] check failed", "soperator rolling update : [hardware_problem] check failed : reboot issued",
		"[node_problem] check failed : [user_problem] memory pressure",
	} {
		t.Run(reason, func(t *testing.T) {
			assert.True(t, rolloutObservedNodeFailureDrain(framework.SlurmNodeInfo{State: "IDLE+DRAIN", Reason: reason}))
			assert.False(t, rolloutObservedNodeFailureDrain(framework.SlurmNodeInfo{State: "IDLE", Reason: reason}))
		})
	}
	for _, reason := range []string{"", "manual", "[user_problem] alloc_mem_used", "[software_problem] nvidia_libraries", "[user_problem] test : [hardware_problem] test"} {
		assert.False(t, rolloutObservedNodeFailureDrain(framework.SlurmNodeInfo{State: "IDLE+DRAIN", Reason: reason}), reason)
	}
}

func TestSameRolloutBudgetObservation(t *testing.T) {
	first := rolloutBudgetObservation{Time: time.Now(), NodeSet: "worker", Budget: 1, Used: 2, Workers: []rolloutWorkerObservation{
		{Name: "worker-0", UID: "uid-0", ResourceVersion: "2", BudgetEvidence: "terminating pod"},
		{Name: "worker-1", UID: "uid-1", ResourceVersion: "3", OperationID: "revision", OperationPhase: "stopping", SlurmState: "IDLE+DRAIN+REBOOT_REQUESTED", BudgetEvidence: "Slurm reboot in progress"},
	}}
	second := first
	second.Time = second.Time.Add(time.Second)
	assert.True(t, sameRolloutBudgetObservation(first, second), "timestamps do not affect coherence")
	for _, mutate := range []func(*rolloutBudgetObservation){
		func(o *rolloutBudgetObservation) { o.Workers[0].UID = "replacement" },
		func(o *rolloutBudgetObservation) { o.Workers[0].ResourceVersion = "4" },
		func(o *rolloutBudgetObservation) { o.Workers[1].SlurmState = "IDLE" },
		func(o *rolloutBudgetObservation) { o.Workers[1].BudgetEvidence = "" },
		func(o *rolloutBudgetObservation) { o.Used = 1 },
		func(o *rolloutBudgetObservation) { o.Budget = 2 },
		func(o *rolloutBudgetObservation) { o.Workers = o.Workers[:1] },
	} {
		second = first
		second.Workers = append([]rolloutWorkerObservation(nil), first.Workers...)
		mutate(&second)
		assert.False(t, sameRolloutBudgetObservation(first, second))
	}
	assert.Contains(t, rolloutBudgetDiagnostic(first), "worker-0 uid=uid-0")
	assert.Contains(t, rolloutBudgetDiagnostic(first), "worker-1 uid=uid-1")
}

func TestObserveRolloutConfirmsBudgetBeforeLatching(t *testing.T) {
	ready := []corev1.Pod{readyWorkerPod("worker-0", "uid-0", nil), readyWorkerPod("worker-1", "uid-1", nil)}
	for i := range ready {
		ready[i].ResourceVersion = "1"
	}
	unavailable := []corev1.Pod{*ready[0].DeepCopy(), *ready[1].DeepCopy()}
	for i := range unavailable {
		unavailable[i].Status.Conditions = nil
	}
	changed := []corev1.Pod{*unavailable[0].DeepCopy(), *unavailable[1].DeepCopy()}
	changed[0].ResourceVersion = "2"
	mixed := []corev1.Pod{*unavailable[0].DeepCopy(), *ready[1].DeepCopy()}
	mixed[1].Labels = map[string]string{rolloutObservedOperationID: "revision-2", rolloutObservedOperationPhase: "stopping"}

	for _, tt := range []struct {
		name          string
		lists         [][]corev1.Pod
		slurm         map[string]string
		wantViolation bool
		wantReads     int
		wantSlurm     int
	}{
		{name: "stable violation cannot recover into success", lists: [][]corev1.Pod{unavailable, unavailable, ready}, wantViolation: true, wantReads: 2},
		{name: "recovery between snapshots is safe", lists: [][]corev1.Pod{unavailable, ready}, wantReads: 2},
		{name: "changed versions retry before condition", lists: [][]corev1.Pod{unavailable, changed, ready}, wantReads: 3},
		{name: "unavailable and accepted handoff share budget", lists: [][]corev1.Pod{mixed, mixed}, slurm: map[string]string{
			"scontrol show node 'worker-1'": "NodeName=worker-1 State=ALLOCATED+DRAIN+REBOOT_REQUESTED Reason=soperator rolling update",
		}, wantViolation: true, wantReads: 2, wantSlurm: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &rolloutObservationRuntime{podLists: tt.lists, slurm: tt.slurm}
			steps := &WorkerRollingUpdate{
				runtime: runtime, kubectl: framework.NewKubectlClient(runtime), slurm: framework.NewSlurmClient(runtime),
				nodeSet:     kubeobjects.NodeSet{Metadata: kubeobjects.ObjectMeta{Name: "worker", Namespace: "soperator"}, Spec: kubeobjects.NodeSetSpec{Replicas: 2}},
				initialPods: map[string]workerPodSnapshot{"worker-0": {UID: "uid-0"}, "worker-1": {UID: "uid-1"}}, rolloutBudget: 1,
			}
			conditionCalls := 0
			err := steps.waitForRollout(t.Context(), "test rollout budget", time.Second, time.Millisecond, func(context.Context) (bool, error) {
				conditionCalls++
				return true, nil
			})
			if tt.wantViolation {
				require.ErrorContains(t, err, "exceeded maxUnavailable=1")
				var invariant rolloutInvariantError
				require.ErrorAs(t, err, &invariant)
				assert.Zero(t, conditionCalls, "a later success must never replace an observed safety violation")
			} else {
				require.NoError(t, err)
				assert.Equal(t, 1, conditionCalls)
			}
			assert.Equal(t, tt.wantReads, runtime.podReads)
			assert.Equal(t, tt.wantSlurm, runtime.slurmReads)
		})
	}
}

type rolloutObservationRuntime struct {
	rolloutPollRuntime
	podLists   [][]corev1.Pod
	podReads   int
	slurm      map[string]string
	slurmReads int
}

func (r *rolloutObservationRuntime) Kubectl() framework.ArgsScope {
	return framework.NewArgsScope(func(_ context.Context, args ...string) (string, error) {
		if len(args) < 2 || args[0] != "get" || args[1] != "pods" {
			return "", fmt.Errorf("unexpected kubectl args %v", args)
		}
		if r.podReads >= len(r.podLists) {
			return "", fmt.Errorf("unexpected pod read %d", r.podReads+1)
		}
		pods := r.podLists[r.podReads]
		r.podReads++
		data, err := json.Marshal(corev1.PodList{Items: pods})
		return string(data), err
	})
}

func (r *rolloutObservationRuntime) Controller() framework.CommandScope {
	return framework.NewCommandScope(func(_ context.Context, command string) (string, error) {
		r.slurmReads++
		if output, ok := r.slurm[command]; ok {
			return output, nil
		}
		return "", fmt.Errorf("unexpected Slurm command %s", command)
	})
}
