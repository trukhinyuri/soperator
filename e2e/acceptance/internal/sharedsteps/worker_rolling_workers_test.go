package sharedsteps

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

func rolloutWorkerTestNodeSet(ephemeral bool) kubeobjects.NodeSet {
	return kubeobjects.NodeSet{
		Metadata: kubeobjects.ObjectMeta{Name: "worker", Namespace: "soperator"},
		Spec: kubeobjects.NodeSetSpec{
			ClusterName: "soperator", Replicas: 10, EphemeralNodes: new(ephemeral),
			GPU: kubeobjects.NodeSetGPUSpec{Enabled: true}, UpdateStrategy: kubeobjects.NodeSetUpdateStrategySlurmAwareRollingUpdate,
			MaxUnavailable: new(intstr.FromString("50%")),
		},
		Status: kubeobjects.NodeSetStatus{Phase: kubeobjects.NodeSetPhaseReady, Replicas: 2},
	}
}

func TestRolloutWorkerNamesUseStaticReplicasOrSparseActiveOrdinals(t *testing.T) {
	static := rolloutWorkerTestNodeSet(false)
	static.Spec.Replicas = 3
	for _, enabled := range []*bool{nil, new(false)} {
		static.Spec.EphemeralNodes = enabled
		names, err := rolloutWorkerNames(static, []int32{2})
		require.NoError(t, err)
		assert.Equal(t, []string{"worker-0", "worker-1", "worker-2"}, names)
	}
	ephemeral := rolloutWorkerTestNodeSet(true)
	names, err := rolloutWorkerNames(ephemeral, []int32{8, 3})
	require.NoError(t, err)
	assert.Equal(t, []string{"worker-3", "worker-8"}, names)
	names, err = rolloutWorkerNames(ephemeral, nil)
	require.NoError(t, err)
	assert.Empty(t, names, "no active workers must not be replaced by configured capacity")
	for _, active := range [][]int32{{-1}, {10}, {3, 3}} {
		_, err := rolloutWorkerNames(ephemeral, active)
		require.ErrorContains(t, err, "duplicate or out-of-range ordinal")
	}
}

func TestRolloutRequiresExactActivePodMembership(t *testing.T) {
	names := []string{"worker-3", "worker-8"}
	pod := func(name string) corev1.Pod { return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}} }
	require.NoError(t, requireRolloutPodNames(names, []corev1.Pod{pod("worker-8"), pod("worker-3")}))
	for _, pods := range [][]corev1.Pod{
		{pod("worker-3")},
		{pod("worker-3"), pod("worker-4")},
		{pod("worker-3"), pod("worker-3")},
		{pod("worker-3"), pod("worker-8"), pod("worker-9")},
	} {
		require.ErrorContains(t, requireRolloutPodNames(names, pods), "match active worker pods")
	}
}

func TestSelectRolloutWorkersIgnoresInactiveSlurmNodes(t *testing.T) {
	pool := []framework.WorkerInfo{
		{Name: "worker-0", SlurmNode: framework.SlurmNodeInfo{State: "IDLE+CLOUD+POWERED_DOWN"}},
		{Name: "worker-3", SlurmNode: framework.SlurmNodeInfo{State: "IDLE+CLOUD"}},
		{Name: "worker-8", SlurmNode: framework.SlurmNodeInfo{State: "IDLE+CLOUD"}},
	}
	workers, err := selectRolloutWorkers([]string{"worker-3", "worker-8"}, pool)
	require.NoError(t, err)
	assert.Equal(t, pool[1:], workers)
	_, err = selectRolloutWorkers([]string{"worker-3", "worker-9"}, pool)
	require.ErrorContains(t, err, "find active worker worker-9")
	_, err = selectRolloutWorkers([]string{"worker-3"}, append(pool, pool[1]))
	require.ErrorContains(t, err, "find unique Slurm worker worker-3")
}

func TestSparseRolloutReplacementAndActiveReadiness(t *testing.T) {
	nodeSet := rolloutWorkerTestNodeSet(true)
	assert.Empty(t, workerRolloutCandidateProblem(nodeSet, "soperator", 2))
	nodeSet.Status.Replicas = 1
	assert.Equal(t, "phase=Ready ready=1/2", workerRolloutCandidateProblem(nodeSet, "soperator", 2))
	initial := map[string]workerPodSnapshot{"worker-3": {UID: "old-3"}, "worker-8": {UID: "old-8"}}
	annotations := map[string]string{workerRolloutAnnotationKey: "new"}
	pods := []corev1.Pod{readyWorkerPod("worker-3", "new-3", annotations), readyWorkerPod("worker-8", "new-8", annotations)}
	assert.Empty(t, replacedWorkerPods(initial, pods, "new"))
	assert.Equal(t, []string{"worker-8 (missing)"}, replacedWorkerPods(initial, pods[:1], "new"))
	pods[1].UID = "old-8"
	assert.Equal(t, []string{"worker-8 (old pod)"}, replacedWorkerPods(initial, pods, "new"))
}

func TestRolloutIdleAcceptsCloudAndRejectsPowerTransitions(t *testing.T) {
	for _, state := range []string{"IDLE", "IDLE+CLOUD"} {
		assert.Empty(t, idleProblems([]framework.SlurmNodeInfo{{Name: "worker-3", State: state}}))
	}
	for _, flag := range []string{"POWER_DOWN", "POWERING_DOWN", "POWERED_DOWN", "POWER_UP", "POWERING_UP"} {
		assert.NotEmpty(t, idleProblems([]framework.SlurmNodeInfo{{Name: "worker-3", State: "IDLE+CLOUD+" + flag}}), flag)
	}
}

type rolloutWorkersRuntime struct {
	framework.Runtime
	nodeSet kubeobjects.NodeSet
	active  []int32
	pods    []corev1.Pod
	calls   []string
}

func (r *rolloutWorkersRuntime) Kubectl() framework.ArgsScope {
	return framework.NewArgsScope(func(_ context.Context, args ...string) (string, error) {
		r.calls = append(r.calls, "kubectl "+strings.Join(args, " "))
		var object any
		switch args[1] {
		case "nodesets":
			object = kubeobjects.NodeSetList{Items: []kubeobjects.NodeSet{r.nodeSet}}
		case "nodesetpowerstate":
			object = kubeobjects.NodeSetPowerState{Spec: kubeobjects.NodeSetPowerStateSpec{ActiveNodes: r.active}}
		case "pods":
			object = corev1.PodList{Items: r.pods}
		default:
			return "", fmt.Errorf("unexpected kubectl command: %v", args)
		}
		data, err := json.Marshal(object)
		return string(data), err
	})
}

func (r *rolloutWorkersRuntime) Controller() framework.CommandScope {
	return framework.NewCommandScope(func(_ context.Context, command string) (string, error) {
		r.calls = append(r.calls, command)
		switch {
		case command == "scontrol show config":
			return "SuspendExcNodes = worker-0,worker-3\n", nil
		case strings.HasPrefix(command, "scontrol show hostnames "):
			return "worker-0\nworker-3\n", nil
		case strings.HasPrefix(command, "sinfo "):
			var names []string
			for ordinal := range r.nodeSet.Spec.Replicas {
				names = append(names, fmt.Sprintf("worker-%d", ordinal))
			}
			return strings.Join(names, "\n"), nil
		case strings.HasPrefix(command, "scontrol show node "):
			state := "IDLE+CLOUD+POWERED_DOWN"
			if command == "scontrol show node 'worker-0'" || command == "scontrol show node 'worker-3'" {
				state = "IDLE+CLOUD"
			}
			return "State=" + state, nil
		default:
			return "", fmt.Errorf("unexpected Slurm command: %s", command)
		}
	})
}

func (r *rolloutWorkersRuntime) WaitFor(ctx context.Context, description string, _, _ time.Duration, condition func(context.Context) (bool, error)) error {
	done, err := condition(ctx)
	if err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("%s remains pending", description)
	}
	return nil
}

func (*rolloutWorkersRuntime) Logf(string, ...any) {}

func TestSelectNodeSetSupportsSparseEphemeralWorkers(t *testing.T) {
	runtime := &rolloutWorkersRuntime{nodeSet: rolloutWorkerTestNodeSet(true), active: []int32{0, 3}}
	for _, ordinal := range runtime.active {
		name := fmt.Sprintf("worker-%d", ordinal)
		pod := readyWorkerPod(name, types.UID(name+"-uid"), nil)
		pod.ResourceVersion = "1"
		pod.Spec.NodeName = fmt.Sprintf("host-%d", ordinal)
		pod.Status.StartTime = new(metav1.Now())
		runtime.pods = append(runtime.pods, pod)
	}
	kubectl := framework.NewKubectlClient(runtime)
	slurm := framework.NewSlurmClient(runtime)
	steps := NewWorkerRollingUpdate(&framework.ClusterInfo{SlurmClusterName: "soperator"}, runtime, slurm, kubectl,
		framework.NewWorkerSelector(kubectl, slurm, "soperator"))
	require.NoError(t, steps.selectNodeSet(t.Context()))
	assert.Equal(t, []string{"worker-0", "worker-3"}, steps.workerNames)
	require.Len(t, steps.workers, 2)
	assert.Equal(t, "worker-3", steps.workers[1].Name)
	assert.Len(t, steps.initialPods, 2)
	assert.Equal(t, 1, steps.rolloutBudget, "50%% maxUnavailable scales against two active workers, not ten configured")
	assert.Empty(t, steps.power.added, "preexisting exclusions remain unowned")
	assert.NotContains(t, strings.Join(runtime.calls, "\n"), "scontrol update")

	runtime.active = []int32{0, 4}
	called := false
	err := steps.waitForRollout(t.Context(), "detect external power change", time.Minute, time.Second, func(context.Context) (bool, error) {
		called = true
		return true, nil
	})
	var invariant rolloutInvariantError
	require.ErrorAs(t, err, &invariant)
	assert.Contains(t, err.Error(), "active workers changed during rollout")
	assert.False(t, called, "active membership drift must fail before a successful condition can mask it")
}

func TestSelectNodeSetWithNoActiveWorkersSkips(t *testing.T) {
	runtime := &rolloutWorkersRuntime{nodeSet: rolloutWorkerTestNodeSet(true)}
	runtime.nodeSet.Status.Replicas = 0
	steps := &WorkerRollingUpdate{info: &framework.ClusterInfo{SlurmClusterName: "soperator"}, runtime: runtime, kubectl: framework.NewKubectlClient(runtime)}
	require.ErrorIs(t, steps.selectNodeSet(t.Context()), godog.ErrSkip)
	assert.Empty(t, steps.power.names)
	assert.NotContains(t, strings.Join(runtime.calls, "\n"), "scontrol")
}
