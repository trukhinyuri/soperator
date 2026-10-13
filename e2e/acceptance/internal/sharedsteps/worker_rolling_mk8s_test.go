package sharedsteps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nebius/soperator/e2e/acceptance/framework"
)

func TestNextWorkerMK8sMaxPods(t *testing.T) {
	for _, tt := range []struct {
		name     string
		original *mk8sInteger
		want     mk8sInteger
		wantErr  bool
	}{
		{"omitted default", nil, 111, false},
		{"explicit default", new(mk8sInteger(0)), 111, false},
		{"configured default", new(mk8sInteger(110)), 111, false},
		{"requested increase", new(mk8sInteger(32)), 33, false},
		{"next run toggles back", new(mk8sInteger(33)), 32, false},
		{"CIDR boundary", new(mk8sInteger(128)), 129, false},
		{"maximum", new(mk8sInteger(256)), 255, false},
		{"minimum", new(mk8sInteger(8)), 9, false},
		{"below minimum", new(mk8sInteger(7)), 0, true},
		{"negative", new(mk8sInteger(-1)), 0, true},
		{"invalid", new(mk8sInteger(257)), 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := nextWorkerMK8sMaxPods(tt.original)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWorkerMK8sDecodeAndFingerprint(t *testing.T) {
	input := `{"metadata":{"id":"group-1","resource_version":"12"},"spec":{"fixed_node_count":"2","template":{"max_pods":110,"cloud_init_user_data":"private","resources":{"platform":"gpu"}}},"status":{"state":"RUNNING","target_node_count":"2","node_count":2,"ready_node_count":2,"outdated_node_count":0}}`
	group, err := decodeWorkerMK8sGroup(input)
	require.NoError(t, err)
	require.NotNil(t, group.Spec.Template.MaxPods)
	assert.Equal(t, mk8sInteger(110), *group.Spec.Template.MaxPods)
	assert.Equal(t, mk8sInteger(12), group.Metadata.ResourceVersion)
	require.NoError(t, workerMK8sGroupReady(group, 2))

	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(input), &raw))
	template := raw["spec"].(map[string]any)["template"].(map[string]any)
	template["max_pods"] = 111
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	changed, err := decodeWorkerMK8sGroup(string(encoded))
	require.NoError(t, err)
	assert.Equal(t, group.fingerprint, changed.fingerprint)

	template["cloud_init_user_data"] = "other-private"
	encoded, err = json.Marshal(raw)
	require.NoError(t, err)
	changed, err = decodeWorkerMK8sGroup(string(encoded))
	require.NoError(t, err)
	assert.NotEqual(t, group.fingerprint, changed.fingerprint)
	assert.NotContains(t, changed.fingerprint, "private")

	_, err = decodeWorkerMK8sGroup(`{"metadata":{"id":"group-1"},"spec":{"template":{}}}`)
	require.ErrorContains(t, err, "resource_version")
}

func TestWorkerMK8sUpdateArgsPreserveOtherFields(t *testing.T) {
	assert.Equal(t, []string{"nebius", "mk8s", "node-group", "update",
		"--id", "group-1", "--patch", "--resource-version", "42", "--async", "--format", "json",
		"--template-max-pods", "33"}, workerMK8sUpdateArgs("group-1", 42, 33))
	assert.False(t, sameWorkerMK8sMaxPods(nil, new(mk8sInteger(0))), "selection must detect a concurrent change from omission to an explicit value")
}

func TestWorkerMK8sOperationParsing(t *testing.T) {
	for _, input := range []string{`"operation-1"`, `{"id":"operation-1"}`, "operation-1\n"} {
		id, err := workerMK8sOperationID(input)
		require.NoError(t, err)
		assert.Equal(t, "operation-1", id)
	}
	_, err := workerMK8sOperationID(`{"request":{"secret":"private"}}`)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "private")
	id, err := workerMK8sOperationID("mk8soperation-e00abc123\n")
	require.NoError(t, err)
	assert.Equal(t, "mk8soperation-e00abc123", id)
	_, err = workerMK8sOperationID("request failed")
	require.Error(t, err)

	done, err := workerMK8sOperationCompleted("operation-1", `{"id":"operation-1"}`)
	require.NoError(t, err)
	assert.False(t, done, "missing status is an operation in progress")
	done, err = workerMK8sOperationCompleted("operation-1", `{"id":"operation-1","status":{}}`)
	require.NoError(t, err)
	assert.True(t, done, "present empty status means successful completion")
	_, err = workerMK8sOperationCompleted("operation-1", `{"id":"operation-1","status":{"code":8,"message":"private"}}`)
	require.ErrorContains(t, err, "status code 8")
	assert.NotContains(t, err.Error(), "private")
	var invariant rolloutInvariantError
	assert.True(t, errors.As(err, &invariant))
	_, err = workerMK8sOperationCompleted("operation-1", `{"id":"operation-2","status":{}}`)
	require.ErrorContains(t, err, "expected operation-1")
}

func TestWorkerMK8sDedicatedPods(t *testing.T) {
	hosts := map[string]types.UID{"host-0": "host-uid", "idle-host": "idle-host-uid"}
	workers := map[string]workerPodSnapshot{"worker-0": {UID: "worker-uid", NodeName: "host-0"}}
	worker := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "soperator", Name: "worker-0", UID: "worker-uid"},
		Spec:       corev1.PodSpec{NodeName: "host-0"},
	}
	daemon := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "daemon", OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Controller: new(true)}}},
		Spec:       corev1.PodSpec{NodeName: "host-0"},
	}
	require.NoError(t, workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{worker, daemon}, 8))
	idleDaemon := daemon.DeepCopy()
	idleDaemon.Spec.NodeName = "idle-host"
	require.NoError(t, workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{worker, daemon, *idleDaemon}, 8))
	idleWorkload := idleDaemon.DeepCopy()
	idleWorkload.OwnerReferences = nil
	err := workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{worker, *idleWorkload}, 8)
	var topology workerMK8sTopologyError
	require.ErrorAs(t, err, &topology, "idle hosts must also be free of unrelated workloads")
	unrelated := worker.DeepCopy()
	unrelated.Name = "other-worker"
	err = workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{worker, daemon, *unrelated}, 8)
	require.ErrorAs(t, err, &topology)
	unrelated.Name = worker.Name
	unrelated.UID = "unexpected-uid"
	require.Error(t, workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{*unrelated}, 8))
	err = workerMK8sDedicatedPods(hosts, workers, []corev1.Pod{worker, daemon}, 1)
	require.ErrorContains(t, err, "exceeding proposed capacity")
	assert.False(t, errors.As(err, &topology), "insufficient configured capacity is a failure, not a topology skip")
}

func readyWorkerMK8sHost(name, group string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: map[string]string{workerMK8sGroupLabel: group}},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

func TestWorkerMK8sHostsIncludeIdleGroupMembers(t *testing.T) {
	workers := map[string]workerPodSnapshot{"worker-1": {NodeName: "host-1"}, "worker-5": {NodeName: "host-5"}}
	nodes := []corev1.Node{
		readyWorkerMK8sHost("host-1", "group-1"), readyWorkerMK8sHost("host-5", "group-1"),
		readyWorkerMK8sHost("idle-host", "group-1"), readyWorkerMK8sHost("unrelated-host", "group-2"),
	}
	group, hosts, err := workerMK8sHosts(workers, nodes)
	require.NoError(t, err)
	assert.Equal(t, "group-1", group)
	assert.Equal(t, map[string]types.UID{"host-1": "host-1-uid", "host-5": "host-5-uid", "idle-host": "idle-host-uid"}, hosts)

	for _, tt := range []struct {
		name   string
		mutate func(*corev1.Node)
	}{
		{"not ready", func(n *corev1.Node) { n.Status.Conditions = nil }},
		{"cordoned", func(n *corev1.Node) { n.Spec.Unschedulable = true }},
		{"terminating", func(n *corev1.Node) { n.DeletionTimestamp = new(metav1.Now()) }},
		{"missing UID", func(n *corev1.Node) { n.UID = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			changed := slices.Clone(nodes)
			changed[2] = *nodes[2].DeepCopy()
			tt.mutate(&changed[2])
			_, _, err := workerMK8sHosts(workers, changed)
			require.ErrorContains(t, err, "idle-host is not ready and schedulable")
			var topology workerMK8sTopologyError
			assert.False(t, errors.As(err, &topology), "an unhealthy idle host must fail rather than skip")
		})
	}
	_, _, err = workerMK8sHosts(map[string]workerPodSnapshot{"worker-1": {NodeName: "absent"}}, nodes)
	require.ErrorContains(t, err, "absent")
	_, _, err = workerMK8sHosts(map[string]workerPodSnapshot{"worker-1": {NodeName: "host-1"}, "worker-5": {NodeName: "host-1"}}, nodes)
	require.ErrorContains(t, err, "multiple workers")
	_, _, err = workerMK8sHosts(map[string]workerPodSnapshot{"worker-1": {NodeName: "host-1"}, "worker-5": {NodeName: "unrelated-host"}}, nodes)
	var topology workerMK8sTopologyError
	require.ErrorAs(t, err, &topology)
}

func TestWorkerMK8sReplacementRequiresNewHostAndPod(t *testing.T) {
	original := map[string]workerPodSnapshot{"worker-0": {UID: "old-pod", NodeName: "old-host"}}
	pod := readyWorkerPod("worker-0", "new-pod", nil)
	pod.Spec.NodeName = "new-host"
	require.NoError(t, workerMK8sReplacedPods(original, []corev1.Pod{pod}))
	pod.Spec.NodeName = "old-host"
	require.Error(t, workerMK8sReplacedPods(original, []corev1.Pod{pod}))
	pod.Spec.NodeName = "new-host"
	pod.UID = "old-pod"
	require.Error(t, workerMK8sReplacedPods(original, []corev1.Pod{pod}))
}

func TestWorkerMK8sStrategyAndConvergence(t *testing.T) {
	surge, err := workerMK8sLimitCount(workerMK8sLimit{Percent: new(mk8sInteger(1))}, 2, true)
	require.NoError(t, err)
	assert.Equal(t, 1, surge)
	unavailable, err := workerMK8sLimitCount(workerMK8sLimit{Percent: new(mk8sInteger(50))}, 3, false)
	require.NoError(t, err)
	assert.Equal(t, 1, unavailable)
	_, err = workerMK8sLimitCount(workerMK8sLimit{}, 2, false)
	require.Error(t, err)

	group, err := decodeWorkerMK8sGroup(`{"metadata":{"id":"group","resource_version":1},"spec":{"fixed_node_count":1,"template":{}},"status":{"state":"RUNNING","target_node_count":1,"node_count":1,"ready_node_count":1}}`)
	require.NoError(t, err)
	require.NoError(t, workerMK8sGroupReady(group, 1))
	group.Status.Reconciling = true
	require.Error(t, workerMK8sGroupReady(group, 1))
	group.Status.Reconciling = false
	group.Status.Outdated = 1
	require.Error(t, workerMK8sGroupReady(group, 1))
	group.Status.Outdated = 0
	group.Status.State = "running"
	require.NoError(t, workerMK8sGroupReady(group, 1))
}

type workerMK8sTestRuntime struct {
	framework.Runtime
	local     func(...string) (string, error)
	kube      func(...string) (string, error)
	polls     int
	kubeReads int
}

func (r *workerMK8sTestRuntime) Local() framework.ArgsScope {
	return framework.NewArgsScope(func(_ context.Context, args ...string) (string, error) { return r.local(args...) })
}

func (r *workerMK8sTestRuntime) Kubectl() framework.ArgsScope {
	return framework.NewArgsScope(func(_ context.Context, args ...string) (string, error) {
		r.kubeReads++
		if r.kube != nil {
			return r.kube(args...)
		}
		pod := readyWorkerPod("worker-0", "replacement-pod", nil)
		pod.Spec.NodeName = "replacement-host"
		data, err := json.Marshal(corev1.PodList{Items: []corev1.Pod{pod}})
		return string(data), err
	})
}

func (r *workerMK8sTestRuntime) Controller() framework.CommandScope {
	return framework.NewCommandScope(func(context.Context, string) (string, error) {
		return "NodeName=worker-0 State=IDLE InstanceId=replacement-instance", nil
	})
}

func (r *workerMK8sTestRuntime) WaitFor(ctx context.Context, _ string, _, _ time.Duration, condition func(context.Context) (bool, error)) error {
	for range 5 {
		r.polls++
		done, err := condition(ctx)
		if done {
			return err
		}
		if err != nil && !strings.Contains(err.Error(), "still in progress") {
			return err
		}
	}
	return fmt.Errorf("test polling budget exhausted")
}

func (r *workerMK8sTestRuntime) Logf(string, ...any) {}

func workerMK8sTestGroup(version int, maxPods *mk8sInteger) string {
	template := map[string]any{"resources": map[string]any{"platform": "gpu"}}
	if maxPods != nil {
		template["max_pods"] = int64(*maxPods)
	}
	data, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"id": "group-1", "resource_version": version},
		"spec":     map[string]any{"fixed_node_count": 1, "template": template},
		"status":   map[string]any{"state": "RUNNING", "target_node_count": 1, "node_count": 1, "ready_node_count": 1},
	})
	return string(data)
}

func workerMK8sTestSteps(t *testing.T, runtime *workerMK8sTestRuntime, original *mk8sInteger) *WorkerRollingUpdate {
	t.Helper()
	group, err := decodeWorkerMK8sGroup(workerMK8sTestGroup(1, original))
	require.NoError(t, err)
	return &WorkerRollingUpdate{
		runtime:     runtime,
		kubectl:     framework.NewKubectlClient(runtime),
		slurm:       framework.NewSlurmClient(runtime),
		workers:     []framework.WorkerInfo{{Name: "worker-0"}},
		initialPods: map[string]workerPodSnapshot{"worker-0": {UID: "original-pod", NodeName: "original-host"}},
		mk8s: workerRolloutMK8s{
			groupID: "group-1", originalMaxPods: original, changedMaxPods: 111,
			specFingerprint: group.fingerprint, originalNodes: map[string]types.UID{"original-host": "original-host-uid"},
		},
	}
}

func TestWorkerMK8sSparseWorkersUseWholeGroupForSelectionAndReplacement(t *testing.T) {
	groupJSON := `{"metadata":{"id":"group-1","resource_version":1},"spec":{"fixed_node_count":2,"template":{"max_pods":110}},"status":{"state":"RUNNING","target_node_count":2,"node_count":2,"ready_node_count":2,"strategy":{"drain_timeout":"0s","max_surge":{"count":0},"max_unavailable":{"percent":50}}}}`
	nodes := []corev1.Node{readyWorkerMK8sHost("host-5", "group-1"), readyWorkerMK8sHost("idle-host", "group-1")}
	pod := readyWorkerPod("worker-5", "original-pod", nil)
	pod.Namespace = "soperator"
	pod.Spec.NodeName = "host-5"
	runtime := &workerMK8sTestRuntime{
		local: func(...string) (string, error) { return groupJSON, nil },
		kube: func(args ...string) (string, error) {
			var object any = corev1.PodList{Items: []corev1.Pod{pod}}
			if slices.Contains(args, "nodes") {
				object = corev1.NodeList{Items: nodes}
			}
			data, err := json.Marshal(object)
			return string(data), err
		},
	}
	steps := &WorkerRollingUpdate{
		runtime: runtime, kubectl: framework.NewKubectlClient(runtime),
		workers:     []framework.WorkerInfo{{Name: "worker-5", SlurmNode: framework.SlurmNodeInfo{InstanceID: "instance-5"}}},
		initialPods: map[string]workerPodSnapshot{"worker-5": {UID: pod.UID, NodeName: pod.Spec.NodeName}},
	}
	require.NoError(t, steps.selectMK8sGroup(t.Context()), "50%% unavailable is one of two hosts, not zero of one active worker")
	assert.Equal(t, map[string]types.UID{"host-5": "host-5-uid", "idle-host": "idle-host-uid"}, steps.mk8s.originalNodes)
	assert.Equal(t, map[string]string{"worker-5": "instance-5"}, steps.mk8s.originalInstances)

	// Isolate Kubernetes replacement proof: even after the active worker moves,
	// the old empty host must also disappear before the rollout can pass.
	steps.mk8s.originalInstances = nil
	groupJSON = strings.Replace(groupJSON, `"max_pods":110`, `"max_pods":111`, 1)
	pod.UID = "replacement-pod"
	pod.Spec.NodeName = "replacement-host"
	nodes[0] = readyWorkerMK8sHost("replacement-host", "group-1")
	require.ErrorContains(t, steps.waitForMK8sReplacement(t.Context()), "original MK8s host idle-host still exists")
	nodes[1] = readyWorkerMK8sHost("replacement-idle-host", "group-1")
	require.NoError(t, steps.waitForMK8sReplacement(t.Context()))
}

func TestWorkerMK8sCleanupRetainsUpdatedSpec(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted request timed out=%t", timedOut), func(t *testing.T) {
			runtime := &workerMK8sTestRuntime{}
			steps := workerMK8sTestSteps(t, runtime, nil)
			var maxPods *mk8sInteger
			updates, calls := 0, 0
			runtime.local = func(args ...string) (string, error) {
				calls++
				if slices.Contains(args, "update") {
					updates++
					require.Equal(t, workerMK8sUpdateArgs("group-1", 1, 111), args)
					maxPods = new(mk8sInteger(111))
					if timedOut {
						return "", fmt.Errorf("request deadline exceeded after acceptance")
					}
					return "operation-1", nil
				}
				return workerMK8sTestGroup(1, maxPods), nil
			}
			err := steps.changeMK8sGroup(t.Context())
			if timedOut {
				require.ErrorContains(t, err, "deadline exceeded")
			} else {
				require.NoError(t, err)
			}
			callsBeforeCleanup := calls
			steps.CleanupAndReset(t.Context())
			steps.CleanupAndReset(t.Context())
			assert.Equal(t, 1, updates, "cleanup must not trigger another node-group rollout")
			assert.Equal(t, callsBeforeCleanup, calls, "cleanup should not call the cloud API")
			require.NotNil(t, maxPods)
			assert.Equal(t, mk8sInteger(111), *maxPods)
			assert.Equal(t, workerRolloutMK8s{}, steps.mk8s)
		})
	}
}

func TestWorkerMK8sRejectsConcurrentConfiguration(t *testing.T) {
	for _, duringRollout := range []bool{false, true} {
		for _, change := range []string{"max_pods", "other spec"} {
			t.Run(fmt.Sprintf("during rollout=%t/%s", duringRollout, change), func(t *testing.T) {
				runtime := &workerMK8sTestRuntime{}
				steps := workerMK8sTestSteps(t, runtime, nil)
				mutations := 0
				runtime.local = func(args ...string) (string, error) {
					if slices.Contains(args, "update") {
						mutations++
						return "", fmt.Errorf("unexpected mutation")
					}
					current := workerMK8sTestGroup(4, new(mk8sInteger(112)))
					if change == "other spec" {
						var maxPods *mk8sInteger
						if duringRollout {
							maxPods = new(mk8sInteger(111))
						}
						current = strings.Replace(workerMK8sTestGroup(4, maxPods), `"gpu"`, `"other-gpu"`, 1)
					}
					return current, nil
				}
				if duringRollout {
					err := steps.waitForMK8sReplacement(t.Context())
					require.ErrorContains(t, err, "configuration changed during rollout")
					var invariant rolloutInvariantError
					assert.ErrorAs(t, err, &invariant)
				} else {
					require.ErrorContains(t, steps.changeMK8sGroup(t.Context()), "configuration changed after selection")
				}
				assert.Zero(t, mutations)
			})
		}
	}
}

func TestWorkerMK8sReplacementWaitsForOperation(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			runtime := &workerMK8sTestRuntime{}
			steps := workerMK8sTestSteps(t, runtime, nil)
			steps.mk8s.operationID = "operation-1"
			pendingReads := 1
			if retry {
				pendingReads = 6
			}
			operationReads := 0
			runtime.local = func(args ...string) (string, error) {
				if slices.Contains(args, "update") {
					t.Fatal("waiting for replacement must not submit another update")
				}
				if slices.Contains(args, "operation") {
					operationReads++
					if operationReads <= pendingReads {
						return `{"id":"operation-1"}`, nil
					}
					return `{"id":"operation-1","status":{}}`, nil
				}
				return workerMK8sTestGroup(2, new(mk8sInteger(111))), nil
			}
			if retry {
				require.ErrorContains(t, steps.waitForMK8sReplacement(t.Context()), "polling budget exhausted")
				assert.Zero(t, runtime.kubeReads, "apparently ready workers cannot bypass a pending cloud operation")
			}
			require.NoError(t, steps.waitForMK8sReplacement(t.Context()))
			assert.Equal(t, pendingReads+1, operationReads)
			assert.Equal(t, 2, runtime.kubeReads, "host and worker checks run only after the operation completes")
			assert.Equal(t, "operation-1", steps.mk8s.operationID)
		})
	}
}
