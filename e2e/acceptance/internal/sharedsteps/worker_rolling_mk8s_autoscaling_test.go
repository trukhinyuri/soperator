package sharedsteps

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nebius/soperator/e2e/acceptance/framework"
)

const workerMK8sAutoscalingLiveShape = `{"metadata":{"id":"group-1","resource_version":"12"},"spec":{"autoscaling":{"max_node_count":"2"},"template":{"max_pods":"32"}},"status":{"state":"RUNNING","target_node_count":"2","node_count":"2","ready_node_count":"2","outdated_node_count":"0","strategy":{"drain_timeout":"0s","max_surge":{"percent":"0"},"max_unavailable":{"percent":"50"}}}}`

func TestWorkerMK8sAutoscalingConvergence(t *testing.T) {
	for _, tt := range []struct {
		name           string
		minimumWorkers int
		mutate         func(*workerMK8sGroup)
		wantErr        bool
	}{
		{name: "omitted minimum defaults to zero", minimumWorkers: 2},
		{name: "idle hosts may scale down", minimumWorkers: 1, mutate: func(g *workerMK8sGroup) {
			g.Status.Target, g.Status.Count, g.Status.Ready = 1, 1, 1
		}},
		{name: "active workers still require capacity", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) {
			g.Status.Target, g.Status.Count, g.Status.Ready = 1, 1, 1
		}},
		{name: "below configured minimum", minimumWorkers: 1, wantErr: true, mutate: func(g *workerMK8sGroup) {
			g.Spec.Autoscaling.MinNodeCount = 2
			g.Status.Target, g.Status.Count, g.Status.Ready = 1, 1, 1
		}},
		{name: "above configured maximum", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) {
			g.Status.Target, g.Status.Count, g.Status.Ready = 3, 3, 3
		}},
		{name: "negative minimum", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.Autoscaling.MinNodeCount = -1 }},
		{name: "inverted bounds", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.Autoscaling.MinNodeCount = 3 }},
		{name: "negative maximum", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.Autoscaling.MaxNodeCount = -1 }},
		{name: "missing maximum cannot accommodate workers", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.Autoscaling.MaxNodeCount = 0 }},
		{name: "mutually exclusive modes", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.FixedNodeCount = new(mk8sInteger(2)) }},
		{name: "missing sizing mode", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Spec.Autoscaling = nil }},
		{name: "node count not converged", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Status.Count = 1 }},
		{name: "readiness not converged", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Status.Ready = 1 }},
		{name: "reconciling", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Status.Reconciling = true }},
		{name: "old configuration remains", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Status.Outdated = 1 }},
		{name: "not running", minimumWorkers: 2, wantErr: true, mutate: func(g *workerMK8sGroup) { g.Status.State = "PROVISIONING" }},
		{name: "fixed group allows idle hosts", minimumWorkers: 1, mutate: func(g *workerMK8sGroup) {
			g.Spec.Autoscaling = nil
			g.Spec.FixedNodeCount = new(mk8sInteger(2))
		}},
		{name: "fixed group must retain configured count", minimumWorkers: 1, wantErr: true, mutate: func(g *workerMK8sGroup) {
			g.Spec.Autoscaling = nil
			g.Spec.FixedNodeCount = new(mk8sInteger(2))
			g.Status.Target, g.Status.Count, g.Status.Ready = 1, 1, 1
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			group, err := decodeWorkerMK8sGroup(workerMK8sAutoscalingLiveShape)
			require.NoError(t, err)
			require.NotNil(t, group.Spec.Autoscaling)
			assert.Zero(t, group.Spec.Autoscaling.MinNodeCount)
			assert.Equal(t, mk8sInteger(2), group.Spec.Autoscaling.MaxNodeCount)
			if tt.mutate != nil {
				tt.mutate(&group)
			}
			err = workerMK8sGroupReady(group, tt.minimumWorkers)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWorkerMK8sAutoscalingGroupSelection(t *testing.T) {
	pods := []corev1.Pod{readyWorkerPod("worker-0", "pod-0", nil), readyWorkerPod("worker-1", "pod-1", nil)}
	nodes := []corev1.Node{readyWorkerMK8sHost("host-0", "group-1"), readyWorkerMK8sHost("host-1", "group-1")}
	workers := make([]framework.WorkerInfo, len(pods))
	snapshots := make(map[string]workerPodSnapshot, len(pods))
	for i := range pods {
		pods[i].Namespace = framework.SoperatorNamespace
		pods[i].Spec.NodeName = nodes[i].Name
		snapshots[pods[i].Name] = workerPodSnapshot{UID: pods[i].UID, NodeName: nodes[i].Name}
		workers[i] = framework.WorkerInfo{Name: pods[i].Name, SlurmNode: framework.SlurmNodeInfo{InstanceID: fmt.Sprintf("instance-%d", i)}}
	}
	runtime := &workerMK8sTestRuntime{
		local: func(...string) (string, error) { return workerMK8sAutoscalingLiveShape, nil },
		kube: func(args ...string) (string, error) {
			var object any = corev1.PodList{Items: pods}
			if slices.Contains(args, "nodes") {
				object = corev1.NodeList{Items: nodes}
			}
			data, err := json.Marshal(object)
			return string(data), err
		},
	}
	steps := &WorkerRollingUpdate{
		runtime: runtime, kubectl: framework.NewKubectlClient(runtime), workers: workers, initialPods: snapshots,
	}
	require.NoError(t, steps.selectMK8sGroup(t.Context()), "an autoscaling group with two healthy active workers must run, not skip")
	assert.Equal(t, mk8sInteger(33), steps.mk8s.changedMaxPods)
	assert.Equal(t, map[string]types.UID{"host-0": "host-0-uid", "host-1": "host-1-uid"}, steps.mk8s.originalNodes)

	// A status reporting two nodes cannot certify a Kubernetes snapshot containing
	// an additional group member, even though all three are individually ready.
	nodes = append(nodes, readyWorkerMK8sHost("unexpected-host", "group-1"))
	require.Error(t, steps.selectMK8sGroup(t.Context()))
}

func workerMK8sAutoscalingTestGroup(t *testing.T, version, target int, maxPods mk8sInteger) string {
	t.Helper()
	var group map[string]any
	require.NoError(t, json.Unmarshal([]byte(workerMK8sAutoscalingLiveShape), &group))
	group["metadata"].(map[string]any)["resource_version"] = version
	spec := group["spec"].(map[string]any)
	spec["autoscaling"].(map[string]any)["max_node_count"] = "3"
	spec["template"].(map[string]any)["max_pods"] = maxPods
	status := group["status"].(map[string]any)
	status["target_node_count"], status["node_count"], status["ready_node_count"] = target, target, target
	status["strategy"].(map[string]any)["max_unavailable"] = map[string]any{"count": 1}
	data, err := json.Marshal(group)
	require.NoError(t, err)
	return string(data)
}

func TestWorkerMK8sRepeatedRunsToggleRetainedSpec(t *testing.T) {
	pod := readyWorkerPod("worker-0", "pod-0", nil)
	pod.Namespace, pod.Spec.NodeName = framework.SoperatorNamespace, "host-0"
	node := readyWorkerMK8sHost(pod.Spec.NodeName, "group-1")
	maxPods := mk8sInteger(32)
	version, updates, calls := 1, 0, 0
	targets := []mk8sInteger{33, 32, 33}
	runtime := &workerMK8sTestRuntime{
		local: func(args ...string) (string, error) {
			calls++
			if slices.Contains(args, "update") {
				require.Less(t, updates, len(targets), "cleanup must not trigger another rollout")
				wanted := targets[updates]
				require.Equal(t, workerMK8sUpdateArgs("group-1", mk8sInteger(version), wanted), args,
					"each run must use the latest resource version and change only max_pods")
				require.NotEqual(t, maxPods, wanted, "each run must change the retained specification")
				maxPods, version, updates = wanted, version+1, updates+1
				return fmt.Sprintf("operation-%d", updates), nil
			}
			return workerMK8sAutoscalingTestGroup(t, version, 1, maxPods), nil
		},
		kube: func(args ...string) (string, error) {
			var object any = corev1.PodList{Items: []corev1.Pod{pod}}
			if slices.Contains(args, "nodes") {
				object = corev1.NodeList{Items: []corev1.Node{node}}
			}
			data, err := json.Marshal(object)
			return string(data), err
		},
	}
	for run, wanted := range targets {
		steps := &WorkerRollingUpdate{
			runtime: runtime, kubectl: framework.NewKubectlClient(runtime),
			workers:     []framework.WorkerInfo{{Name: pod.Name, SlurmNode: framework.SlurmNodeInfo{InstanceID: "instance-0"}}},
			initialPods: map[string]workerPodSnapshot{pod.Name: {UID: pod.UID, NodeName: pod.Spec.NodeName}},
		}
		require.NoError(t, steps.selectMK8sGroup(t.Context()))
		require.NotNil(t, steps.mk8s.originalMaxPods)
		assert.Equal(t, maxPods, *steps.mk8s.originalMaxPods)
		assert.Equal(t, wanted, steps.mk8s.changedMaxPods)
		require.NoError(t, steps.changeMK8sGroup(t.Context()))
		assert.Equal(t, run+1, updates, "each run must issue exactly one update")
		callsBeforeCleanup := calls
		steps.CleanupAndReset(t.Context())
		assert.Equal(t, callsBeforeCleanup, calls, "cleanup must leave the cloud specification untouched")
		assert.Equal(t, wanted, maxPods, "the next run must see the retained value")
		assert.Equal(t, workerRolloutMK8s{}, steps.mk8s)
	}
}

func TestWorkerMK8sAutoscalingUpdatePreservesScaling(t *testing.T) {
	pod := readyWorkerPod("worker-0", "original-pod", nil)
	pod.Namespace, pod.Spec.NodeName = framework.SoperatorNamespace, "original-host"
	nodes := []corev1.Node{readyWorkerMK8sHost("original-host", "group-1"), readyWorkerMK8sHost("idle-host", "group-1")}
	target, version, updates := 2, 1, 0
	maxPods := mk8sInteger(32)
	workerPresent := true
	runtime := &workerMK8sTestRuntime{}
	runtime.kube = func(args ...string) (string, error) {
		pods := []corev1.Pod{pod}
		if !workerPresent {
			pods = nil
		}
		var object any = corev1.PodList{Items: pods}
		if slices.Contains(args, "nodes") {
			object = corev1.NodeList{Items: nodes}
		}
		data, err := json.Marshal(object)
		return string(data), err
	}
	runtime.local = func(args ...string) (string, error) {
		switch {
		case slices.Contains(args, "update"):
			updates++
			wanted := mk8sInteger(33)
			require.Equal(t, workerMK8sUpdateArgs("group-1", mk8sInteger(version), wanted), args,
				"the only mutated spec field is max_pods; autoscaling bounds and mode must remain intact")
			maxPods, version = wanted, version+1
			return fmt.Sprintf("operation-%d", updates), nil
		case slices.Contains(args, "operation"):
			return fmt.Sprintf(`{"id":"operation-%d","status":{}}`, updates), nil
		case slices.Contains(args, "compute"):
			return "", fmt.Errorf("instance not found")
		default:
			return workerMK8sAutoscalingTestGroup(t, version, target, maxPods), nil
		}
	}
	steps := &WorkerRollingUpdate{
		runtime: runtime, kubectl: framework.NewKubectlClient(runtime), slurm: framework.NewSlurmClient(runtime),
		workers:     []framework.WorkerInfo{{Name: "worker-0", SlurmNode: framework.SlurmNodeInfo{InstanceID: "original-instance"}}},
		initialPods: map[string]workerPodSnapshot{"worker-0": {UID: pod.UID, NodeName: pod.Spec.NodeName}},
	}
	require.NoError(t, steps.selectMK8sGroup(t.Context()))
	originalFingerprint := steps.mk8s.specFingerprint
	require.NoError(t, steps.changeMK8sGroup(t.Context()))

	// Autoscaling may remove idle capacity while MK8s replaces the active host.
	// Every originally captured host still has to disappear; the worker must
	// retain its ordinal and return on a new pod, host, and Compute instance.
	target = 1
	pod.UID, pod.Spec.NodeName = "replacement-pod", "replacement-host"
	nodes = []corev1.Node{readyWorkerMK8sHost("replacement-host", "group-1")}
	workerPresent = false
	require.Error(t, steps.waitForMK8sReplacement(t.Context()), "autoscaling must not let a missing active worker pass")
	workerPresent = true
	target = 2
	nodes = append(nodes, readyWorkerMK8sHost("idle-host", "group-1"))
	require.ErrorContains(t, steps.waitForMK8sReplacement(t.Context()), "original MK8s host idle-host still exists")
	target, nodes = 1, nodes[:1]
	require.NoError(t, steps.waitForMK8sReplacement(t.Context()))
	steps.CleanupAndReset(t.Context())
	assert.Equal(t, 1, updates)
	assert.Equal(t, mk8sInteger(33), maxPods)
	group, err := decodeWorkerMK8sGroup(workerMK8sAutoscalingTestGroup(t, version, target, maxPods))
	require.NoError(t, err)
	assert.Equal(t, originalFingerprint, group.fingerprint)
	assert.Nil(t, group.Spec.FixedNodeCount)
	assert.Equal(t, mk8sInteger(3), group.Spec.Autoscaling.MaxNodeCount)
	assert.Zero(t, group.Spec.Autoscaling.MinNodeCount)
}

func TestWorkerMK8sAutoscalingBoundsRemainProtectedByFingerprint(t *testing.T) {
	group, err := decodeWorkerMK8sGroup(workerMK8sAutoscalingLiveShape)
	require.NoError(t, err)
	changed, err := decodeWorkerMK8sGroup(strings.Replace(workerMK8sAutoscalingLiveShape, `"max_node_count":"2"`, `"max_node_count":"3"`, 1))
	require.NoError(t, err)
	assert.NotEqual(t, group.fingerprint, changed.fingerprint)

	runtime := &workerMK8sTestRuntime{}
	steps := workerMK8sTestSteps(t, runtime, new(mk8sInteger(32)))
	steps.mk8s.specFingerprint, steps.mk8s.changedMaxPods = group.fingerprint, 33
	runtime.local = func(args ...string) (string, error) {
		if slices.Contains(args, "update") {
			t.Fatal("rollout must not overwrite a concurrent autoscaling change")
		}
		return strings.Replace(workerMK8sAutoscalingLiveShape, `"max_node_count":"2"`, `"max_node_count":"3"`, 1), nil
	}
	require.ErrorContains(t, steps.changeMK8sGroup(t.Context()), "configuration changed after selection")
}
