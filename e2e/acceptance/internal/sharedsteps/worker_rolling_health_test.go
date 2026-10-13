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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

func TestHealthSetupUsesActiveWorkerCount(t *testing.T) {
	for _, tt := range []struct {
		name        string
		workerNames []string
		workers     []framework.WorkerInfo
		wantSkip    bool
	}{
		{"one active out of many configured", []string{"worker-5"}, []framework.WorkerInfo{{Name: "worker-5"}}, true},
		{"active peer missing", []string{"worker-1", "worker-5"}, []framework.WorkerInfo{{Name: "worker-5"}}, false},
		{"only expected active worker missing", []string{"worker-5"}, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &healthCleanupRuntime{}
			steps := &WorkerRollingUpdate{
				runtime: runtime,
				nodeSet: kubeobjects.NodeSet{
					Metadata: kubeobjects.ObjectMeta{Name: "worker"},
					Spec:     kubeobjects.NodeSetSpec{Replicas: 8, EphemeralNodes: new(true)},
				},
				workerNames: tt.workerNames, workers: tt.workers,
			}
			err := steps.requireHealthTestSetup(t.Context())
			if tt.wantSkip {
				require.ErrorIs(t, err, godog.ErrSkip)
				assert.Contains(t, strings.Join(runtime.logs, "\n"), "fewer than two active workers")
			} else {
				require.ErrorContains(t, err, "active workers but only")
				assert.NotErrorIs(t, err, godog.ErrSkip)
			}
		})
	}
}

func TestHealthNodeReplacementDisabled(t *testing.T) {
	for _, tt := range []struct {
		name       string
		containers []corev1.Container
		disabled   bool
		errorPart  string
	}{
		{"disabled", []corev1.Container{{Name: "manager", Args: []string{"--enable-node-replacement=false"}}}, true, ""},
		{"enabled", []corev1.Container{{Name: "manager", Args: []string{"--enable-node-replacement=true"}}}, false, ""},
		{"bare bool enables", []corev1.Container{{Name: "manager", Args: []string{"--enable-node-replacement"}}}, false, ""},
		{"single dash", []corev1.Container{{Name: "manager", Args: []string{"-enable-node-replacement=false"}}}, true, ""},
		{"absent flag is unknown", []corev1.Container{{Name: "manager", Args: []string{"--other=false"}}}, false, "explicitly configure"},
		{"invalid bool", []corev1.Container{{Name: "manager", Args: []string{"--enable-node-replacement=maybe"}}}, false, "invalid"},
		{"conflicting flags", []corev1.Container{{Name: "manager", Args: []string{"--enable-node-replacement=true", "--enable-node-replacement=false"}}}, false, "multiple"},
		{"wrong container", []corev1.Container{{Name: "sidecar", Args: []string{"--enable-node-replacement=false"}}}, false, "manager container not found"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			disabled, err := healthNodeReplacementDisabled(tt.containers)
			if tt.errorPart != "" {
				require.ErrorContains(t, err, tt.errorPart)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.disabled, disabled)
		})
	}
}

func healthSourceCheck(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	var source unstructured.Unstructured
	require.NoError(t, json.Unmarshal([]byte(`{
 "apiVersion":"slurm.nebius.ai/v1alpha1", "kind":"ActiveCheck",
 "metadata":{"name":"soperator-gpu-checks","uid":"not-owned","resourceVersion":"42","ownerReferences":[{"name":"deployed"}]},
 "spec":{
  "name":"soperator-gpu-checks","checkType":"slurmJob","slurmClusterRefName":"soperator",
  "runAfterCreation":true,"suspend":false,"dependsOn":["external-check"],"podTemplateNameRef":"gpu-template",
  "hostUsers":false,"tolerations":[{"key":"test","operator":"Exists"}],
  "successReactions":{"commentSlurmNode":{"commentPrefix":"success"}},
  "slurmJobSpec":{
   "eachWorkerJobs":true,"maxNumberOfJobs":8,"sbatchScriptRefName":"gpu-script",
   "jobContainer":{"image":"local/slurm-check:5.0.0","command":["custom-gpu"],"args":["--exclusive"],
    "env":[{"name":"SBATCH_GPUS_PER_NODE","value":"8"},{"name":"SBATCH_EXCLUSIVE","value":"true"},{"name":"ACTIVE_CHECK_REQUIRES_GPU","value":"true"},{"name":"EACH_WORKER_JOBS","value":"true"},{"name":"K8S_POD_NAME","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}},{"name":"SNCCLD_ENABLED","value":"false"}],
    "volumeMounts":[{"name":"jail","mountPath":"/mnt/jail"}],"volumes":[{"name":"jail","persistentVolumeClaim":{"claimName":"jail"}}]},
   "mungeContainer":{"image":"local/munge:5.0.0"}
  }
 }
}`), &source))
	return &source
}

func TestHealthFailureManifestIsIsolatedAndPinsOneCPUJob(t *testing.T) {
	source := healthSourceCheck(t)
	before, err := json.Marshal(source)
	require.NoError(t, err)
	manifest, err := healthFailureManifest(source, "acceptance-health-owned", "soperator", "worker-0", "/scenario/gate with 'quotes'")
	require.NoError(t, err)
	after, err := json.Marshal(source)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after), "the deployed check must not be mutated")
	assert.Equal(t, "acceptance-health-owned", manifest.GetName())
	assert.Empty(t, manifest.GetUID())
	assert.Empty(t, manifest.GetResourceVersion())
	assert.Empty(t, manifest.GetOwnerReferences())
	for _, removed := range []string{"dependsOn", "successReactions", "podTemplateNameRef"} {
		_, found, err := unstructured.NestedFieldNoCopy(manifest.Object, "spec", removed)
		require.NoError(t, err)
		assert.False(t, found, removed)
	}
	for key, expected := range map[string]bool{"suspend": true, "runAfterCreation": false, "hostUsers": false} {
		value, found, err := unstructured.NestedBool(manifest.Object, "spec", key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, expected, value)
	}
	everyWorker, found, err := unstructured.NestedBool(manifest.Object, "spec", "slurmJobSpec", "eachWorkerJobs")
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, everyWorker)
	image, _, err := unstructured.NestedString(manifest.Object, "spec", "slurmJobSpec", "jobContainer", "image")
	require.NoError(t, err)
	assert.Equal(t, "local/slurm-check:5.0.0", image)
	env, _, err := unstructured.NestedSlice(manifest.Object, "spec", "slurmJobSpec", "jobContainer", "env")
	require.NoError(t, err)
	require.Len(t, env, 2)
	assert.Equal(t, "K8S_POD_NAME", env[0].(map[string]any)["name"])
	assert.Equal(t, "SNCCLD_ENABLED", env[1].(map[string]any)["name"])
	prefix, _, err := unstructured.NestedString(manifest.Object, "spec", "failureReactions", "drainSlurmNode", "drainReasonPrefix")
	require.NoError(t, err)
	assert.Equal(t, "[node_problem]", prefix)
	script, _, err := unstructured.NestedString(manifest.Object, "spec", "slurmJobSpec", "sbatchScript")
	require.NoError(t, err)
	assert.Contains(t, script, "#SBATCH --nodelist=worker-0\n")
	assert.Contains(t, script, "#SBATCH --cpus-per-task=1\n")
	assert.Contains(t, script, "#SBATCH --mem=64M\n")
	assert.Contains(t, script, "#SBATCH --no-requeue\n")
	assert.Contains(t, script, framework.ShellQuote("/scenario/gate with 'quotes'"))
	assert.NotContains(t, script, "--gpus")
	assert.NotContains(t, script, "--exclusive")
	assert.True(t, strings.HasSuffix(script, "exit 1\n"))
}

func TestHealthFailureManifestRejectsWrongClusterOrMissingImages(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*unstructured.Unstructured)
		want   string
	}{
		{"cluster", func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(c.Object, "another", "spec", "slurmClusterRefName")
		}, "cluster=\"another\""},
		{"job type", func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(c.Object, "k8sJob", "spec", "checkType")
		}, "checkType=\"k8sJob\""},
		{"submitter image", func(c *unstructured.Unstructured) {
			unstructured.RemoveNestedField(c.Object, "spec", "slurmJobSpec", "jobContainer", "image")
		}, "jobContainer image is missing"},
		{"munge image", func(c *unstructured.Unstructured) {
			unstructured.RemoveNestedField(c.Object, "spec", "slurmJobSpec", "mungeContainer", "image")
		}, "mungeContainer image is missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			check := healthSourceCheck(t)
			tt.mutate(check)
			_, err := healthFailureManifest(check, "test", "soperator", "worker-0", "/gate")
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestHealthReasonMatchesOnlyOwnedFailure(t *testing.T) {
	const reason = "[node_problem] acceptance-check: job 123 [slurm_job]"
	for _, tt := range []struct {
		reason string
		want   bool
	}{
		{reason, true},
		{reason + " [soperatorchecks@2026-10-12T01:02:03]", true},
		{reason + " : reboot issued [root@timestamp]", true},
		{reason + " : [hardware_problem] unrelated", false},
		{"soperator rolling update : " + reason, false},
		{"[node_problem] acceptance-check-other: job 123 [slurm_job]", false},
		{"[node_problem] acceptance-check: job 1234 [slurm_job]", false},
		{"[node_problem] another-check: job 123 [slurm_job]", false},
		{"[user_problem] acceptance-check: job 123 [slurm_job]", false},
	} {
		assert.Equal(t, tt.want, healthReasonMatches(tt.reason, "acceptance-check", "123"), tt.reason)
	}
	assert.False(t, healthReasonMatches(reason, "", "123"))
	assert.False(t, healthReasonMatches(reason, "acceptance-check", ""))
}

func healthRecoveredFixture(t *testing.T) (corev1.Pod, framework.SlurmNodeInfo, workerHealthCRR, map[string]string) {
	t.Helper()
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "soperator", UID: "original", Annotations: map[string]string{
			workerHealthRecoveryRequest: "recovery-request", workerHealthRecoveryContainer: "containerd://old-slurmd",
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{
			{Name: "slurmd", ContainerID: "containerd://new-slurmd"}, {Name: "sidecar", ContainerID: "containerd://unchanged"},
		}},
	}
	var request workerHealthCRR
	require.NoError(t, json.Unmarshal([]byte(`{"metadata":{"name":"recovery-request","labels":{"crr.apps.kruise.io/pod-uid":"original"},"ownerReferences":[{"kind":"Pod","name":"worker-0","uid":"original","controller":true}]},"spec":{"podName":"worker-0","containers":[{"name":"slurmd","statusContext":{"containerID":"containerd://old-slurmd"}}]},"status":{"phase":"Completed","containerRecreateStates":[{"name":"slurmd","phase":"Succeeded"}]}}`), &request))
	node := framework.SlurmNodeInfo{Name: "worker-0", State: "IDLE+DRAIN", CPUAlloc: new(uint64(0)), AllocMemoryMiB: new(uint64(0))}
	return pod, node, request, map[string]string{"slurmd": "containerd://old-slurmd", "sidecar": "containerd://unchanged"}
}

func TestHealthRecoveryRequiresSamePodOwnedCRRAndOnlySlurmdRestart(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*corev1.Pod, *framework.SlurmNodeInfo, *workerHealthCRR)
		wantDone  bool
		wantError string
	}{
		{"complete", func(*corev1.Pod, *framework.SlurmNodeInfo, *workerHealthCRR) {}, true, ""},
		{"request running", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) { r.Status.Phase = "Recreating" }, false, "phase=Recreating"},
		{"pending context", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Status.Phase = "Pending"
			r.Spec.Containers[0].StatusContext = nil
		}, false, "identity is not populated yet"},
		{"completed without context", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Spec.Containers[0].StatusContext = nil
		}, false, "target only"},
		{"wrong pod uid", func(p *corev1.Pod, _ *framework.SlurmNodeInfo, _ *workerHealthCRR) { p.UID = "replacement" }, false, "target only"},
		{"wrong owner", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Metadata.OwnerReferences[0].UID = "other"
		}, false, "not controlled"},
		{"wrong target", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Spec.Containers[0].Name = "sidecar"
		}, false, "target only"},
		{"wrong old container", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Spec.Containers[0].StatusContext.ContainerID = "other"
		}, false, "target only"},
		{"recreation failed", func(_ *corev1.Pod, _ *framework.SlurmNodeInfo, r *workerHealthCRR) {
			r.Status.ContainerRecreateStates[0].Phase = "Failed"
		}, false, "without successful"},
		{"identity unchanged", func(p *corev1.Pod, _ *framework.SlurmNodeInfo, _ *workerHealthCRR) {
			p.Status.ContainerStatuses[0].ContainerID = "containerd://old-slurmd"
		}, false, "registration pending"},
		{"sidecar restarted", func(p *corev1.Pod, _ *framework.SlurmNodeInfo, _ *workerHealthCRR) {
			p.Status.ContainerStatuses[1].ContainerID = "new"
		}, false, "unexpectedly restarted"},
		{"reboot uncleared", func(_ *corev1.Pod, n *framework.SlurmNodeInfo, _ *workerHealthCRR) {
			n.State = "IDLE+DRAIN+REBOOT_ISSUED"
		}, false, "registration pending"},
		{"unknown allocations", func(_ *corev1.Pod, n *framework.SlurmNodeInfo, _ *workerHealthCRR) { n.CPUAlloc = nil }, false, "registration pending"},
		{"memory allocated", func(_ *corev1.Pod, n *framework.SlurmNodeInfo, _ *workerHealthCRR) {
			n.AllocMemoryMiB = new(uint64(64))
		}, false, "registration pending"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod, node, request, containers := healthRecoveredFixture(t)
			tt.mutate(&pod, &node, &request)
			done, err := healthRecoveryObserved(pod, node, request, containers)
			assert.Equal(t, tt.wantDone, done)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHealthWorkerSettledRejectsUnknownOrUnsafeSlurmState(t *testing.T) {
	base := framework.SlurmNodeInfo{
		State:          "IDLE+DRAIN",
		CPUAlloc:       new(uint64(0)),
		AllocMemoryMiB: new(uint64(0)),
	}
	assert.True(t, healthWorkerSettled(base))
	for _, flag := range []string{"COMPLETING", "REBOOT_REQUESTED", "REBOOT_ISSUED", "DOWN", "NOT_RESPONDING", "INVALID_REG", "FAIL"} {
		node := base
		node.State += "+" + flag
		assert.False(t, healthWorkerSettled(node), flag)
	}
	for _, tt := range []struct {
		name   string
		modify func(*framework.SlurmNodeInfo)
	}{
		{"unknown cpu", func(n *framework.SlurmNodeInfo) { n.CPUAlloc = nil }},
		{"unknown memory", func(n *framework.SlurmNodeInfo) { n.AllocMemoryMiB = nil }},
		{"allocated cpu", func(n *framework.SlurmNodeInfo) { n.CPUAlloc = new(uint64(1)) }},
		{"allocated memory", func(n *framework.SlurmNodeInfo) { n.AllocMemoryMiB = new(uint64(1)) }},
		{"not idle", func(n *framework.SlurmNodeInfo) { n.State = "ALLOCATED+DRAIN" }},
	} {
		node := base
		tt.modify(&node)
		assert.False(t, healthWorkerSettled(node), tt.name)
	}
}

func TestHealthPendingPeersExcludesOnlyFaultWorker(t *testing.T) {
	initial := map[string]workerPodSnapshot{"worker-0": {UID: "old-0"}, "worker-1": {UID: "old-1"}}
	pods := []corev1.Pod{readyWorkerPod("worker-0", "old-0", nil), readyWorkerPod("worker-1", "new-1", map[string]string{workerRolloutAnnotationKey: "new"})}
	assert.Empty(t, healthPendingPeers("worker-0", initial, pods, "new"))
	pods[1].UID = "old-1"
	assert.Equal(t, []string{"worker-1"}, healthPendingPeers("worker-0", initial, pods, "new"))
	assert.Equal(t, []string{"worker-1"}, healthPendingPeers("worker-0", initial, pods[:1], "new"))
}

func TestHealthCancelCommandTargetsOnlyUniqueOwnedName(t *testing.T) {
	assert.Equal(t, "scancel --name='acceptance-health-123'", healthCancelCommand("acceptance-health-123"))
	assert.Equal(t, "scancel --name='name'\"'\"'; touch unexpected'", healthCancelCommand("name'; touch unexpected"))
}

type healthCleanupRuntime struct {
	framework.Runtime
	calls          []string
	logs           []string
	nodeOutput     string
	podOutput      string
	deleteJobError error
	jobsRemaining  string
}

func (r *healthCleanupRuntime) Kubectl() framework.ArgsScope { return healthCleanupArgs{r: r} }
func (r *healthCleanupRuntime) Controller() framework.CommandScope {
	return healthCleanupCommand{r: r, scope: "controller"}
}
func (r *healthCleanupRuntime) Jail() framework.CommandScope {
	return healthCleanupCommand{r: r, scope: "jail"}
}
func (r *healthCleanupRuntime) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}
func (r *healthCleanupRuntime) WaitFor(ctx context.Context, _ string, _, _ time.Duration, condition func(context.Context) (bool, error)) error {
	done, err := condition(ctx)
	if err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("condition remains pending")
	}
	return nil
}

type healthCleanupArgs struct{ r *healthCleanupRuntime }

func (s healthCleanupArgs) Run(_ context.Context, args ...string) (string, error) {
	command := strings.Join(args, " ")
	s.r.calls = append(s.r.calls, "kubectl "+command)
	if strings.HasPrefix(command, "delete job ") && s.r.deleteJobError != nil {
		return "", s.r.deleteJobError
	}
	if strings.HasPrefix(command, "get pod ") {
		return s.r.podOutput, nil
	}
	return "", nil
}
func (s healthCleanupArgs) RunWithDefaultRetry(ctx context.Context, args ...string) (string, error) {
	return s.Run(ctx, args...)
}
func (s healthCleanupArgs) RunWithRetry(ctx context.Context, _ int, _ time.Duration, args ...string) (string, error) {
	return s.Run(ctx, args...)
}

type healthCleanupCommand struct {
	r     *healthCleanupRuntime
	scope string
}

func (s healthCleanupCommand) Run(_ context.Context, command string) (string, error) {
	s.r.calls = append(s.r.calls, s.scope+" "+command)
	if strings.HasPrefix(command, "scontrol show node ") {
		return s.r.nodeOutput, nil
	}
	if strings.HasPrefix(command, "squeue ") {
		return s.r.jobsRemaining, nil
	}
	return "", nil
}
func (s healthCleanupCommand) RunWithDefaultRetry(ctx context.Context, command string) (string, error) {
	return s.Run(ctx, command)
}
func (s healthCleanupCommand) RunWithRetry(ctx context.Context, command string, _ int, _ time.Duration) (string, error) {
	return s.Run(ctx, command)
}

func TestHealthCleanupStopsPartialSubmissionAndOnlyResumesSettledOwnedFault(t *testing.T) {
	for _, tt := range []struct {
		name           string
		state          string
		reason         string
		missingJobID   bool
		deleteJobError error
		jobsRemaining  string
		wantResume     bool
	}{
		{name: "owned fault", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned: job 123 [slurm_job]", wantResume: true},
		{name: "lost submission annotation", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned: job 123 [slurm_job]", missingJobID: true, wantResume: true},
		{name: "unrelated drain", state: "IDLE+DRAIN", reason: "[node_problem] another-check: job 123 [slurm_job]"},
		{name: "later fault with own name prefix", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned-extra: job 123 [slurm_job]"},
		{name: "wrong owned job", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned: job 456 [slurm_job]"},
		{name: "reboot still issued", state: "IDLE+DRAIN+REBOOT_ISSUED", reason: "[node_problem] acceptance-health-owned: job 123 [slurm_job]"},
		{name: "submission could still be running", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned: job 123 [slurm_job]", deleteJobError: fmt.Errorf("temporary API failure")},
		{name: "Slurm cancellation not settled", state: "IDLE+DRAIN", reason: "[node_problem] acceptance-health-owned: job 123 [slurm_job]", jobsRemaining: "123\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &healthCleanupRuntime{
				nodeOutput:     fmt.Sprintf("NodeName=worker-0 State=%s CPUAlloc=0 AllocMem=0 Reason=%s [root@timestamp]", tt.state, tt.reason),
				podOutput:      `{"metadata":{"name":"worker-0","namespace":"soperator","uid":"original"},"spec":{"nodeName":"host-original"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}`,
				deleteJobError: tt.deleteJobError, jobsRemaining: tt.jobsRemaining,
			}
			steps := &WorkerRollingUpdate{
				runtime: runtime, kubectl: framework.NewKubectlClient(runtime), slurm: framework.NewSlurmClient(runtime),
				workers:     []framework.WorkerInfo{{Name: "worker-0"}},
				initialPods: map[string]workerPodSnapshot{"worker-0": {UID: "original", NodeName: "host-original"}},
				health:      workerRolloutHealth{checkName: "acceptance-health-owned", k8sJobName: "acceptance-health-owned-run", jobID: "123", gatePath: "/scenario/release"},
			}
			steps.nodeSet.Metadata.Namespace = "soperator"
			if tt.missingJobID {
				steps.health.jobID = ""
			}
			steps.cleanupHealth(context.Background())
			require.GreaterOrEqual(t, len(runtime.calls), 5)
			assert.Contains(t, runtime.calls[0], "kubectl delete job acceptance-health-owned-run")
			assert.Contains(t, runtime.calls[0], "--cascade=foreground --wait=true")
			assert.Equal(t, "controller scancel --name='acceptance-health-owned'", runtime.calls[1])
			assert.Contains(t, runtime.calls[2], "controller squeue --noheader --name='acceptance-health-owned'")
			assert.Contains(t, runtime.calls[3], "kubectl delete activecheck acceptance-health-owned")
			assert.Equal(t, "jail rm -f -- '/scenario/release'", runtime.calls[len(runtime.calls)-1])
			resumed := false
			for _, call := range runtime.calls {
				if strings.Contains(call, "scontrol update ") {
					resumed = true
				}
			}
			assert.Equal(t, tt.wantResume, resumed, strings.Join(runtime.calls, "\n"))
			assert.Equal(t, workerRolloutHealth{}, steps.health, "scenario state resets even when cleanup is incomplete")
			before := len(runtime.calls)
			steps.cleanupHealth(context.Background())
			assert.Len(t, runtime.calls, before, "cleanup is idempotent")
		})
	}
}
