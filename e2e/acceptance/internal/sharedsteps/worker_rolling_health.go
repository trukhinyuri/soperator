package sharedsteps

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

const (
	workerHealthFailureTimeout    = 5 * time.Minute
	workerHealthRecoveryTimeout   = 12 * time.Minute // Includes the CRR's five-minute deadline and controller polling.
	workerHealthOperationPhase    = "slurm.nebius.ai/worker-operation-phase"
	workerHealthOperationID       = "slurm.nebius.ai/worker-operation-id"
	workerHealthRecoveryRequest   = "slurm.nebius.ai/worker-recovery-request"
	workerHealthRecoveryContainer = "slurm.nebius.ai/worker-recovery-container-id"
)

var slurmReasonAttribution = regexp.MustCompile(`\s+\[[^\[\]]+@[^\[\]]+\]$`)

type workerRolloutHealth struct {
	source       *unstructured.Unstructured
	checkName    string
	k8sJobName   string
	jobID        string
	gatePath     string
	artifacts    framework.ScenarioArtifactPaths
	containerIDs map[string]string
}

// This local shape keeps OpenKruise-specific observations in the rollout steps.
type workerHealthCRR struct {
	Metadata metav1.ObjectMeta `json:"metadata"`
	Spec     struct {
		PodName    string `json:"podName"`
		Containers []struct {
			Name          string `json:"name"`
			StatusContext *struct {
				ContainerID string `json:"containerID"`
			} `json:"statusContext"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase                   string `json:"phase"`
		Message                 string `json:"message"`
		ContainerRecreateStates []struct {
			Name  string `json:"name"`
			Phase string `json:"phase"`
		} `json:"containerRecreateStates"`
	} `json:"status"`
}

func (s *WorkerRollingUpdate) registerHealthSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the NodeSet supports controlled health failure testing$`, s.requireHealthTestSetup)
	sc.Step(`^a controllable health check is running on the test worker$`, s.prepareHealthFailure)
	sc.Step(`^the health check fails before the worker update$`, s.failHealthCheck)
	sc.Step(`^the health check fails while the worker handoff is waiting for jobs$`, s.failHealthCheckDuringHandoff)
	sc.Step(`^healthy peers update while the unhealthy worker stays on its original host$`, s.waitForHealthyPeers)
	sc.Step(`^the unhealthy worker recovers slurmd in its original pod without losing its drain$`, s.waitForHealthRecovery)
	sc.Step(`^the test health failure is cleared$`, s.clearHealthFailure)
}

func (s *WorkerRollingUpdate) healthJobs() *activeCheckJobs {
	return &activeCheckJobs{clusterName: s.info.SlurmClusterName, runtime: s.runtime, kubectl: s.kubectl}
}

func (s *WorkerRollingUpdate) requireHealthTestSetup(ctx context.Context) error {
	if s.nodeSet.Metadata.Name == "" {
		return fmt.Errorf("select a NodeSet before checking health-failure prerequisites")
	}
	if len(s.workers) != len(s.workerNames) {
		return fmt.Errorf("NodeSet %s requires %d active workers but only %d were discovered", s.nodeSet.Metadata.Name, len(s.workerNames), len(s.workers))
	}
	if len(s.workerNames) < 2 {
		s.runtime.Logf("Acceptance: NodeSet %s has fewer than two active workers; skipping health/rollout coexistence", s.nodeSet.Metadata.Name)
		return godog.ErrSkip
	}
	var deployments appsv1.DeploymentList
	if err := s.kubectl.GetJSON(ctx, &deployments, "get", "deployments", "-A", "-l", "control-plane=soperatorchecks", "-o", "json"); err != nil {
		return fmt.Errorf("discover node-replacement configuration: %w", err)
	}
	if len(deployments.Items) != 1 {
		return fmt.Errorf("determine node-replacement configuration: expected one soperatorchecks Deployment, found %d", len(deployments.Items))
	}
	deployment := deployments.Items[0]
	disabled, err := healthNodeReplacementDisabled(deployment.Spec.Template.Spec.Containers)
	if err != nil {
		return fmt.Errorf("inspect Deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}
	if !disabled {
		s.runtime.Logf("Acceptance: Deployment %s/%s enables node replacement; health-rollout scenarios require it disabled so host maintenance cannot race recovery, skipping", deployment.Namespace, deployment.Name)
		return godog.ErrSkip
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 || deployment.Status.ObservedGeneration < deployment.Generation ||
		deployment.Status.UpdatedReplicas != *deployment.Spec.Replicas || deployment.Status.AvailableReplicas != *deployment.Spec.Replicas ||
		deployment.Status.Replicas != *deployment.Spec.Replicas {
		return fmt.Errorf("soperatorchecks Deployment %s/%s is not fully available at its configured generation: %+v", deployment.Namespace, deployment.Name, deployment.Status)
	}
	if deployment.Spec.Selector == nil || len(deployment.Spec.Selector.MatchLabels) == 0 || len(deployment.Spec.Selector.MatchExpressions) != 0 {
		return fmt.Errorf("cannot determine live pods for soperatorchecks Deployment %s/%s", deployment.Namespace, deployment.Name)
	}
	var pods corev1.PodList
	if err := s.kubectl.GetJSON(ctx, &pods, "get", "pods", "-n", deployment.Namespace, "-l", labels.SelectorFromSet(deployment.Spec.Selector.MatchLabels).String(), "-o", "json"); err != nil {
		return err
	}
	if len(pods.Items) != int(*deployment.Spec.Replicas) {
		return fmt.Errorf("soperatorchecks Deployment %s/%s has %d live pods, expected %d", deployment.Namespace, deployment.Name, len(pods.Items), *deployment.Spec.Replicas)
	}
	for _, pod := range pods.Items {
		disabled, err := healthNodeReplacementDisabled(pod.Spec.Containers)
		if err != nil || !disabled || !kubeobjects.PodReady(pod) || pod.DeletionTimestamp != nil {
			return fmt.Errorf("soperatorchecks pod %s/%s must be Ready with node replacement disabled: disabled=%t ready=%t deleting=%t error=%v", pod.Namespace, pod.Name, disabled, kubeobjects.PodReady(pod), pod.DeletionTimestamp != nil, err)
		}
	}
	var source unstructured.Unstructured
	name := activeCheckResourceName(s.info.SlurmClusterName, activeGPUChecksSuffix)
	out, err := s.runtime.Kubectl().Run(ctx, "get", "activecheck", name, "-n", framework.SoperatorNamespace, "--ignore-not-found", "-o", "json")
	if err != nil {
		return fmt.Errorf("read ActiveCheck %s as health-check template: %w", name, err)
	}
	if strings.TrimSpace(out) == "" {
		s.runtime.Logf("Acceptance: GPU ActiveCheck %s is not configured; skipping controlled health failure", name)
		return godog.ErrSkip
	}
	if err := json.Unmarshal([]byte(out), &source); err != nil {
		return fmt.Errorf("decode ActiveCheck %s: %w", name, err)
	}
	if _, err := healthFailureManifest(&source, "acceptance-health-validation", s.info.SlurmClusterName, s.workers[0].Name, "/tmp/acceptance-health-validation"); err != nil {
		return fmt.Errorf("validate ActiveCheck %s template: %w", name, err)
	}
	// Discovery failure is a broken configured rollout environment, not an optional topology.
	if _, err := s.runtime.Kubectl().Run(ctx, "get", "containerrecreaterequests.apps.kruise.io", "-n", s.nodeSet.Metadata.Namespace, "-o", "name"); err != nil {
		return fmt.Errorf("discover worker recovery requests: %w", err)
	}
	s.health.source = source.DeepCopy()
	return nil
}

func healthNodeReplacementDisabled(containers []corev1.Container) (bool, error) {
	for _, container := range containers {
		if container.Name != "manager" {
			continue
		}
		var setting *bool
		for _, arg := range container.Args {
			flag := strings.TrimPrefix(arg, "-")
			flag = strings.TrimPrefix(flag, "-")
			if flag != "enable-node-replacement" && !strings.HasPrefix(flag, "enable-node-replacement=") {
				continue
			}
			if setting != nil {
				return false, fmt.Errorf("manager has multiple enable-node-replacement flags")
			}
			enabled := true
			if _, value, found := strings.Cut(flag, "="); found {
				var err error
				enabled, err = strconv.ParseBool(value)
				if err != nil {
					return false, fmt.Errorf("invalid enable-node-replacement flag %q", arg)
				}
			}
			setting = &enabled
		}
		if setting == nil {
			return false, fmt.Errorf("manager must explicitly configure --enable-node-replacement")
		}
		return !*setting, nil
	}
	return false, fmt.Errorf("manager container not found")
}

func healthFailureManifest(source *unstructured.Unstructured, name, cluster, worker, gate string) (*unstructured.Unstructured, error) {
	if source == nil {
		return nil, fmt.Errorf("ActiveCheck template was not captured")
	}
	checkType, _, _ := unstructured.NestedString(source.Object, "spec", "checkType")
	clusterRef, _, _ := unstructured.NestedString(source.Object, "spec", "slurmClusterRefName")
	if checkType != "slurmJob" || clusterRef != cluster {
		return nil, fmt.Errorf("expected slurmJob ActiveCheck for cluster %s, got checkType=%q cluster=%q", cluster, checkType, clusterRef)
	}
	for _, container := range []string{"jobContainer", "mungeContainer"} {
		image, _, _ := unstructured.NestedString(source.Object, "spec", "slurmJobSpec", container, "image")
		if strings.TrimSpace(image) == "" {
			return nil, fmt.Errorf("ActiveCheck %s image is missing", container)
		}
	}
	spec, found, err := unstructured.NestedMap(source.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("read ActiveCheck spec: found=%t error=%v", found, err)
	}
	slurmSpec := spec["slurmJobSpec"].(map[string]any)
	jobContainer := slurmSpec["jobContainer"].(map[string]any)
	// Keep the deployed image, authentication, jail and scheduling configuration,
	// but make submission independent of the GPU check's resource requirements.
	delete(jobContainer, "command")
	delete(jobContainer, "args")
	if rawEnv, ok := jobContainer["env"]; ok {
		env, ok := rawEnv.([]any)
		if !ok {
			return nil, fmt.Errorf("ActiveCheck jobContainer.env is not a list")
		}
		filtered := make([]any, 0, len(env))
		for _, value := range env {
			entry, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("ActiveCheck jobContainer.env contains a non-object")
			}
			key, _ := entry["name"].(string)
			if strings.HasPrefix(key, "SBATCH_") || strings.HasPrefix(key, "ACTIVE_CHECK_") || key == "EACH_WORKER_JOBS" {
				continue
			}
			filtered = append(filtered, entry)
		}
		jobContainer["env"] = filtered
	}
	delete(spec, "dependsOn")
	delete(spec, "successReactions")
	delete(spec, "podTemplateNameRef") // A borrowed PodTemplate could replace the controlled command or resources.
	spec["name"], spec["suspend"], spec["runAfterCreation"] = name, true, false
	spec["schedule"], spec["activeDeadlineSeconds"] = "0 0 1 1 *", int64(300)
	spec["failureReactions"] = map[string]any{"drainSlurmNode": map[string]any{"drainReasonPrefix": "[node_problem]"}}
	slurmSpec["eachWorkerJobs"], slurmSpec["maxNumberOfJobs"] = false, int64(0)
	delete(slurmSpec, "sbatchScriptRefName")
	slurmSpec["sbatchScript"] = healthFailureScript(worker, gate)
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "slurm.nebius.ai/v1alpha1", "kind": "ActiveCheck",
		"metadata": map[string]any{"name": name, "namespace": framework.SoperatorNamespace},
		"spec":     spec,
	}}, nil
}

func healthFailureScript(worker, gate string) string {
	return fmt.Sprintf(`#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64M
#SBATCH --time=65:00
#SBATCH --no-requeue
#SBATCH --nodelist=%s
set -euo pipefail
deadline=$((SECONDS + 3600))
while [[ ! -e %s ]]; do
  if (( SECONDS >= deadline )); then exit 124; fi
  sleep 1
done
printf 'Intentional acceptance health-check failure\n'
exit 1
`, worker, framework.ShellQuote(gate))
}

func (s *WorkerRollingUpdate) prepareHealthFailure(ctx context.Context) error {
	if s.health.source == nil || len(s.workers) == 0 {
		return fmt.Errorf("validate controlled health failure prerequisites before starting the check")
	}
	artifacts, ok := framework.ScenarioArtifacts(ctx)
	if !ok {
		return fmt.Errorf("controlled health failure requires assigned scenario artifact paths")
	}
	s.health.artifacts = artifacts
	s.health.checkName = "acceptance-rollout-health-" + strings.ToLower(rand.Text()[:12])
	s.health.k8sJobName = s.health.checkName + "-run"
	s.health.gatePath = path.Join(artifacts.JailDir, s.health.checkName+".release")
	pod, err := s.healthWorkerPod(ctx)
	if err != nil {
		return err
	}
	s.health.containerIDs = make(map[string]string, len(pod.Status.ContainerStatuses))
	for _, container := range pod.Status.ContainerStatuses {
		s.health.containerIDs[container.Name] = container.ContainerID
	}
	if s.health.containerIDs["slurmd"] == "" {
		return fmt.Errorf("worker %s has no slurmd container identity", pod.Name)
	}
	manifest, err := healthFailureManifest(s.health.source, s.health.checkName, s.info.SlurmClusterName, s.workers[0].Name, s.health.gatePath)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestPath := filepath.Join(artifacts.RunnerDir, s.health.checkName+".json")
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write controlled ActiveCheck manifest: %w", err)
	}
	if _, err := s.runtime.Kubectl().Run(ctx, "create", "-f", manifestPath); err != nil {
		return fmt.Errorf("create controlled ActiveCheck %s: %w", s.health.checkName, err)
	}
	if err := s.waitForRollout(ctx, "controlled health-check CronJob created", activeK8sJobTimeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		out, err := s.runtime.Kubectl().Run(waitCtx, "get", "cronjob", s.health.checkName, "-n", framework.SoperatorNamespace, "--ignore-not-found", "-o", "name")
		return strings.TrimSpace(out) != "", err
	}); err != nil {
		return err
	}
	jobs := s.healthJobs()
	if err := jobs.createNamedJobFromCronJob(ctx, s.health.checkName, s.health.k8sJobName); err != nil {
		return err
	}
	ids, err := jobs.waitForK8sJobSlurmJobIDs(ctx, s.health.k8sJobName, activeK8sJobTimeout)
	if err != nil {
		return err
	}
	if len(ids) != 1 {
		return fmt.Errorf("controlled ActiveCheck %s submitted %d Slurm jobs, expected one: %v", s.health.checkName, len(ids), ids)
	}
	s.health.jobID = ids[0]
	if err := jobs.waitForK8sJobCompleteSuccessfully(ctx, s.health.k8sJobName, activeK8sJobTimeout); err != nil {
		return err
	}
	return s.slurm.WaitForJobRunning(ctx, s.health.jobID, activeK8sJobTimeout)
}

func (s *WorkerRollingUpdate) failHealthCheckDuringHandoff(ctx context.Context) error {
	if err := s.waitForWorkerHandoff(ctx); err != nil {
		return err
	}
	return s.failHealthCheck(ctx)
}

func (s *WorkerRollingUpdate) failHealthCheck(ctx context.Context) error {
	if s.health.jobID == "" || s.health.gatePath == "" {
		return fmt.Errorf("start the controlled health check before failing it")
	}
	if err := s.slurm.AssertJobRunningWithoutRestarts(ctx, s.health.jobID); err != nil {
		return fmt.Errorf("controlled health check must still be running at failure injection: %w", err)
	}
	if _, err := s.runtime.Jail().Run(ctx, "touch -- "+framework.ShellQuote(s.health.gatePath)); err != nil {
		return fmt.Errorf("release controlled health-check failure: %w", err)
	}
	return s.waitForRollout(ctx, "controlled ActiveCheck fails and drains the original worker", workerHealthFailureTimeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		if !s.job.IsZero() {
			if err := s.assertRolloutJobRunning(waitCtx); err != nil {
				return false, err
			}
		}
		if _, err := s.healthWorkerPod(waitCtx); err != nil {
			return false, err
		}
		status, matches, err := s.healthJobs().slurmRun(waitCtx, s.health.checkName, s.health.jobID)
		if err != nil || !matches {
			return false, err
		}
		if status.LastRunStatus != kubeobjects.ActiveCheckSlurmRunStatusFailed {
			switch status.LastRunStatus {
			case kubeobjects.ActiveCheckSlurmRunStatusComplete, kubeobjects.ActiveCheckSlurmRunStatusCancelled, kubeobjects.ActiveCheckSlurmRunStatusError, kubeobjects.ActiveCheckSlurmRunStatusSkipped:
				return false, rolloutInvariant("controlled ActiveCheck %s expected Failed, got %s", s.health.checkName, status.LastRunStatus)
			}
			return false, fmt.Errorf("ActiveCheck %s run %s status=%s", s.health.checkName, s.health.jobID, status.LastRunStatus)
		}
		job, err := s.slurm.JobInfo(waitCtx, s.health.jobID)
		if err != nil {
			return false, err
		}
		if !job.SacctFound || job.IsAlive() {
			return false, fmt.Errorf("controlled health check %s accounting pending: state=%s queue=%s", s.health.jobID, job.SacctState, job.QueueState)
		}
		if job.SacctState != "FAILED" || job.SacctExit != "1:0" || (job.Restarts != nil && *job.Restarts > 0) {
			return false, rolloutInvariant("controlled health check %s must fail deliberately once: state=%s exit=%s restarts=%v", s.health.jobID, job.SacctState, job.SacctExit, job.Restarts)
		}
		if !job.HasNoRestarts() {
			return false, fmt.Errorf("read restart count for controlled health check %s", s.health.jobID)
		}
		node, err := s.slurm.NodeInfoOnce(waitCtx, s.workers[0].Name)
		if err != nil {
			return false, err
		}
		if !node.HasStateFlag("DRAIN") || !healthReasonMatches(node.Reason, s.health.checkName, s.health.jobID) {
			return false, fmt.Errorf("wait for controlled health failure drain on %s: state=%s reason=%q", node.Name, node.State, node.Reason)
		}
		return true, nil
	})
}

func (s *WorkerRollingUpdate) healthWorkerPod(ctx context.Context) (corev1.Pod, error) {
	if len(s.workers) == 0 {
		return corev1.Pod{}, fmt.Errorf("health-failure worker was not selected")
	}
	name := s.workers[0].Name
	var pod corev1.Pod
	if err := s.kubectl.GetJSON(ctx, &pod, "get", "pod", name, "-n", s.nodeSet.Metadata.Namespace, "-o", "json"); err != nil {
		return pod, err
	}
	initial, ok := s.initialPods[name]
	if !ok || pod.UID != initial.UID || pod.Spec.NodeName != initial.NodeName || pod.DeletionTimestamp != nil {
		return pod, rolloutInvariant("unhealthy worker %s must stay in pod UID=%s on host=%s; observed UID=%s host=%s deleting=%t", name, initial.UID, initial.NodeName, pod.UID, pod.Spec.NodeName, pod.DeletionTimestamp != nil)
	}
	return pod, nil
}

func (s *WorkerRollingUpdate) healthDrainedWorker(ctx context.Context) (corev1.Pod, framework.SlurmNodeInfo, error) {
	pod, err := s.healthWorkerPod(ctx)
	if err != nil {
		return pod, framework.SlurmNodeInfo{}, err
	}
	node, err := s.slurm.NodeInfoOnce(ctx, pod.Name)
	if err == nil && (!node.HasStateFlag("DRAIN") || !healthReasonMatches(node.Reason, s.health.checkName, s.health.jobID)) {
		err = rolloutInvariant("worker %s lost the controlled health drain: state=%s reason=%q", pod.Name, node.State, node.Reason)
	}
	return pod, node, err
}

func (s *WorkerRollingUpdate) waitForHealthyPeers(ctx context.Context) error {
	timeout := workerRolloutReplaceBase + time.Duration(len(s.workers)-1)*workerRolloutReplacePerPod
	return s.waitForRollout(ctx, "healthy peers updated while original unhealthy worker remains protected", timeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		pod, node, err := s.healthDrainedWorker(waitCtx)
		if err != nil {
			return false, err
		}
		if pod.Labels[workerHealthOperationPhase] != "" || pod.Labels[workerHealthOperationID] != "" || node.HasStateFlag("REBOOT_REQUESTED") || node.HasStateFlag("REBOOT_ISSUED") {
			return false, rolloutInvariant("pre-existing health drain must defer handoff for %s: phase=%q operation=%q state=%s", pod.Name, pod.Labels[workerHealthOperationPhase], pod.Labels[workerHealthOperationID], node.State)
		}
		if current := healthContainerID(pod, "slurmd"); current != s.health.containerIDs["slurmd"] {
			return false, rolloutInvariant("pre-existing unhealthy worker %s restarted slurmd: before=%s after=%s", pod.Name, s.health.containerIDs["slurmd"], current)
		}
		if !s.job.IsZero() {
			if err := s.assertRolloutJobRunning(waitCtx); err != nil {
				return false, err
			}
		}
		pods, err := s.nodeSetPods(waitCtx)
		if err != nil {
			return false, err
		}
		remaining := healthPendingPeers(s.workers[0].Name, s.initialPods, pods, s.annotationValue)
		if len(remaining) != 0 {
			return false, fmt.Errorf("healthy peer updates pending: %s", strings.Join(remaining, ", "))
		}
		return true, nil
	})
}

func healthPendingPeers(unhealthy string, initial map[string]workerPodSnapshot, pods []corev1.Pod, annotation string) []string {
	var remaining []string
	byName := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		byName[pod.Name] = pod
	}
	for name, original := range initial {
		if name == unhealthy {
			continue
		}
		pod, found := byName[name]
		if !found || pod.UID == original.UID || pod.DeletionTimestamp != nil || !kubeobjects.PodReady(pod) || pod.Annotations[workerRolloutAnnotationKey] != annotation {
			remaining = append(remaining, name)
		}
	}
	sort.Strings(remaining)
	return remaining
}

func (s *WorkerRollingUpdate) waitForHealthRecovery(ctx context.Context) error {
	return s.waitForRollout(ctx, "slurmd recovered in the original health-drained worker pod", workerHealthRecoveryTimeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		pod, node, err := s.healthDrainedWorker(waitCtx)
		if err != nil {
			return false, err
		}
		if pod.Annotations[workerHealthRecoveryRequest] == "" {
			return false, fmt.Errorf("worker %s recovery request pending: phase=%q state=%s", pod.Name, pod.Labels[workerHealthOperationPhase], node.State)
		}
		return s.healthRecoveryComplete(waitCtx, pod, node)
	})
}

func (s *WorkerRollingUpdate) healthRecoveryComplete(ctx context.Context, pod corev1.Pod, node framework.SlurmNodeInfo) (bool, error) {
	name := pod.Annotations[workerHealthRecoveryRequest]
	var request workerHealthCRR
	if err := s.kubectl.GetJSON(ctx, &request, "get", "containerrecreaterequest", name, "-n", pod.Namespace, "-o", "json"); err != nil {
		return false, err
	}
	return healthRecoveryObserved(pod, node, request, s.health.containerIDs)
}

func healthRecoveryObserved(pod corev1.Pod, node framework.SlurmNodeInfo, request workerHealthCRR, initialContainers map[string]string) (bool, error) {
	initialID := initialContainers["slurmd"]
	if initialID == "" || pod.Annotations[workerHealthRecoveryContainer] != initialID ||
		request.Metadata.Name != pod.Annotations[workerHealthRecoveryRequest] || request.Spec.PodName != pod.Name ||
		len(request.Spec.Containers) != 1 || request.Spec.Containers[0].Name != "slurmd" {
		return false, rolloutInvariant("worker %s recovery request %s must target only its original slurmd container %s", pod.Name, request.Metadata.Name, initialID)
	}
	// Kruise supplies these fields. Do not mistake an observation before their
	// population for a completed request aimed at the wrong container.
	uid := request.Metadata.Labels["crr.apps.kruise.io/pod-uid"]
	container := request.Spec.Containers[0].StatusContext
	if request.Status.Phase != "Completed" && (uid == "" || container == nil || container.ContainerID == "") {
		return false, fmt.Errorf("recovery request %s identity is not populated yet", request.Metadata.Name)
	}
	if uid != string(pod.UID) || container == nil || container.ContainerID != initialID {
		return false, rolloutInvariant("worker %s recovery request %s must target only its original slurmd container %s", pod.Name, request.Metadata.Name, initialID)
	}
	owned := false
	for _, owner := range request.Metadata.OwnerReferences {
		if owner.Kind == "Pod" && owner.Name == pod.Name && owner.UID == pod.UID && owner.Controller != nil && *owner.Controller {
			owned = true
		}
	}
	if !owned {
		return false, rolloutInvariant("worker recovery request %s is not controlled by original pod %s/%s", request.Metadata.Name, pod.Name, pod.UID)
	}
	for name, id := range initialContainers {
		if name != "slurmd" && healthContainerID(pod, name) != id {
			return false, rolloutInvariant("worker %s recovery unexpectedly restarted container %s", pod.Name, name)
		}
	}
	if request.Status.Phase != "Completed" {
		return false, fmt.Errorf("recovery request %s phase=%s message=%s", request.Metadata.Name, request.Status.Phase, request.Status.Message)
	}
	succeeded := false
	for _, state := range request.Status.ContainerRecreateStates {
		if state.Name == "slurmd" && state.Phase == "Succeeded" {
			succeeded = true
		}
	}
	if !succeeded {
		return false, rolloutInvariant("recovery request %s completed without successful slurmd recreation: %s %+v", request.Metadata.Name, request.Status.Message, request.Status.ContainerRecreateStates)
	}
	if id := healthContainerID(pod, "slurmd"); id == "" || id == initialID || !healthWorkerSettled(node) || !kubeobjects.PodReady(pod) {
		return false, fmt.Errorf("worker %s recovery registration pending: container=%s original=%s state=%s cpu=%v memory=%v ready=%t", pod.Name, id, initialID, node.State, node.CPUAlloc, node.AllocMemoryMiB, kubeobjects.PodReady(pod))
	}
	return true, nil
}

func healthContainerID(pod corev1.Pod, name string) string {
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == name {
			return container.ContainerID
		}
	}
	return ""
}

func healthWorkerSettled(node framework.SlurmNodeInfo) bool {
	if !node.HasStateFlag("IDLE") || node.CPUAlloc == nil || *node.CPUAlloc != 0 || node.AllocMemoryMiB == nil || *node.AllocMemoryMiB != 0 {
		return false
	}
	for _, flag := range []string{"COMPLETING", "REBOOT_REQUESTED", "REBOOT_ISSUED", "DOWN", "NOT_RESPONDING", "INVALID_REG", "FAIL"} {
		if node.HasStateFlag(flag) {
			return false
		}
	}
	return true
}

func healthReasonMatches(reason, checkName, jobID string) bool {
	if checkName == "" || jobID == "" {
		return false
	}
	observedID, ok := healthReasonJobID(reason, checkName)
	return ok && observedID == jobID
}

func healthReasonJobID(reason, checkName string) (string, bool) {
	if checkName == "" {
		return "", false
	}
	reason = slurmReasonAttribution.ReplaceAllString(strings.TrimSpace(reason), "")
	reason = strings.TrimSuffix(reason, " : reboot issued")
	id, ok := strings.CutPrefix(reason, "[node_problem] "+checkName+": job ")
	if !ok {
		return "", false
	}
	id, ok = strings.CutSuffix(id, " [slurm_job]")
	if !ok || id == "" || strings.IndexFunc(id, func(r rune) bool { return r < '0' || r > '9' }) != -1 {
		return "", false
	}
	return id, true
}

func (s *WorkerRollingUpdate) clearHealthFailure(ctx context.Context) error {
	if err := s.waitForRollout(ctx, "health-drained worker settled before clearing its test fault", workerHealthRecoveryTimeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		_, node, err := s.healthDrainedWorker(waitCtx)
		if err != nil {
			return false, err
		}
		if !healthWorkerSettled(node) {
			return false, fmt.Errorf("worker %s not settled: state=%s CPUAlloc=%v AllocMem=%v", node.Name, node.State, node.CPUAlloc, node.AllocMemoryMiB)
		}
		return true, nil
	}); err != nil {
		return err
	}
	return s.slurm.ResumeNodeIfDrainedByReason(ctx, s.workers[0].Name, fmt.Sprintf("[node_problem] %s: job %s [slurm_job]", s.health.checkName, s.health.jobID))
}

func healthCancelCommand(checkName string) string {
	return "scancel --name=" + framework.ShellQuote(checkName)
}

func (s *WorkerRollingUpdate) cleanupHealth(ctx context.Context) {
	defer func() { s.health = workerRolloutHealth{} }()
	if s.health.checkName == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerHealthRecoveryTimeout+2*time.Minute)
	defer cancel()
	// Stop submission before cancelling by the unique job name; even a failed
	// annotation write after sbatch cannot leave an unrecorded Slurm job behind.
	submissionStopped := true
	if s.health.k8sJobName != "" {
		if _, err := s.runtime.Kubectl().Run(cleanupCtx, "delete", "job", s.health.k8sJobName, "-n", framework.SoperatorNamespace, "--ignore-not-found", "--cascade=foreground", "--wait=true", "--timeout=60s"); err != nil {
			submissionStopped = false
			s.runtime.Logf("Cleanup: stop health-check submission Job %s: %v", s.health.k8sJobName, err)
		}
	}
	if _, err := s.runtime.Controller().Run(cleanupCtx, healthCancelCommand(s.health.checkName)); err != nil {
		s.runtime.Logf("Cleanup: cancel health-check Slurm jobs named %s: %v", s.health.checkName, err)
	}
	jobsStopped := true
	if err := s.runtime.WaitFor(cleanupCtx, "controlled health-check jobs removed from Slurm", 2*time.Minute, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		out, err := s.runtime.Controller().Run(waitCtx, "squeue --noheader --name="+framework.ShellQuote(s.health.checkName)+" --format=%A")
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(out) != "" {
			return false, fmt.Errorf("health-check jobs still present: %s", strings.TrimSpace(out))
		}
		return true, nil
	}); err != nil {
		jobsStopped = false
		s.runtime.Logf("Cleanup: wait for health-check jobs named %s to stop: %v", s.health.checkName, err)
	}
	if _, err := s.runtime.Kubectl().Run(cleanupCtx, "delete", "activecheck", s.health.checkName, "-n", framework.SoperatorNamespace, "--ignore-not-found", "--wait=false"); err != nil {
		s.runtime.Logf("Cleanup: delete controlled ActiveCheck %s: %v", s.health.checkName, err)
	}
	if len(s.workers) > 0 && submissionStopped && jobsStopped {
		worker := s.workers[0].Name
		err := s.runtime.WaitFor(cleanupCtx, "owned health drain settled before cleanup", workerHealthRecoveryTimeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
			node, err := s.slurm.NodeInfoOnce(waitCtx, worker)
			if err != nil {
				return false, err
			}
			ownedJobID, owned := healthReasonJobID(node.Reason, s.health.checkName)
			if !node.HasStateFlag("DRAIN") || !owned || (s.health.jobID != "" && s.health.jobID != ownedJobID) {
				return true, nil
			}
			pod, err := s.healthWorkerPod(waitCtx)
			if err != nil {
				return false, err
			}
			if !healthWorkerSettled(node) {
				return false, fmt.Errorf("worker %s still recovering: state=%s", worker, node.State)
			}
			if pod.Annotations[workerHealthRecoveryRequest] != "" {
				done, err := s.healthRecoveryComplete(waitCtx, pod, node)
				if err != nil || !done {
					return false, err
				}
			}
			return true, s.slurm.ResumeNodeIfDrainedByReason(waitCtx, worker, fmt.Sprintf("[node_problem] %s: job %s [slurm_job]", s.health.checkName, ownedJobID))
		})
		if err != nil {
			s.runtime.Logf("Cleanup: preserve unsafe or unrelated drain on worker %s after controlled ActiveCheck %s: %v", worker, s.health.checkName, err)
		}
	} else if !submissionStopped || !jobsStopped {
		s.runtime.Logf("Cleanup: preserve worker drain because ActiveCheck %s submission or jobs could not be confirmed stopped", s.health.checkName)
	}
	if s.health.gatePath != "" {
		if _, err := s.runtime.Jail().Run(cleanupCtx, "rm -f -- "+framework.ShellQuote(s.health.gatePath)); err != nil {
			s.runtime.Logf("Cleanup: remove health-check gate %s: %v", s.health.gatePath, err)
		}
	}
}
