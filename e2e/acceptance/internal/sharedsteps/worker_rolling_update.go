package sharedsteps

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cucumber/godog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

const (
	workerRolloutAnnotationKey  = "slurm.nebius.ai/e2e-rollout"
	workerRolloutJobName        = "e2e-worker-rolling-update"
	workerRolloutJobGateTimeout = time.Hour
	workerRolloutHandoffTimeout = 5 * time.Minute
	workerRolloutIdleTimeout    = 4 * time.Minute
	workerRolloutDaemonSetWait  = 10 * time.Minute
	workerRolloutJobStartWait   = 5 * time.Minute
	workerRolloutJobFinishWait  = 5 * time.Minute
	workerRolloutReplaceBase    = 3 * time.Minute
	workerRolloutReplacePerPod  = 6 * time.Minute
	workerRolloutStuckMargin    = 90 * time.Second
	workerRolloutSmokeTimeout   = 3 * time.Minute
	workerRolloutCleanupTimeout = 2 * time.Minute
)

type WorkerRollingUpdate struct {
	info     *framework.ClusterInfo
	runtime  framework.Runtime
	slurm    *framework.SlurmClient
	kubectl  *framework.KubectlClient
	selector *framework.WorkerSelector

	nodeSet         kubeobjects.NodeSet
	workerNames     []string
	workers         []framework.WorkerInfo
	initialPods     map[string]workerPodSnapshot
	annotationValue string
	job             framework.SbatchJob
	jobReleasePath  string
	rolloutBudget   int
	health          workerRolloutHealth
	mk8s            workerRolloutMK8s
	power           workerRolloutPower
}

type workerPodSnapshot struct {
	UID               types.UID
	NodeName          string
	StartTime         time.Time
	SlurmdContainerID string
}

func NewWorkerRollingUpdate(
	info *framework.ClusterInfo,
	runtime framework.Runtime,
	slurm *framework.SlurmClient,
	kubectl *framework.KubectlClient,
	selector *framework.WorkerSelector,
) *WorkerRollingUpdate {
	return &WorkerRollingUpdate{
		info:     info,
		runtime:  runtime,
		slurm:    slurm,
		kubectl:  kubectl,
		selector: selector,
	}
}

func (s *WorkerRollingUpdate) RegisterSteps(sc *godog.ScenarioContext) {
	sc.Step(`^a ready GPU NodeSet with active workers and slurm-aware rolling update is selected$`, s.selectNodeSet)
	sc.Step(`^the Soperator system components have been upgraded since the workers started$`, s.upgradeSystemComponents)
	sc.Step(`^a test job is running on one of its workers$`, s.submitJobOnWorker)
	sc.Step(`^the worker template of the NodeSet is changed$`, s.changeWorkerTemplate)
	sc.Step(`^the worker handoff has started while the test job is running$`, s.waitForWorkerHandoff)
	sc.Step(`^the running job finishes without being killed$`, s.waitForJobCompleted)
	sc.Step(`^every active worker pod of the NodeSet is replaced within the rollout budget$`, s.waitForWorkersReplaced)
	sc.Step(`^all active workers of the NodeSet are idle with no leftover drain reason$`, s.waitForWorkersIdle)
	sc.Step(`^the updated NodeSet accepts a targeted smoke job$`, s.runSmokeJobs)
	s.registerHealthSteps(sc)
	s.registerMK8sSteps(sc)
}

// CleanupAndReset removes temporary jobs, health failures, and suspension holds.
// Worker template annotations and MK8s max_pods changes are intentionally retained
// to avoid a second rollout. Each run selects a new annotation or max_pods value.
func (s *WorkerRollingUpdate) CleanupAndReset(ctx context.Context) {
	if !s.job.IsZero() {
		cleanupCtx, cancel := context.WithTimeout(ctx, workerRolloutCleanupTimeout)
		if err := s.slurm.CancelJob(cleanupCtx, s.job.ID, 0); err != nil {
			s.runtime.Logf("cleanup: cancel rolling update job %s: %v", s.job.ID, err)
		}
		cancel()
	} else if s.job.JobName != "" {
		// Submission may succeed even if its response (and therefore job ID) is lost.
		cleanupCtx, cancel := context.WithTimeout(ctx, workerRolloutCleanupTimeout)
		if _, err := s.runtime.Controller().Run(cleanupCtx, "scancel --name="+framework.ShellQuote(s.job.JobName)); err != nil {
			s.runtime.Logf("Cleanup: cancel rollout job named %s: %v", s.job.JobName, err)
		}
		cancel()
	}
	s.cleanupHealth(ctx)
	s.cleanupEphemeralWorkerHold(ctx)
	if s.jobReleasePath != "" {
		if _, err := s.runtime.Jail().Run(ctx, "rm -f -- "+framework.ShellQuote(s.jobReleasePath)); err != nil {
			s.runtime.Logf("Cleanup: remove rollout gate %s: %v", s.jobReleasePath, err)
		}
	}

	s.mk8s = workerRolloutMK8s{}
	s.nodeSet = kubeobjects.NodeSet{}
	s.workerNames = nil
	s.workers = nil
	s.initialPods = nil
	s.annotationValue = ""
	s.job = framework.SbatchJob{}
	s.jobReleasePath = ""
	s.rolloutBudget = 0
}

func (s *WorkerRollingUpdate) selectNodeSet(ctx context.Context) error {
	var nodeSets kubeobjects.NodeSetList
	if err := s.kubectl.GetJSON(ctx, &nodeSets, "get", "nodesets", "-n", framework.SoperatorNamespace, "-o", "json"); err != nil {
		return fmt.Errorf("list NodeSets: %w", err)
	}
	sort.Slice(nodeSets.Items, func(i, j int) bool {
		return nodeSets.Items[i].Metadata.Name < nodeSets.Items[j].Metadata.Name
	})

	var rejected []string
	for _, nodeSet := range nodeSets.Items {
		if reason := workerRolloutConfigurationProblem(nodeSet, s.info.SlurmClusterName); reason != "" {
			rejected = append(rejected, fmt.Sprintf("%s: %s", nodeSet.Metadata.Name, reason))
			continue
		}
		names, err := s.rolloutWorkerNames(ctx, nodeSet)
		if err != nil {
			return err
		}
		if reason := workerRolloutCandidateProblem(nodeSet, s.info.SlurmClusterName, len(names)); reason != "" {
			return fmt.Errorf("require configured rollout NodeSet %s to be healthy: %s", nodeSet.Metadata.Name, reason)
		}
		if len(names) == 0 {
			rejected = append(rejected, nodeSet.Metadata.Name+": no active workers")
			continue
		}
		s.nodeSet = nodeSet
		s.workerNames = names
		break
	}
	if s.nodeSet.Metadata.Name == "" {
		s.runtime.Logf("acceptance: no ready GPU NodeSet with active workers and %s was found (%s), skipping scenario",
			kubeobjects.NodeSetUpdateStrategySlurmAwareRollingUpdate, strings.Join(rejected, "; "))
		return godog.ErrSkip
	}

	budget, err := workerRolloutBudget(s.nodeSet.Spec.MaxUnavailable, len(s.workerNames))
	if err != nil {
		return fmt.Errorf("read NodeSet %s rollout budget: %w", s.nodeSet.Metadata.Name, err)
	}
	s.rolloutBudget = budget
	if isEphemeralRollout(s.nodeSet) {
		if err := s.holdEphemeralWorkers(ctx, s.workerNames); err != nil {
			return err
		}
		if err := s.checkRolloutWorkerSet(ctx); err != nil {
			return err
		}
	}
	pods, err := s.nodeSetPods(ctx)
	if err != nil {
		return err
	}
	if err := requireRolloutPodNames(s.workerNames, pods); err != nil {
		return fmt.Errorf("check NodeSet %s active pods: %w", s.nodeSet.Metadata.Name, err)
	}
	s.initialPods = make(map[string]workerPodSnapshot, len(pods))
	for _, pod := range pods {
		if !kubeobjects.PodReady(pod) || pod.DeletionTimestamp != nil || pod.Status.StartTime == nil || pod.UID == "" || pod.Spec.NodeName == "" {
			return fmt.Errorf("worker pod %s is not a running ready pod: phase=%s deletionTimestamp=%v", pod.Name, pod.Status.Phase, pod.DeletionTimestamp)
		}
		snapshot := workerPodSnapshot{UID: pod.UID, NodeName: pod.Spec.NodeName, StartTime: pod.Status.StartTime.Time}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "slurmd" {
				snapshot.SlurmdContainerID = container.ContainerID
			}
		}
		s.initialPods[pod.Name] = snapshot
	}

	snapshot, err := s.selector.Snapshot(ctx)
	if err != nil {
		return err
	}
	pool := snapshot.WorkersByNodeSet[s.nodeSet.Metadata.Name]
	if !isEphemeralRollout(s.nodeSet) && len(pool) != len(s.workerNames) {
		return fmt.Errorf("NodeSet %s has %d Slurm workers in the main partition, expected %d", s.nodeSet.Metadata.Name, len(pool), len(s.workerNames))
	}
	s.workers, err = selectRolloutWorkers(s.workerNames, pool)
	if err != nil {
		return fmt.Errorf("select NodeSet %s active Slurm workers: %w", s.nodeSet.Metadata.Name, err)
	}
	return s.waitForIdle(ctx, "selected NodeSet workers idle before the rollout")
}

// upgradeSystemComponents reproduces the part of a Soperator upgrade that precedes worker rollouts:
// the operator DaemonSets are rolled out, so their pods on worker nodes are younger than the workers.
func (s *WorkerRollingUpdate) upgradeSystemComponents(ctx context.Context) error {
	daemonSets, err := s.systemDaemonSets(ctx)
	if err != nil {
		return err
	}
	if len(daemonSets) == 0 {
		return fmt.Errorf("find DaemonSets in namespace %s", framework.SoperatorSystemNamespace)
	}
	previousGenerations := make(map[string]int64, len(daemonSets))
	for _, daemonSet := range daemonSets {
		previousGenerations[daemonSet.Name] = daemonSet.Generation
		if _, err := s.runtime.Kubectl().RunWithDefaultRetry(ctx,
			"rollout", "restart", "daemonset", daemonSet.Name, "-n", framework.SoperatorSystemNamespace); err != nil {
			return fmt.Errorf("restart DaemonSet %s: %w", daemonSet.Name, err)
		}
	}

	if err := s.runtime.WaitFor(ctx, "Soperator system DaemonSets rolled out", workerRolloutDaemonSetWait, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			current, err := s.systemDaemonSets(waitCtx)
			if err != nil {
				return false, err
			}
			var pending []string
			for _, daemonSet := range current {
				if !daemonSetRolledOut(daemonSet, previousGenerations[daemonSet.Name]) {
					pending = append(pending, fmt.Sprintf("%s updated=%d/%d available=%d",
						daemonSet.Name, daemonSet.Status.UpdatedNumberScheduled, daemonSet.Status.DesiredNumberScheduled, daemonSet.Status.NumberAvailable))
				}
			}
			if len(pending) > 0 {
				return false, fmt.Errorf("DaemonSets still rolling: %s", strings.Join(pending, "; "))
			}
			return true, nil
		}); err != nil {
		return err
	}

	// The scenario is only meaningful when a system pod was (re)started on each worker node after its worker.
	for podName, snapshot := range s.initialPods {
		var pods corev1.PodList
		if err := s.kubectl.GetJSON(ctx, &pods, "get", "pods", "-n", framework.SoperatorSystemNamespace,
			"--field-selector", "spec.nodeName="+snapshot.NodeName, "-o", "json"); err != nil {
			return fmt.Errorf("list system pods on node %s: %w", snapshot.NodeName, err)
		}
		if !daemonSetPodStartedAfter(snapshot.StartTime, pods.Items) {
			return fmt.Errorf("find a Soperator system DaemonSet pod on node %s started after worker pod %s (%s)",
				snapshot.NodeName, podName, snapshot.StartTime.Format(time.RFC3339))
		}
	}
	return nil
}

func (s *WorkerRollingUpdate) submitJobOnWorker(ctx context.Context) error {
	if len(s.workers) == 0 {
		return fmt.Errorf("select a NodeSet before submitting the job")
	}
	if err := s.checkRolloutWorkerSet(ctx); err != nil {
		return err
	}
	worker := s.workers[0]
	gateDir := framework.AcceptanceJobOutputDir
	if artifacts, ok := framework.ScenarioArtifacts(ctx); ok {
		gateDir = artifacts.JailDir
	}
	s.jobReleasePath = path.Join(gateDir, fmt.Sprintf("worker-rollout-%d.release", time.Now().UnixNano()))
	s.job = framework.SbatchJob{JobName: fmt.Sprintf("%s-%d", workerRolloutJobName, time.Now().UnixNano())}
	opts := framework.SbatchOptions{
		JobName:    s.job.JobName,
		Nodelist:   []string{worker.Name},
		Nodes:      1,
		ExtraFlags: []string{"--no-requeue", "--time=65", "--cpus-per-task=1", "--mem=128M"},
		Wrap:       rolloutJobGate(s.jobReleasePath, workerRolloutJobGateTimeout),
	}
	if s.nodeSet.Spec.GPU.Enabled {
		opts.GPUsPerNode = 1
	}
	job, err := s.slurm.SubmitBatch(ctx, opts)
	if err != nil {
		return err
	}
	s.job = job
	s.runtime.Logf("worker rolling update: submitted job id=%s on %s stdout=%s", job.ID, worker.Name, job.StdoutPath)
	return s.slurm.WaitForJobRunning(ctx, job.ID, workerRolloutJobStartWait)
}

func (s *WorkerRollingUpdate) changeWorkerTemplate(ctx context.Context) error {
	if err := s.checkRolloutWorkerSet(ctx); err != nil {
		return err
	}
	s.annotationValue = fmt.Sprintf("%d", time.Now().UnixNano())
	patch := fmt.Sprintf(`{"spec":{"workerAnnotations":{%q:%q}}}`, workerRolloutAnnotationKey, s.annotationValue)
	if _, err := s.runtime.Kubectl().RunWithDefaultRetry(ctx,
		"patch", "nodeset", s.nodeSet.Metadata.Name, "-n", s.nodeSet.Metadata.Namespace,
		"--type=merge", "-p", patch); err != nil {
		return fmt.Errorf("patch NodeSet worker annotations: %w", err)
	}
	return nil
}

func (s *WorkerRollingUpdate) waitForJobCompleted(ctx context.Context) error {
	if s.job.IsZero() {
		return fmt.Errorf("submit the job before waiting for its completion")
	}
	if err := s.assertRolloutJobRunning(ctx); err != nil {
		return framework.AnnotateWithJobLog(ctx, s.runtime, s.slurm, s.job, err)
	}
	if _, err := s.runtime.Jail().Run(ctx, "touch -- "+framework.ShellQuote(s.jobReleasePath)); err != nil {
		return fmt.Errorf("release rollout job %s: %w", s.job.ID, err)
	}
	var fatal error
	err := s.waitForRollout(ctx, fmt.Sprintf("job %s completed", s.job.ID), workerRolloutJobFinishWait, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			info, err := s.slurm.JobInfo(waitCtx, s.job.ID)
			if err != nil {
				return false, err
			}
			done, failure := jobOutcome(info)
			if failure != "" {
				fatal = fmt.Errorf("%s", failure)
				return true, nil
			}
			if !done {
				return false, fmt.Errorf("job %s state=%q sacct=%q restarts=%v", s.job.ID, info.QueueState, info.SacctState, info.Restarts)
			}
			return true, nil
		})
	if err == nil {
		err = fatal
	}
	if err != nil {
		return framework.AnnotateWithJobLog(ctx, s.runtime, s.slurm, s.job, err)
	}
	return nil
}

func (s *WorkerRollingUpdate) waitForWorkersReplaced(ctx context.Context) error {
	timeout := workerRolloutReplaceBase + time.Duration(len(s.initialPods))*workerRolloutReplacePerPod
	var fatal error
	var lastProgress string
	err := s.waitForRollout(ctx, fmt.Sprintf("NodeSet %s worker pods replaced", s.nodeSet.Metadata.Name), timeout, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			pods, err := s.nodeSetPods(waitCtx)
			if err != nil {
				return false, err
			}
			now := time.Now()
			for _, pod := range pods {
				if past, terminatingFor := podTerminatingPastDeadline(pod, now); past {
					fatal = fmt.Errorf("worker pod %s on node %s is still terminating %s after its %ds grace period (phase=%s)",
						pod.Name, pod.Spec.NodeName, terminatingFor.Round(time.Second), podGracePeriodSeconds(pod), pod.Status.Phase)
					return true, nil
				}
			}
			remaining := replacedWorkerPods(s.initialPods, pods, s.annotationValue)
			if len(remaining) == 0 {
				return true, nil
			}
			progress := strings.Join(remaining, ", ")
			if progress != lastProgress {
				s.runtime.Logf("worker rolling update: waiting for %s", progress)
				lastProgress = progress
			}
			return false, nil
		})
	if err != nil {
		return err
	}
	return fatal
}

func (s *WorkerRollingUpdate) waitForWorkersIdle(ctx context.Context) error {
	return s.waitForIdle(ctx, fmt.Sprintf("NodeSet %s workers idle after the rollout", s.nodeSet.Metadata.Name))
}

func (s *WorkerRollingUpdate) runSmokeJobs(ctx context.Context) error {
	var problems []string
	for _, worker := range s.workers {
		if err := s.checkRolloutWorkerSet(ctx); err != nil {
			return err
		}
		command := fmt.Sprintf("timeout %.0f srun -w %s hostname", workerRolloutSmokeTimeout.Seconds(), framework.ShellQuote(worker.Name))
		if s.nodeSet.Spec.GPU.Enabled {
			command = fmt.Sprintf("timeout %.0f srun -w %s --gpus-per-node=1 nvidia-smi -L >/dev/null",
				workerRolloutSmokeTimeout.Seconds(), framework.ShellQuote(worker.Name))
		}
		jobCtx, cancel := context.WithTimeout(ctx, workerRolloutSmokeTimeout+time.Minute)
		_, err := s.runtime.Jail().Run(jobCtx, command)
		cancel()
		if err != nil {
			problems = append(problems, fmt.Sprintf("worker %s smoke job failed: %v", worker.Name, err))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return s.checkRolloutWorkerSet(ctx)
}

func (s *WorkerRollingUpdate) waitForIdle(ctx context.Context, description string) error {
	return s.waitForRollout(ctx, description, workerRolloutIdleTimeout, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			nodes := make([]framework.SlurmNodeInfo, 0, len(s.workers))
			for _, worker := range s.workers {
				node, err := s.slurm.NodeInfo(waitCtx, worker.Name)
				if err != nil {
					return false, err
				}
				nodes = append(nodes, node)
			}
			if problems := idleProblems(nodes); len(problems) > 0 {
				return false, fmt.Errorf("%s", strings.Join(problems, "; "))
			}
			return true, nil
		})
}

func (s *WorkerRollingUpdate) nodeSetPods(ctx context.Context) ([]corev1.Pod, error) {
	var pods corev1.PodList
	selector := fmt.Sprintf("%s=%s", nodeSetLabelKey, s.nodeSet.Metadata.Name)
	if err := s.kubectl.GetJSON(ctx, &pods, "get", "pods", "-n", s.nodeSet.Metadata.Namespace,
		"-l", selector, "-o", "json"); err != nil {
		return nil, fmt.Errorf("list worker pods for NodeSet %s: %w", s.nodeSet.Metadata.Name, err)
	}
	return pods.Items, nil
}

func (s *WorkerRollingUpdate) systemDaemonSets(ctx context.Context) ([]appsv1.DaemonSet, error) {
	var daemonSets appsv1.DaemonSetList
	if err := s.kubectl.GetJSON(ctx, &daemonSets, "get", "daemonsets", "-n", framework.SoperatorSystemNamespace, "-o", "json"); err != nil {
		return nil, fmt.Errorf("list DaemonSets in %s: %w", framework.SoperatorSystemNamespace, err)
	}
	return daemonSets.Items, nil
}

// workerRolloutCandidateProblem explains why a NodeSet cannot drive the scenario, or returns "" for a fit.
func workerRolloutCandidateProblem(nodeSet kubeobjects.NodeSet, clusterName string, activeCount int) string {
	if reason := workerRolloutConfigurationProblem(nodeSet, clusterName); reason != "" {
		return reason
	}
	if int(nodeSet.Status.Replicas) != activeCount || nodeSet.Status.Phase != kubeobjects.NodeSetPhaseReady {
		return fmt.Sprintf("phase=%s ready=%d/%d", nodeSet.Status.Phase, nodeSet.Status.Replicas, activeCount)
	}
	return ""
}

func workerRolloutConfigurationProblem(nodeSet kubeobjects.NodeSet, clusterName string) string {
	switch {
	case nodeSet.Spec.ClusterName != "" && nodeSet.Spec.ClusterName != clusterName:
		return "belongs to another cluster"
	case !nodeSet.Spec.GPU.Enabled:
		return "no GPU"
	case nodeSet.Spec.UpdateStrategy != kubeobjects.NodeSetUpdateStrategySlurmAwareRollingUpdate:
		return fmt.Sprintf("updateStrategy=%q", nodeSet.Spec.UpdateStrategy)
	case nodeSet.Spec.Replicas == 0:
		return "no replicas"
	}
	return ""
}

// podGracePeriodSeconds is set by the API server together with deletionTimestamp.
func podGracePeriodSeconds(pod corev1.Pod) int64 {
	if pod.DeletionGracePeriodSeconds != nil {
		return *pod.DeletionGracePeriodSeconds
	}
	return 0
}

// podTerminatingPastDeadline reports whether a pod has been terminating longer than its grace period plus the stuck margin.
func podTerminatingPastDeadline(pod corev1.Pod, now time.Time) (past bool, terminatingFor time.Duration) {
	if pod.DeletionTimestamp == nil {
		return false, 0
	}
	terminatingFor = now.Sub(pod.DeletionTimestamp.Time)
	deadline := time.Duration(podGracePeriodSeconds(pod))*time.Second + workerRolloutStuckMargin
	return terminatingFor > deadline, terminatingFor
}

// replacedWorkerPods lists the ordinals that are not yet replaced by a ready pod carrying the new template.
func replacedWorkerPods(initial map[string]workerPodSnapshot, pods []corev1.Pod, annotationValue string) (remaining []string) {
	byName := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		byName[pod.Name] = pod
	}
	for name := range initial {
		pod, ok := byName[name]
		switch {
		case !ok:
			remaining = append(remaining, name+" (missing)")
		case pod.UID == initial[name].UID:
			remaining = append(remaining, name+" (old pod)")
		case pod.DeletionTimestamp != nil:
			remaining = append(remaining, name+" (terminating)")
		case pod.Annotations[workerRolloutAnnotationKey] != annotationValue:
			remaining = append(remaining, name+" (old template)")
		case !kubeobjects.PodReady(pod):
			remaining = append(remaining, fmt.Sprintf("%s (%s, not ready)", name, pod.Status.Phase))
		}
	}
	sort.Strings(remaining)
	return remaining
}

// daemonSetRolledOut requires a generation bump past previousGeneration so a stale status cannot pass.
func daemonSetRolledOut(daemonSet appsv1.DaemonSet, previousGeneration int64) bool {
	status := daemonSet.Status
	return daemonSet.Generation > previousGeneration &&
		status.ObservedGeneration >= daemonSet.Generation &&
		status.UpdatedNumberScheduled == status.DesiredNumberScheduled &&
		status.NumberAvailable == status.DesiredNumberScheduled &&
		status.NumberUnavailable == 0
}

func daemonSetPodStartedAfter(workerStart time.Time, pods []corev1.Pod) bool {
	for _, pod := range pods {
		if !ownedByDaemonSet(pod) || pod.Status.Phase != corev1.PodRunning || pod.Status.StartTime == nil {
			continue
		}
		if pod.Status.StartTime.Time.After(workerStart) {
			return true
		}
	}
	return false
}

func ownedByDaemonSet(pod corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func idleProblems(nodes []framework.SlurmNodeInfo) []string {
	var problems []string
	for _, node := range nodes {
		if !node.HasStateFlag("IDLE") || !node.IsUsable() || node.Reason != "" ||
			node.HasStateFlag("COMPLETING") || node.HasStateFlag("REBOOT_REQUESTED") || node.HasStateFlag("REBOOT_ISSUED") ||
			node.HasStateFlag("POWER_DOWN") || node.HasStateFlag("POWERING_DOWN") || node.HasStateFlag("POWERED_DOWN") ||
			node.HasStateFlag("POWER_UP") || node.HasStateFlag("POWERING_UP") {
			problems = append(problems, fmt.Sprintf("%s state=%s reason=%q", node.Name, node.State, node.Reason))
		}
	}
	return problems
}

// jobOutcome reports whether the job reached a terminal state; a non-empty failure means it did not complete cleanly.
func jobOutcome(info framework.SlurmJobInfo) (done bool, failure string) {
	if info.Restarts != nil && *info.Restarts > 0 {
		return true, fmt.Sprintf("job %s restarted %d times", info.ID, *info.Restarts)
	}
	if info.IsAlive() || !info.SacctFound {
		return false, ""
	}
	if !info.CompletedSuccessfully() {
		return true, fmt.Sprintf("job %s ended in state %s with exit code %s", info.ID, info.SacctState, info.SacctExit)
	}
	if !info.HasNoRestarts() {
		return false, ""
	}
	return true, ""
}

func rolloutJobGate(releasePath string, timeout time.Duration) string {
	return fmt.Sprintf("timeout %.0fs sh -c %s", timeout.Seconds(), framework.ShellQuote(
		"while [ ! -f "+framework.ShellQuote(releasePath)+" ]; do sleep 1; done"))
}

func workerRolloutBudget(value *intstr.IntOrString, replicas int) (int, error) {
	if replicas < 1 {
		return 0, fmt.Errorf("require positive worker replica count, got %d", replicas)
	}
	if value == nil {
		return 1, nil
	}
	budget, err := intstr.GetScaledValueFromIntOrPercent(value, replicas, false)
	if err != nil {
		return 0, fmt.Errorf("parse maxUnavailable %q: %w", value.String(), err)
	}
	return min(replicas, max(1, budget)), nil
}

type rolloutInvariantError struct{ error }

func rolloutInvariant(format string, args ...any) error {
	return rolloutInvariantError{fmt.Errorf(format, args...)}
}

// WaitFor retries transient errors; an observed safety violation must survive later recovery.
func (s *WorkerRollingUpdate) waitForRollout(ctx context.Context, description string, timeout, interval time.Duration, condition func(context.Context) (bool, error)) error {
	var fatal error
	err := s.runtime.WaitFor(ctx, description, timeout, interval, func(waitCtx context.Context) (bool, error) {
		err := s.observeRollout(waitCtx)
		var done bool
		if err == nil {
			done, err = condition(waitCtx)
		}
		var invariant rolloutInvariantError
		if errors.As(err, &invariant) {
			fatal = err
			return true, nil
		}
		return done, err
	})
	if fatal != nil {
		return fatal
	}
	return err
}

func (s *WorkerRollingUpdate) assertRolloutJobRunning(ctx context.Context) error {
	info, err := s.slurm.JobInfo(ctx, s.job.ID)
	if err != nil {
		return err
	}
	if info.QueueState == "" && (!info.SacctFound || info.IsAlive()) {
		return fmt.Errorf("read current queue state for rollout job %s (accounting=%s)", s.job.ID, info.SacctState)
	}
	if info.QueueState != "RUNNING" || (info.Restarts != nil && *info.Restarts > 0) {
		return rolloutInvariant("rollout job %s must remain RUNNING without restarts: state=%s accounting=%s restarts=%v", s.job.ID, info.QueueState, info.SacctState, info.Restarts)
	}
	if !info.HasNoRestarts() {
		return fmt.Errorf("read restart count for rollout job %s", s.job.ID)
	}
	return nil
}

func (s *WorkerRollingUpdate) waitForWorkerHandoff(ctx context.Context) error {
	worker := s.workers[0]
	initial := s.initialPods[worker.Name]
	err := s.waitForRollout(ctx, "worker handoff waits for the running job", workerRolloutHandoffTimeout, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			if err := s.assertRolloutJobRunning(waitCtx); err != nil {
				return false, err
			}
			pods, err := s.nodeSetPods(waitCtx)
			if err != nil {
				return false, err
			}
			for _, pod := range pods {
				if pod.Name != worker.Name {
					continue
				}
				if pod.UID != initial.UID || pod.Spec.NodeName != initial.NodeName || pod.DeletionTimestamp != nil {
					return false, rolloutInvariant("worker %s moved before job completion: pod=%s host=%s deletion=%v", worker.Name, pod.UID, pod.Spec.NodeName, pod.DeletionTimestamp)
				}
				node, err := s.slurm.NodeInfoOnce(waitCtx, worker.Name)
				if err != nil {
					return false, err
				}
				if pod.Labels["slurm.nebius.ai/worker-operation-phase"] == "stopping" &&
					pod.Labels["slurm.nebius.ai/worker-operation-id"] != "" && node.HasStateFlag("REBOOT_REQUESTED") {
					return true, nil
				}
				return false, fmt.Errorf("worker %s has phase=%s Slurm state=%s reason=%q", worker.Name, pod.Labels["slurm.nebius.ai/worker-operation-phase"], node.State, node.Reason)
			}
			return false, rolloutInvariant("worker %s disappeared while job %s was running", worker.Name, s.job.ID)
		})
	if err != nil {
		return framework.AnnotateWithJobLog(ctx, s.runtime, s.slurm, s.job, err)
	}
	return nil
}
