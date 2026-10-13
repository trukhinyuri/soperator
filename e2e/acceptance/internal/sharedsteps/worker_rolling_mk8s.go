package sharedsteps

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

const (
	workerMK8sGroupLabel = "nebius.com/node-group-id"
	workerMK8sTimeout    = 45 * time.Minute
)

type workerRolloutMK8s struct {
	groupID           string
	originalMaxPods   *mk8sInteger
	changedMaxPods    mk8sInteger
	specFingerprint   string
	originalNodes     map[string]types.UID
	originalInstances map[string]string
	operationID       string
}

// The CLI can emit protobuf int64 fields as either numbers or strings.
type mk8sInteger int64

func (n *mk8sInteger) UnmarshalJSON(data []byte) error {
	value := strings.Trim(string(data), "\"")
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid MK8s integer: %w", err)
	}
	*n = mk8sInteger(parsed)
	return nil
}

type workerMK8sGroup struct {
	Metadata struct {
		ID              string      `json:"id"`
		ResourceVersion mk8sInteger `json:"resource_version"`
	} `json:"metadata"`
	Spec struct {
		FixedNodeCount *mk8sInteger           `json:"fixed_node_count"`
		Autoscaling    *workerMK8sAutoscaling `json:"autoscaling"`
		Template       struct {
			MaxPods *mk8sInteger `json:"max_pods"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		State       string      `json:"state"`
		Reconciling bool        `json:"reconciling"`
		Target      mk8sInteger `json:"target_node_count"`
		Count       mk8sInteger `json:"node_count"`
		Ready       mk8sInteger `json:"ready_node_count"`
		Outdated    mk8sInteger `json:"outdated_node_count"`
		Strategy    *struct {
			DrainTimeout   string           `json:"drain_timeout"`
			MaxSurge       *workerMK8sLimit `json:"max_surge"`
			MaxUnavailable *workerMK8sLimit `json:"max_unavailable"`
		} `json:"strategy"`
	} `json:"status"`
	fingerprint string
}

type workerMK8sAutoscaling struct {
	MinNodeCount mk8sInteger `json:"min_node_count"`
	MaxNodeCount mk8sInteger `json:"max_node_count"`
}

type workerMK8sLimit struct {
	Count   *mk8sInteger `json:"count"`
	Percent *mk8sInteger `json:"percent"`
}

type workerMK8sOperation struct {
	ID     string `json:"id"`
	Status *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"status"`
}

type workerMK8sTopologyError struct{ error }

func (s *WorkerRollingUpdate) registerMK8sSteps(sc *godog.ScenarioContext) {
	sc.Step(`^its workers occupy a dedicated MK8s node group$`, s.selectMK8sGroup)
	sc.Step(`^the MK8s node group specification is changed to replace its nodes$`, s.changeMK8sGroup)
	sc.Step(`^the running worker is protected while its Kubernetes node drains$`, s.waitForMK8sHandoff)
	sc.Step(`^the MK8s node group finishes replacing every original node$`, s.waitForMK8sReplacement)
}

func decodeWorkerMK8sGroup(output string) (workerMK8sGroup, error) {
	var group workerMK8sGroup
	if err := json.Unmarshal([]byte(output), &group); err != nil {
		return group, fmt.Errorf("decode MK8s node group: %w", err)
	}
	// Hash configuration without the one field owned by this scenario. Never
	// include the spec in diagnostics: it can contain cloud-init credentials.
	var raw struct {
		Spec map[string]any `json:"spec"`
	}
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return group, err
	}
	template, ok := raw.Spec["template"].(map[string]any)
	if !ok {
		return group, fmt.Errorf("MK8s node group lacks a template object")
	}
	delete(template, "max_pods")
	data, err := json.Marshal(raw.Spec)
	if err != nil {
		return group, err
	}
	group.fingerprint = fmt.Sprintf("%x", sha256.Sum256(data))
	if group.Metadata.ID == "" || group.Metadata.ResourceVersion <= 0 {
		return group, fmt.Errorf("MK8s node group lacks ID or positive resource_version")
	}
	return group, nil
}

func (s *WorkerRollingUpdate) mk8sGroup(ctx context.Context) (workerMK8sGroup, error) {
	output, err := s.runtime.Local().Run(ctx, "nebius", "mk8s", "node-group", "get",
		"--id", s.mk8s.groupID, "--format", "json")
	if err != nil {
		return workerMK8sGroup{}, fmt.Errorf("read MK8s group %s: %w", s.mk8s.groupID, err)
	}
	group, err := decodeWorkerMK8sGroup(output)
	if err != nil {
		return group, err
	}
	if group.Metadata.ID != s.mk8s.groupID {
		return group, fmt.Errorf("MK8s returned group %s, expected %s", group.Metadata.ID, s.mk8s.groupID)
	}
	return group, nil
}

func (s *WorkerRollingUpdate) selectMK8sGroup(ctx context.Context) error {
	var nodes corev1.NodeList
	if err := s.kubectl.GetJSON(ctx, &nodes, "get", "nodes", "-o", "json"); err != nil {
		return err
	}
	groupID, hosts, err := workerMK8sHosts(s.initialPods, nodes.Items)
	if err != nil {
		var topology workerMK8sTopologyError
		if errors.As(err, &topology) {
			s.runtime.Logf("acceptance: %v, skipping", err)
			return godog.ErrSkip
		}
		return err
	}
	s.mk8s.groupID = groupID
	group, err := s.mk8sGroup(ctx)
	if err != nil {
		return err
	}
	if err := workerMK8sGroupReady(group, len(s.initialPods)); err != nil {
		return err
	}
	if group.Status.Count != mk8sInteger(len(hosts)) {
		return fmt.Errorf("match MK8s group %s node snapshot: API reports %d nodes, Kubernetes has %d", groupID, group.Status.Count, len(hosts))
	}
	if group.Status.Strategy == nil || group.Status.Strategy.MaxSurge == nil || group.Status.Strategy.MaxUnavailable == nil {
		return fmt.Errorf("MK8s group %s does not expose effective rollout strategy", groupID)
	}
	surge, err := workerMK8sLimitCount(*group.Status.Strategy.MaxSurge, len(hosts), true)
	if err != nil {
		return err
	}
	unavailable, err := workerMK8sLimitCount(*group.Status.Strategy.MaxUnavailable, len(hosts), false)
	if err != nil {
		return err
	}
	if surge != 0 || unavailable != 1 {
		s.runtime.Logf("acceptance: MK8s group requires zero surge and one unavailable node; got surge=%d unavailable=%d, skipping", surge, unavailable)
		return godog.ErrSkip
	}
	drain, err := time.ParseDuration(group.Status.Strategy.DrainTimeout)
	if err != nil {
		return fmt.Errorf("parse effective MK8s drain timeout: %w", err)
	}
	if drain < 0 || drain > 0 && drain < 10*time.Minute {
		return fmt.Errorf("MK8s group %s drain timeout %s is insufficient for protected worker handoff", groupID, drain)
	}
	changed, err := nextWorkerMK8sMaxPods(group.Spec.Template.MaxPods)
	if err != nil {
		s.runtime.Logf("acceptance: MK8s group has unsupported Pod capacity: %v, skipping", err)
		return godog.ErrSkip
	}
	var pods corev1.PodList
	if err := s.kubectl.GetJSON(ctx, &pods, "get", "pods", "--all-namespaces", "-o", "json"); err != nil {
		return err
	}
	if err := workerMK8sDedicatedPods(hosts, s.initialPods, pods.Items, int(changed)); err != nil {
		var topology workerMK8sTopologyError
		if errors.As(err, &topology) {
			s.runtime.Logf("acceptance: %v, skipping", err)
			return godog.ErrSkip
		}
		return err
	}
	instances := make(map[string]string, len(s.workers))
	for _, worker := range s.workers {
		if worker.SlurmNode.InstanceID == "" {
			return fmt.Errorf("worker %s lacks Compute instance ID", worker.Name)
		}
		instances[worker.Name] = worker.SlurmNode.InstanceID
	}
	s.mk8s.originalMaxPods = group.Spec.Template.MaxPods
	s.mk8s.changedMaxPods = changed
	s.mk8s.specFingerprint = group.fingerprint
	s.mk8s.originalNodes = hosts
	s.mk8s.originalInstances = instances
	return nil
}

// Include idle hosts: an ephemeral NodeSet may populate only part of its
// group, but the MK8s update must replace every original host.
func workerMK8sHosts(workers map[string]workerPodSnapshot, nodes []corev1.Node) (string, map[string]types.UID, error) {
	byName := make(map[string]corev1.Node, len(nodes))
	for _, node := range nodes {
		byName[node.Name] = node
	}
	hosts := make(map[string]types.UID, len(nodes))
	groupID := ""
	for name, pod := range workers {
		node, ok := byName[pod.NodeName]
		if !ok {
			return "", nil, fmt.Errorf("worker %s host %s is absent", name, pod.NodeName)
		}
		id := node.Labels[workerMK8sGroupLabel]
		if id == "" || (groupID != "" && id != groupID) {
			return "", nil, workerMK8sTopologyError{fmt.Errorf("workers do not share one MK8s group")}
		}
		if _, exists := hosts[node.Name]; exists {
			return "", nil, fmt.Errorf("multiple workers occupy Kubernetes node %s", node.Name)
		}
		groupID = id
		hosts[node.Name] = node.UID
	}
	if groupID == "" {
		return "", nil, fmt.Errorf("selected NodeSet has no worker hosts")
	}
	for _, node := range nodes {
		if node.Labels[workerMK8sGroupLabel] != groupID {
			continue
		}
		if node.UID == "" || node.Spec.Unschedulable || !workerMK8sNodeReady(node) {
			return "", nil, fmt.Errorf("MK8s host %s is not ready and schedulable", node.Name)
		}
		hosts[node.Name] = node.UID
	}
	return groupID, hosts, nil
}

func workerMK8sNodeReady(node corev1.Node) bool {
	if node.DeletionTimestamp != nil {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func workerMK8sDedicatedPods(hosts map[string]types.UID, workers map[string]workerPodSnapshot, pods []corev1.Pod, maxPods int) error {
	counts := make(map[string]int)
	for _, pod := range pods {
		if _, ok := hosts[pod.Spec.NodeName]; !ok || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		counts[pod.Spec.NodeName]++
		if original, ok := workers[pod.Name]; ok && pod.Namespace == framework.SoperatorNamespace && pod.UID == original.UID {
			continue
		}
		if pod.Annotations["kubernetes.io/config.mirror"] != "" {
			continue
		}
		daemon := false
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "DaemonSet" && owner.Controller != nil && *owner.Controller {
				daemon = true
			}
		}
		if !daemon {
			return workerMK8sTopologyError{fmt.Errorf("MK8s group is not dedicated: unrelated pod %s/%s on %s", pod.Namespace, pod.Name, pod.Spec.NodeName)}
		}
	}
	for host, count := range counts {
		if count > maxPods {
			return fmt.Errorf("MK8s host %s has %d Pods, exceeding proposed capacity %d", host, count, maxPods)
		}
	}
	return nil
}

func workerMK8sLimitCount(limit workerMK8sLimit, size int, roundUp bool) (int, error) {
	if limit.Count != nil && limit.Percent == nil && *limit.Count >= 0 {
		return int(*limit.Count), nil
	}
	if limit.Percent != nil && limit.Count == nil && *limit.Percent >= 0 && *limit.Percent <= 100 {
		count := int(*limit.Percent) * size
		if roundUp {
			count += 99
		}
		return count / 100, nil
	}
	return 0, fmt.Errorf("invalid MK8s rollout count or percentage")
}

func nextWorkerMK8sMaxPods(original *mk8sInteger) (mk8sInteger, error) {
	current := mk8sInteger(110)
	if original != nil && *original != 0 {
		current = *original
	}
	if current < 8 || current > 256 {
		return 0, fmt.Errorf("max_pods=%d is outside 8..256", current)
	}
	if current == 33 {
		return 32, nil
	}
	if current == 256 {
		return current - 1, nil
	}
	return current + 1, nil
}

func workerMK8sGroupReady(group workerMK8sGroup, minimumWorkers int) error {
	if (group.Spec.FixedNodeCount == nil) == (group.Spec.Autoscaling == nil) {
		return fmt.Errorf("validate MK8s group %s sizing: require exactly one of fixed_node_count or autoscaling", group.Metadata.ID)
	}
	var minimum, maximum mk8sInteger
	if group.Spec.FixedNodeCount != nil {
		minimum, maximum = *group.Spec.FixedNodeCount, *group.Spec.FixedNodeCount
	} else {
		minimum, maximum = group.Spec.Autoscaling.MinNodeCount, group.Spec.Autoscaling.MaxNodeCount
	}
	if minimum < 0 || maximum < minimum {
		return fmt.Errorf("validate MK8s group %s sizing: invalid node count bounds %d..%d", group.Metadata.ID, minimum, maximum)
	}
	// Autoscaling can remove idle hosts or add capacity during the update. Its
	// current target must converge, while every selected worker still needs a host.
	if minimumWorkers < 1 || group.Status.Target < mk8sInteger(minimumWorkers) ||
		group.Status.Target < minimum || group.Status.Target > maximum ||
		!strings.EqualFold(group.Status.State, "RUNNING") || group.Status.Reconciling || group.Status.Outdated != 0 ||
		group.Status.Count != group.Status.Target || group.Status.Ready != group.Status.Target {
		return fmt.Errorf("MK8s group %s not converged: state=%s reconciling=%t target=%d nodes=%d ready=%d outdated=%d bounds=%d..%d minimum_workers=%d",
			group.Metadata.ID, group.Status.State, group.Status.Reconciling, group.Status.Target, group.Status.Count, group.Status.Ready, group.Status.Outdated, minimum, maximum, minimumWorkers)
	}
	return nil
}

func workerMK8sUpdateArgs(groupID string, version, maxPods mk8sInteger) []string {
	return []string{"nebius", "mk8s", "node-group", "update", "--id", groupID,
		"--patch", "--resource-version", strconv.FormatInt(int64(version), 10), "--async", "--format", "json",
		"--template-max-pods", strconv.FormatInt(int64(maxPods), 10)}
}

func workerMK8sOperationID(output string) (string, error) {
	var id string
	if json.Unmarshal([]byte(output), &id) == nil && workerMK8sIdentifier(id) {
		return id, nil
	}
	var operation workerMK8sOperation
	if json.Unmarshal([]byte(output), &operation) == nil && workerMK8sIdentifier(operation.ID) {
		return operation.ID, nil
	}
	// Older CLI versions print the async ID as plain text even with --format.
	id = strings.TrimSpace(output)
	if workerMK8sIdentifier(id) {
		return id, nil
	}
	return "", fmt.Errorf("MK8s async update did not return an operation ID")
}

func workerMK8sIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, c := range value {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func sameWorkerMK8sMaxPods(a, b *mk8sInteger) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func (s *WorkerRollingUpdate) changeMK8sGroup(ctx context.Context) error {
	if err := s.checkRolloutWorkerSet(ctx); err != nil {
		return err
	}
	group, err := s.mk8sGroup(ctx)
	if err != nil {
		return err
	}
	if group.fingerprint != s.mk8s.specFingerprint || !sameWorkerMK8sMaxPods(group.Spec.Template.MaxPods, s.mk8s.originalMaxPods) {
		return fmt.Errorf("MK8s group %s configuration changed after selection", s.mk8s.groupID)
	}
	if err := workerMK8sGroupReady(group, len(s.initialPods)); err != nil {
		return err
	}
	// Log before the request: a timeout can still leave an accepted update running.
	s.runtime.Logf("Update MK8s group %s to max_pods=%d; the setting is retained without rollback, including after failure",
		s.mk8s.groupID, s.mk8s.changedMaxPods)
	output, err := s.runtime.Local().Run(ctx, workerMK8sUpdateArgs(s.mk8s.groupID, group.Metadata.ResourceVersion, s.mk8s.changedMaxPods)...)
	if err != nil {
		return fmt.Errorf("start MK8s group %s rollout to max_pods=%d (no automatic rollback): %w", s.mk8s.groupID, s.mk8s.changedMaxPods, err)
	}
	s.mk8s.operationID, err = workerMK8sOperationID(output)
	if err != nil {
		return fmt.Errorf("read MK8s group %s rollout operation (max_pods=%d may be retained): %w", s.mk8s.groupID, s.mk8s.changedMaxPods, err)
	}
	return nil
}

func (s *WorkerRollingUpdate) waitForMK8sHandoff(ctx context.Context) error {
	original := s.initialPods[s.workers[0].Name]
	err := s.waitForRollout(ctx, "running worker host cordoned by MK8s", workerMK8sTimeout, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			if err := s.mk8sOperationFailure(waitCtx); err != nil {
				return false, err
			}
			if err := s.assertRolloutJobRunning(waitCtx); err != nil {
				return false, err
			}
			var nodes corev1.NodeList
			if err := s.kubectl.GetJSON(waitCtx, &nodes, "get", "nodes", "-o", "json"); err != nil {
				return false, err
			}
			for _, node := range nodes.Items {
				if node.Name == original.NodeName && node.UID == s.mk8s.originalNodes[node.Name] {
					return node.Spec.Unschedulable, nil
				}
			}
			return false, rolloutInvariant("MK8s removed original host %s while job %s was running", original.NodeName, s.job.ID)
		})
	if err != nil {
		return err
	}
	return s.waitForWorkerHandoff(ctx)
}

func (s *WorkerRollingUpdate) mk8sOperationFailure(ctx context.Context) error {
	_, err := s.mk8sOperationCompleted(ctx)
	return err
}

func (s *WorkerRollingUpdate) mk8sOperationCompleted(ctx context.Context) (bool, error) {
	if s.mk8s.operationID == "" {
		return true, nil
	}
	output, err := s.runtime.Local().Run(ctx, "nebius", "mk8s", "node-group", "operation", "get", "--id", s.mk8s.operationID, "--format", "json")
	if err != nil {
		return false, err
	}
	return workerMK8sOperationCompleted(s.mk8s.operationID, output)
}

func workerMK8sOperationCompleted(operationID, output string) (bool, error) {
	var operation workerMK8sOperation
	if err := json.Unmarshal([]byte(output), &operation); err != nil {
		return false, fmt.Errorf("decode MK8s operation: %w", err)
	}
	if operation.ID != operationID {
		return false, rolloutInvariant("MK8s returned operation %s, expected %s", operation.ID, operationID)
	}
	if operation.Status != nil && operation.Status.Code != 0 {
		// Do not log the operation request/progress, which can contain user data.
		return false, rolloutInvariant("MK8s operation %s failed with status code %d", operationID, operation.Status.Code)
	}
	return operation.Status != nil, nil
}

func (s *WorkerRollingUpdate) waitForMK8sReplacement(ctx context.Context) error {
	return s.waitForRollout(ctx, "MK8s original nodes and workers replaced", workerMK8sTimeout, framework.DefaultPollInterval,
		func(waitCtx context.Context) (bool, error) {
			completed, err := s.mk8sOperationCompleted(waitCtx)
			if err != nil {
				return false, err
			}
			if !completed {
				return false, fmt.Errorf("MK8s operation %s is still in progress", s.mk8s.operationID)
			}
			group, err := s.mk8sGroup(waitCtx)
			if err != nil {
				return false, err
			}
			if group.fingerprint != s.mk8s.specFingerprint || !sameWorkerMK8sMaxPods(group.Spec.Template.MaxPods, &s.mk8s.changedMaxPods) {
				return false, rolloutInvariant("MK8s group %s configuration changed during rollout", s.mk8s.groupID)
			}
			if err := workerMK8sGroupReady(group, len(s.initialPods)); err != nil {
				return false, err
			}
			var nodes corev1.NodeList
			if err := s.kubectl.GetJSON(waitCtx, &nodes, "get", "nodes", "-o", "json"); err != nil {
				return false, err
			}
			for _, node := range nodes.Items {
				if uid, ok := s.mk8s.originalNodes[node.Name]; ok && node.UID == uid {
					return false, fmt.Errorf("original MK8s host %s still exists", node.Name)
				}
			}
			pods, err := s.nodeSetPods(waitCtx)
			if err != nil {
				return false, err
			}
			if err := workerMK8sReplacedPods(s.initialPods, pods); err != nil {
				return false, err
			}
			for worker, instance := range s.mk8s.originalInstances {
				node, err := s.slurm.NodeInfoOnce(waitCtx, worker)
				if err != nil {
					return false, err
				}
				if node.InstanceID == "" || node.InstanceID == instance || !node.IsUsable() {
					return false, fmt.Errorf("worker %s has not registered on a usable new instance", worker)
				}
				removed, err := framework.InstanceRemoved(waitCtx, s.runtime, instance)
				if err != nil {
					return false, fmt.Errorf("check original instance %s: %w", instance, err)
				}
				if !removed {
					return false, fmt.Errorf("original instance %s still exists", instance)
				}
			}
			return true, nil
		})
}

func workerMK8sReplacedPods(original map[string]workerPodSnapshot, pods []corev1.Pod) error {
	if len(pods) != len(original) {
		return fmt.Errorf("MK8s rollout has %d workers, expected %d", len(pods), len(original))
	}
	for _, pod := range pods {
		old, ok := original[pod.Name]
		if !ok || pod.UID == old.UID || pod.Spec.NodeName == "" || pod.Spec.NodeName == old.NodeName || pod.DeletionTimestamp != nil || !kubeobjects.PodReady(pod) {
			return fmt.Errorf("worker %s has not been replaced and become ready on a new MK8s host", pod.Name)
		}
	}
	return nil
}
