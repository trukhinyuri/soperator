package sharedsteps

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nebius/soperator/e2e/acceptance/framework"
	"github.com/nebius/soperator/e2e/acceptance/internal/kubeobjects"
)

// activeCheckJobs holds the Kubernetes submission and Slurm-run discovery shared
// by active-check validation and rolling updates with controlled check failures.
type activeCheckJobs struct {
	clusterName string
	runtime     framework.Runtime
	kubectl     *framework.KubectlClient
}

func activeCheckResourceName(cluster, suffix string) string {
	return fmt.Sprintf("%s-%s", cluster, suffix)
}

func (s *activeCheckJobs) createJobFromCronJob(ctx context.Context, cronJobName string) (string, error) {
	jobName := uniqueK8sJobName(strings.TrimPrefix(cronJobName, s.clusterName+"-"))
	if err := s.createNamedJobFromCronJob(ctx, cronJobName, jobName); err != nil {
		return "", err
	}
	return jobName, nil
}

func (s *activeCheckJobs) createNamedJobFromCronJob(ctx context.Context, cronJobName, jobName string) error {
	if _, err := s.runtime.Kubectl().RunWithDefaultRetry(ctx,
		"create", "job", "-n", framework.SoperatorNamespace,
		"--from=cronjob/"+cronJobName,
		jobName,
	); err != nil {
		return fmt.Errorf("trigger CronJob %s as Job %s: %w", cronJobName, jobName, err)
	}
	s.runtime.Logf("active checks: triggered CronJob %s as Job %s", cronJobName, jobName)
	return nil
}

func (s *activeCheckJobs) waitForK8sJobSlurmJobIDs(ctx context.Context, jobName string, timeout time.Duration) ([]string, error) {
	var latest k8sJobStatus
	err := s.runtime.WaitFor(ctx, fmt.Sprintf("Kubernetes Job %s Slurm job IDs", jobName), timeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		var job k8sJobStatus
		if err := s.kubectl.GetJSON(waitCtx, &job, "get", "job", "-n", framework.SoperatorNamespace, jobName, "-o", "json"); err != nil {
			return false, err
		}
		latest = job
		if job.Status.Failed > 0 {
			return false, fmt.Errorf("Kubernetes Job %s failed before Slurm job IDs were recorded: %+v", jobName, job.Status)
		}
		return len(slurmJobIDsFromAnnotation(job.Metadata.Annotations)) > 0, nil
	})
	if err != nil {
		return nil, err
	}
	return slurmJobIDsFromAnnotation(latest.Metadata.Annotations), nil
}

func (s *activeCheckJobs) waitForK8sJobCompleteSuccessfully(ctx context.Context, jobName string, timeout time.Duration) error {
	return s.runtime.WaitFor(ctx, fmt.Sprintf("Kubernetes Job %s complete", jobName), timeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		var job k8sJobStatus
		if err := s.kubectl.GetJSON(waitCtx, &job, "get", "job", "-n", framework.SoperatorNamespace, jobName, "-o", "json"); err != nil {
			return false, err
		}
		if job.Status.Succeeded > 0 {
			return true, nil
		}
		if job.Status.Failed > 0 {
			return false, fmt.Errorf("Kubernetes Job %s failed: %+v", jobName, job.Status)
		}
		return false, nil
	})
}

func (s *activeCheckJobs) waitForActiveCheckSlurmRunComplete(ctx context.Context, checkName, firstJobID string, timeout time.Duration) error {
	return s.runtime.WaitFor(ctx, fmt.Sprintf("ActiveCheck %s Slurm run %s complete", checkName, firstJobID), timeout, framework.DefaultPollInterval, func(waitCtx context.Context) (bool, error) {
		status, matches, err := s.slurmRun(waitCtx, checkName, firstJobID)
		if err != nil {
			return false, err
		}
		if !matches {
			return false, nil
		}
		switch status.LastRunStatus {
		case kubeobjects.ActiveCheckSlurmRunStatusComplete:
			return true, nil
		case kubeobjects.ActiveCheckSlurmRunStatusFailed,
			kubeobjects.ActiveCheckSlurmRunStatusCancelled,
			kubeobjects.ActiveCheckSlurmRunStatusError,
			kubeobjects.ActiveCheckSlurmRunStatusSkipped:
			return false, fmt.Errorf("ActiveCheck %s finished with status=%s fail_jobs=%+v error_jobs=%+v cancelled_jobs=%v",
				checkName, status.LastRunStatus, status.LastRunFailJobsAndReasons, status.LastRunErrorJobsAndReasons, status.LastRunCancelledJobs)
		default:
			return false, nil
		}
	})
}

func (s *activeCheckJobs) slurmRun(ctx context.Context, checkName, firstJobID string) (kubeobjects.ActiveCheckSlurmJobsStatus, bool, error) {
	var check kubeobjects.ActiveCheck
	if err := s.kubectl.GetJSON(ctx, &check, "get", "activecheck", "-n", framework.SoperatorNamespace, checkName, "-o", "json"); err != nil {
		return kubeobjects.ActiveCheckSlurmJobsStatus{}, false, err
	}
	status := check.Status.SlurmJobsStatus
	return status, status.LastRunID == firstJobID, nil
}

func uniqueK8sJobName(prefix string) string {
	prefix = strings.Trim(strings.ToLower(prefix), "-")
	if prefix == "" {
		prefix = "job"
	}
	name := fmt.Sprintf("acceptance-%s-%d", prefix, time.Now().Unix())
	if len(name) <= 63 {
		return name
	}
	return name[:63]
}

func slurmJobIDsFromAnnotation(annotations map[string]string) []string {
	raw := strings.TrimSpace(annotations["slurm-job-id"])
	if raw == "" {
		return nil
	}
	var ids []string
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

type k8sJobStatus struct {
	Metadata struct {
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Status struct {
		Succeeded int32 `json:"succeeded"`
		Failed    int32 `json:"failed"`
	} `json:"status"`
}
