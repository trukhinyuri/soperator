package values

import (
	"k8s.io/utils/ptr"

	slurmv1 "nebius.ai/slurm-operator/api/v1"
)

func buildJobTmpfsEnabledFrom(config *slurmv1.JobTmpfs) bool {
	if config == nil {
		return true
	}
	return ptr.Deref(config.Enabled, true)
}
