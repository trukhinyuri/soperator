package values

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	slurmv1 "nebius.ai/slurm-operator/api/v1"
)

func TestBuildJobTmpfsEnabledFrom(t *testing.T) {
	assert.True(t, buildJobTmpfsEnabledFrom(nil))
	assert.True(t, buildJobTmpfsEnabledFrom(&slurmv1.JobTmpfs{}))
	assert.True(t, buildJobTmpfsEnabledFrom(&slurmv1.JobTmpfs{Enabled: ptr.To(true)}))
	assert.False(t, buildJobTmpfsEnabledFrom(&slurmv1.JobTmpfs{Enabled: ptr.To(false)}))
}
