package sharedsteps

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nebius/soperator/e2e/acceptance/framework"
)

func TestWorkerSuspendExclusionValue(t *testing.T) {
	for _, tt := range []struct{ input, value, wantErr string }{
		{"SuspendExcNodes = worker-[0,3-5]\nSuspendExcParts = other", "worker-[0,3-5]", ""},
		{"  SuspendExcNodes = (null)\n", "", ""},
		{"SuspendExcNodes =\n", "", ""},
		{"SuspendExcParts = other\n", "", "missing SuspendExcNodes"},
		{"SuspendExcNodes = a\nSuspendExcNodes = b", "", "multiple SuspendExcNodes"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			value, err := workerSuspendExclusionValue(tt.input)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.value, value)
			}
		})
	}
}

func TestSplitWorkerSuspendExclusions(t *testing.T) {
	terms, err := splitWorkerSuspendExclusions("worker-[0,3-5],other-[0-9]:2,last-0")
	require.NoError(t, err)
	assert.Equal(t, []string{"worker-[0,3-5]", "other-[0-9]:2", "last-0"}, terms)
	terms, err = splitWorkerSuspendExclusions("")
	require.NoError(t, err)
	assert.Empty(t, terms)
	for _, bad := range []string{"worker-[0", "worker-0]", "worker-[[0]]", "worker-0,", ",worker-0", "worker-0,,other"} {
		_, err := splitWorkerSuspendExclusions(bad)
		require.Error(t, err, bad)
	}
}

func TestEphemeralWorkerHoldPreservesPreexistingAndConcurrentExclusions(t *testing.T) {
	runtime := newRolloutPowerRuntime(t,
		powerConfigReply("worker-[0,2],other-0"),
		powerHostsReply("worker-[0,2],other-0", "worker-0\nworker-2\nother-0"),
		powerUpdateReply("+=", "worker-5"),
		powerConfigReply("worker-[0,2,5],other-0"),
		powerHostsReply("worker-[0,2,5],other-0", "worker-0\nworker-2\nworker-5\nother-0"),
		// An unrelated actor adds another exclusion before our cleanup.
		powerConfigReply("worker-[0,2,5],other-[0-1]"),
		powerHostsReply("worker-[0,2,5],other-[0-1]", "worker-0\nworker-2\nworker-5\nother-0\nother-1"),
		powerUpdateReply("-=", "worker-5"),
		powerConfigReply("worker-[0,2],other-[0-1]"),
		powerHostsReply("worker-[0,2],other-[0-1]", "worker-0\nworker-2\nother-0\nother-1"),
	)
	s := WorkerRollingUpdate{runtime: runtime}
	require.NoError(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-5", "worker-0", "worker-5"}))
	assert.Equal(t, []string{"worker-0", "worker-5"}, s.power.names)
	assert.Equal(t, []string{"worker-5"}, s.power.added)
	s.cleanupEphemeralWorkerHold(context.Background())
	assert.Equal(t, workerRolloutPower{}, s.power)
	assert.Empty(t, runtime.logs)
	s.cleanupEphemeralWorkerHold(context.Background())
}

func TestEphemeralWorkerHoldCleansUncertainPartialMutation(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%t", applied), func(t *testing.T) {
			write := powerUpdateReply("+=", "worker-3,worker-7")
			write.err = errors.New("response lost")
			replies := []rolloutPowerReply{powerConfigReply("(null)"), write}
			if applied {
				// Only one addition is observed after an uncertain failure.
				replies = append(replies, powerConfigReply("worker-3"), powerHostsReply("worker-3", "worker-3"),
					powerUpdateReply("-=", "worker-3"), powerConfigReply("(null)"))
			} else {
				replies = append(replies, powerConfigReply("(null)"))
			}
			runtime := newRolloutPowerRuntime(t, replies...)
			s := WorkerRollingUpdate{runtime: runtime}
			require.ErrorContains(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-3", "worker-7"}), "response lost")
			assert.Equal(t, []string{"worker-3", "worker-7"}, s.power.added)
			s.cleanupEphemeralWorkerHold(context.Background())
			assert.Empty(t, runtime.logs)
			assert.Equal(t, workerRolloutPower{}, s.power)
		})
	}
}

func TestEphemeralWorkerHoldCountedRangesNeverProveProtection(t *testing.T) {
	runtime := newRolloutPowerRuntime(t, powerConfigReply("worker-[0-9]:2"))
	s := WorkerRollingUpdate{runtime: runtime}
	require.ErrorContains(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-3"}), "explicitly exclude these workers")
	assert.Empty(t, s.power.names)

	runtime = newRolloutPowerRuntime(t,
		powerConfigReply("worker-3,other-[0-9]:2"), powerHostsReply("worker-3", "worker-3"),
		powerConfigReply("worker-3,other-[0-9]:2"), powerHostsReply("worker-3", "worker-3"),
	)
	s = WorkerRollingUpdate{runtime: runtime}
	require.NoError(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-3"}))
	assert.Empty(t, s.power.added)
	s.cleanupEphemeralWorkerHold(context.Background())
	assert.Empty(t, runtime.logs)
}

func TestEphemeralWorkerHoldLostProtectionFailsInvariant(t *testing.T) {
	runtime := newRolloutPowerRuntime(t, powerConfigReply("worker-3"), powerHostsReply("worker-3", "worker-3"))
	s := WorkerRollingUpdate{runtime: runtime, power: workerRolloutPower{names: []string{"worker-3", "worker-7"}}}
	err := s.checkEphemeralWorkerHold(context.Background())
	require.ErrorContains(t, err, "worker-7 lost automatic suspension protection")
	var invariant rolloutInvariantError
	assert.ErrorAs(t, err, &invariant)
}

func TestEphemeralWorkerHoldRejectsUnknownConfigAndInvalidNames(t *testing.T) {
	runtime := newRolloutPowerRuntime(t, rolloutPowerReply{command: "scontrol show config", output: "SuspendExcParts = none"})
	s := WorkerRollingUpdate{runtime: runtime}
	require.ErrorContains(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-3"}), "missing SuspendExcNodes")
	assert.Empty(t, s.power.names)
	for _, names := range [][]string{nil, {"worker-[0-3]"}, {"worker-3;echo bad"}, {"worker-0,worker-3"}, {""}} {
		require.Error(t, s.holdEphemeralWorkers(context.Background(), names))
	}
}

func TestEphemeralWorkerHoldRejectsInvalidExpansion(t *testing.T) {
	for _, output := range []string{"", "worker-0 extra", "worker-[0-2]"} {
		runtime := newRolloutPowerRuntime(t, powerConfigReply("worker-[0-2]"), powerHostsReply("worker-[0-2]", output))
		s := WorkerRollingUpdate{runtime: runtime}
		require.ErrorContains(t, s.holdEphemeralWorkers(context.Background(), []string{"worker-0"}), "invalid node name")
		assert.Empty(t, s.power.names)
	}
}

func TestEphemeralWorkerHoldCleanupRefusesNewCountedConfiguration(t *testing.T) {
	runtime := newRolloutPowerRuntime(t,
		powerConfigReply("worker-3,other-[0-9]:2"), powerHostsReply("worker-3", "worker-3"))
	s := WorkerRollingUpdate{runtime: runtime, power: workerRolloutPower{names: []string{"worker-3"}, added: []string{"worker-3"}}}
	s.cleanupEphemeralWorkerHold(context.Background())
	require.Len(t, runtime.logs, 1)
	assert.Contains(t, runtime.logs[0], "refusing an incremental removal")
	assert.Contains(t, runtime.logs[0], "worker-3")
	assert.Equal(t, workerRolloutPower{}, s.power)
}

func TestEphemeralWorkerHoldCleanupChecksRemovalAndResets(t *testing.T) {
	runtime := newRolloutPowerRuntime(t,
		powerConfigReply("worker-3"), powerHostsReply("worker-3", "worker-3"),
		powerUpdateReply("-=", "worker-3"),
		powerConfigReply("worker-3"), powerHostsReply("worker-3", "worker-3"))
	s := WorkerRollingUpdate{runtime: runtime, power: workerRolloutPower{names: []string{"worker-3"}, added: []string{"worker-3"}}}
	s.cleanupEphemeralWorkerHold(context.Background())
	require.Len(t, runtime.logs, 1)
	assert.Contains(t, runtime.logs[0], "remains after removal")
	assert.Equal(t, workerRolloutPower{}, s.power)
}

type rolloutPowerReply struct {
	command string
	output  string
	err     error
}

type rolloutPowerRuntime struct {
	framework.Runtime
	t       *testing.T
	replies []rolloutPowerReply
	logs    []string
}

func newRolloutPowerRuntime(t *testing.T, replies ...rolloutPowerReply) *rolloutPowerRuntime {
	t.Helper()
	r := &rolloutPowerRuntime{t: t, replies: replies}
	t.Cleanup(func() { assert.Empty(t, r.replies, "unconsumed Slurm commands") })
	return r
}

func (r *rolloutPowerRuntime) Controller() framework.CommandScope { return rolloutPowerScope{r: r} }
func (r *rolloutPowerRuntime) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

type rolloutPowerScope struct {
	framework.CommandScope
	r *rolloutPowerRuntime
}

func (s rolloutPowerScope) Run(_ context.Context, command string) (string, error) {
	s.r.t.Helper()
	require.NotEmpty(s.r.t, s.r.replies, "unexpected Slurm command: %s", command)
	reply := s.r.replies[0]
	s.r.replies = s.r.replies[1:]
	require.Equal(s.r.t, reply.command, command)
	return reply.output, reply.err
}
func (s rolloutPowerScope) RunWithDefaultRetry(ctx context.Context, command string) (string, error) {
	return s.Run(ctx, command)
}

func powerConfigReply(value string) rolloutPowerReply {
	return rolloutPowerReply{command: "scontrol show config", output: "SuspendExcNodes = " + value}
}
func powerHostsReply(expression, names string) rolloutPowerReply {
	return rolloutPowerReply{command: "scontrol show hostnames " + framework.ShellQuote(expression), output: names}
}
func powerUpdateReply(operator, names string) rolloutPowerReply {
	return rolloutPowerReply{command: workerSuspendUpdateCommand(operator, strings.Split(names, ","))}
}
