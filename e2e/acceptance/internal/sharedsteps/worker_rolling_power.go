package sharedsteps

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/nebius/soperator/e2e/acceptance/framework"
)

// Exclusions protect the selected active workers without running extra jobs that
// would block their handoff. Only additions owned by this scenario are removed.
type workerRolloutPower struct {
	names []string
	added []string
}

type workerSuspendExclusions struct {
	names   map[string]bool
	counted bool
}

func (s *WorkerRollingUpdate) holdEphemeralWorkers(ctx context.Context, names []string) error {
	if len(s.power.names) != 0 {
		return fmt.Errorf("establish ephemeral worker suspension protection: already established")
	}
	names = slices.Clone(names)
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) == 0 {
		return fmt.Errorf("protect ephemeral workers: empty worker set")
	}
	for _, name := range names {
		if !validRolloutSlurmName(name) {
			return fmt.Errorf("invalid ephemeral worker name %q", name)
		}
	}
	current, err := s.readWorkerSuspendExclusions(ctx)
	if err != nil {
		return err
	}
	var added []string
	for _, name := range names {
		if !current.names[name] {
			added = append(added, name)
		}
	}
	if len(added) != 0 && current.counted {
		return fmt.Errorf("protect ephemeral workers %s: SuspendExcNodes uses counted exclusions (:count), which do not support incremental updates; explicitly exclude these workers before running the scenario", strings.Join(added, ","))
	}
	// Record ownership before the write: a failed response may still have applied it.
	s.power = workerRolloutPower{names: names, added: added}
	if len(added) != 0 {
		if _, err := s.runtime.Controller().Run(ctx, workerSuspendUpdateCommand("+=", added)); err != nil {
			return fmt.Errorf("exclude ephemeral workers %s from automatic suspension: %w", strings.Join(added, ","), err)
		}
	}
	return s.checkEphemeralWorkerHold(ctx)
}

func (s *WorkerRollingUpdate) checkEphemeralWorkerHold(ctx context.Context) error {
	if len(s.power.names) == 0 {
		return nil
	}
	current, err := s.readWorkerSuspendExclusions(ctx)
	if err != nil {
		return err
	}
	var missing []string
	for _, name := range s.power.names {
		if !current.names[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		return rolloutInvariant("ephemeral workers %s lost automatic suspension protection in SuspendExcNodes", strings.Join(missing, ","))
	}
	return nil
}

func (s *WorkerRollingUpdate) cleanupEphemeralWorkerHold(ctx context.Context) {
	defer func() { s.power = workerRolloutPower{} }()
	if len(s.power.added) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, workerRolloutCleanupTimeout)
	defer cancel()
	if err := s.releaseEphemeralWorkers(cleanupCtx); err != nil {
		s.runtime.Logf("Cleanup: restore automatic suspension for ephemeral workers %s: %v; check SuspendExcNodes and remove only these scenario additions", strings.Join(s.power.added, ","), err)
	}
}

func (s *WorkerRollingUpdate) releaseEphemeralWorkers(ctx context.Context) error {
	current, err := s.readWorkerSuspendExclusions(ctx)
	if err != nil {
		return err
	}
	var remove []string
	for _, name := range s.power.added {
		if current.names[name] {
			remove = append(remove, name)
		}
	}
	if len(remove) == 0 {
		return nil
	}
	if current.counted {
		return fmt.Errorf("SuspendExcNodes now uses counted exclusions (:count); refusing an incremental removal that Slurm does not support")
	}
	if _, err := s.runtime.Controller().Run(ctx, workerSuspendUpdateCommand("-=", remove)); err != nil {
		return fmt.Errorf("remove temporary SuspendExcNodes entries: %w", err)
	}
	after, err := s.readWorkerSuspendExclusions(ctx)
	if err != nil {
		return err
	}
	for _, name := range remove {
		if after.names[name] {
			return fmt.Errorf("temporary SuspendExcNodes entry %s remains after removal", name)
		}
	}
	return nil
}

func workerSuspendUpdateCommand(operator string, names []string) string {
	return "scontrol update " + framework.ShellQuote("SuspendExcNodes"+operator+strings.Join(names, ","))
}

func (s *WorkerRollingUpdate) readWorkerSuspendExclusions(ctx context.Context) (workerSuspendExclusions, error) {
	result := workerSuspendExclusions{names: make(map[string]bool)}
	output, err := s.runtime.Controller().RunWithDefaultRetry(ctx, "scontrol show config")
	if err != nil {
		return result, fmt.Errorf("read Slurm automatic suspension exclusions: %w", err)
	}
	expression, err := workerSuspendExclusionValue(output)
	if err != nil {
		return result, rolloutInvariant("read Slurm automatic suspension exclusions: %v", err)
	}
	terms, err := splitWorkerSuspendExclusions(expression)
	if err != nil {
		return result, rolloutInvariant("parse SuspendExcNodes: %v", err)
	}
	var unconditional []string
	for _, term := range terms {
		if strings.Contains(term, ":") {
			result.counted = true
			continue
		}
		unconditional = append(unconditional, term)
	}
	if len(unconditional) == 0 {
		return result, nil
	}
	output, err = s.runtime.Controller().RunWithDefaultRetry(ctx,
		"scontrol show hostnames "+framework.ShellQuote(strings.Join(unconditional, ",")))
	if err != nil {
		return result, fmt.Errorf("expand SuspendExcNodes hostlist: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		name := strings.TrimSpace(line)
		if !validRolloutSlurmName(name) {
			return result, rolloutInvariant("expand SuspendExcNodes returned invalid node name %q", name)
		}
		result.names[name] = true
	}
	return result, nil
}

func workerSuspendExclusionValue(output string) (string, error) {
	value, found := "", false
	for _, line := range strings.Split(output, "\n") {
		key, candidate, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(key) != "SuspendExcNodes" {
			continue
		}
		if found {
			return "", fmt.Errorf("Slurm config contains multiple SuspendExcNodes fields")
		}
		value, found = strings.TrimSpace(candidate), true
	}
	if !found {
		return "", fmt.Errorf("Slurm config is missing SuspendExcNodes")
	}
	if value == "(null)" {
		value = ""
	}
	return value, nil
}

// A hostlist may contain commas inside brackets. Counted ranges are kept intact
// so they are never mistaken for unconditional protection of individual nodes.
func splitWorkerSuspendExclusions(expression string) ([]string, error) {
	if expression == "" {
		return nil, nil
	}
	var terms []string
	depth, start := 0, 0
	for i, r := range expression {
		switch r {
		case '[':
			if depth != 0 {
				return nil, fmt.Errorf("nested brackets in exclusion hostlist")
			}
			depth++
		case ']':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unmatched bracket in exclusion hostlist")
			}
		case ',':
			if depth == 0 {
				terms = append(terms, expression[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unclosed bracket in exclusion hostlist")
	}
	terms = append(terms, expression[start:])
	for i, term := range terms {
		terms[i] = strings.TrimSpace(term)
		if terms[i] == "" {
			return nil, fmt.Errorf("empty entry in exclusion hostlist")
		}
	}
	return terms, nil
}

func validRolloutSlurmName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
