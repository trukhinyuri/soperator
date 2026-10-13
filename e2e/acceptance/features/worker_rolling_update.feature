Feature: Worker rolling update
  # Restarts the DaemonSets in soperator-system and leaves a changed worker
  # annotation on the selected NodeSet; each run sets a new value.
  @gpu @soperator_version_>=5.0.0
  Scenario: A worker template change rolls all workers after a Soperator system upgrade
    Given a ready GPU NodeSet with active workers and slurm-aware rolling update is selected
    And the Soperator system components have been upgraded since the workers started
    And a test job is running on one of its workers
    When the worker template of the NodeSet is changed
    Then the worker handoff has started while the test job is running
    And the running job finishes without being killed
    And every active worker pod of the NodeSet is replaced within the rollout budget
    And all active workers of the NodeSet are idle with no leftover drain reason
    And the updated NodeSet accepts a targeted smoke job

  @gpu @soperator_version_>=5.0.0
  Scenario: An MK8s node group update replaces worker hosts without interrupting jobs
    Given a ready GPU NodeSet with active workers and slurm-aware rolling update is selected
    And its workers occupy a dedicated MK8s node group
    And a test job is running on one of its workers
    When the MK8s node group specification is changed to replace its nodes
    Then the running worker is protected while its Kubernetes node drains
    And the running job finishes without being killed
    And the MK8s node group finishes replacing every original node
    And all active workers of the NodeSet are idle with no leftover drain reason
    And the updated NodeSet accepts a targeted smoke job
    # The changed node-group setting is retained, including after failure.
    # Restoring it would trigger another full host replacement.

  @gpu @soperator_version_>=5.0.0
  Scenario: A health failure before a worker update preserves the affected host until recovery
    Given a ready GPU NodeSet with active workers and slurm-aware rolling update is selected
    And the NodeSet supports controlled health failure testing
    And the Soperator system components have been upgraded since the workers started
    And a test job is running on one of its workers
    And a controllable health check is running on the test worker
    When the health check fails before the worker update
    And the worker template of the NodeSet is changed
    Then healthy peers update while the unhealthy worker stays on its original host
    And the running job finishes without being killed
    When the test health failure is cleared
    Then every active worker pod of the NodeSet is replaced within the rollout budget
    And all active workers of the NodeSet are idle with no leftover drain reason
    And the updated NodeSet accepts a targeted smoke job

  @gpu @soperator_version_>=5.0.0
  Scenario: A health failure during a worker update recovers the worker before replacement
    Given a ready GPU NodeSet with active workers and slurm-aware rolling update is selected
    And the NodeSet supports controlled health failure testing
    And the Soperator system components have been upgraded since the workers started
    And a test job is running on one of its workers
    And a controllable health check is running on the test worker
    When the worker template of the NodeSet is changed
    And the health check fails while the worker handoff is waiting for jobs
    Then the running job finishes without being killed
    And the unhealthy worker recovers slurmd in its original pod without losing its drain
    When the test health failure is cleared
    Then every active worker pod of the NodeSet is replaced within the rollout budget
    And all active workers of the NodeSet are idle with no leftover drain reason
    And the updated NodeSet accepts a targeted smoke job

# Operational notes for this feature:
# Static and ephemeral GPU NodeSets are supported; only active workers participate.
# Ephemeral workers are temporarily protected from idle suspension via SuspendExcNodes.
# Cleanup removes only test-owned exclusions, jobs, health checks, and drain reasons.
#
# MK8s requires an authenticated local nebius CLI and a dedicated fixed-size or
# autoscaling group, with one worker per host, zero surge, one unavailable node,
# and a drain timeout of zero or at least ten minutes.
# max_pods toggles 32 <-> 33; other supported values increment, except 256 -> 255.
# The new setting remains after success or failure; later infrastructure reconciliation
# belongs to the caller. There is no restoration rollout during cleanup.
# The 32 -> 33 change doubles the per-node Pod IP block from /26 to /25; ensure capacity.
#
# Health-failure scenarios require two active workers, the gpu-checks ActiveCheck,
# and soperatorchecks --enable-node-replacement=false to observe recovery in place.
