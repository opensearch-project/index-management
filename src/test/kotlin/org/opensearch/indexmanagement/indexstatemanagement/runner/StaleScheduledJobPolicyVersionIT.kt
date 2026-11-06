/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import org.opensearch.common.unit.TimeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import java.time.Instant

/**
 * Runs against a two data node remote-store cluster (gradle task `remoteStoreDataNodesIntegTest`).
 *
 * Replica copies of a remote-store backed index do not execute write operations, so the job scheduler on the node that
 * owns a job through the replica copy of the config index only learns about job document updates at its next full sweep.
 * When such a job applies a policy change it writes the metadata and the job document itself, then keeps running from
 * the stale in-memory job until that sweep: its next runs see metadata stamped with the new policy version and a job
 * with the old one. With the sweeper period stretched beyond the job interval these stale runs are guaranteed; they
 * used to fail and disable the managed index ("version conflict") and now continue with the job document.
 */
class StaleScheduledJobPolicyVersionIT : IndexStateManagementRestTestCase() {
    // The remote store repositories are system repositories and cannot be deleted by the test framework's cleanup.
    override fun preserveReposUponCompletion(): Boolean = true

    fun `test policy change on jobs owned through the replica copy does not fail them`() {
        val prefix = "stale_scheduler_on"
        withStaleScheduler(prefix) { indices, replicaOwned, policyB, convergedAt ->
            // Every job runs at least once more from its stale in-memory copy before the next sweep refreshes it
            val deadline = convergedAt.plusSeconds(OBSERVATION_WINDOW_SECONDS)
            while (Instant.now().isBefore(deadline)) {
                indices.keys.forEach { index ->
                    assertTrue("job of $index was disabled", getExistingManagedIndexConfig(index).enabled)
                    assertEquals("managed index $index was failed", false, getExplainManagedIndexMetaData(index).policyRetryInfo?.failed)
                }
                Thread.sleep(POLL_INTERVAL_MILLIS)
            }
            indices.keys.forEach { index ->
                val metadata = getExplainManagedIndexMetaData(index)
                assertEquals("metadata of $index is not on the new policy", policyB.id, metadata.policyID)
                assertEquals("metadata of $index is not on the new policy version", policyB.seqNo, metadata.policySeqNo)
                assertEquals(false, metadata.policyRetryInfo?.failed)
            }
            logger.info("${replicaOwned.size} jobs owned through the replica copy ran from a stale scheduler copy without being failed")
        }
    }

    fun `test policy change on jobs owned through the replica copy fails them when reconciliation is disabled`() {
        val prefix = "stale_scheduler_off"
        updateClusterSetting(ManagedIndexSettings.POLICY_VERSION_RECONCILIATION_ENABLED.key, "false", escapeValue = false)
        try {
            withStaleScheduler(prefix) { indices, replicaOwned, _, _ ->
                // The pre-reconciliation behaviour: the first stale run fails and disables the managed index
                waitFor(Instant.ofEpochSecond(OBSERVATION_WINDOW_SECONDS)) {
                    val failed =
                        replicaOwned.filter { index ->
                            getExplainManagedIndexMetaData(index).info?.get("message") == VERSION_CONFLICT_MESSAGE &&
                                !getExistingManagedIndexConfig(index).enabled
                        }
                    assertTrue("expected a job owned through the replica copy to be failed by its stale run", failed.isNotEmpty())
                }
                (indices.keys - replicaOwned).forEach { index ->
                    assertTrue("job of $index is owned through the primary copy and must not be affected", getExistingManagedIndexConfig(index).enabled)
                }
            }
        } finally {
            updateClusterSetting(ManagedIndexSettings.POLICY_VERSION_RECONCILIATION_ENABLED.key, "true", escapeValue = false)
        }
    }

    /**
     * Attaches policy A to [INDEX_COUNT] indices, changes them to policy B and stretches the scheduler sweep period so
     * that the jobs owned through the replica copy of the config index keep running from a stale in-memory job after they
     * applied the change. Calls [block] once every job document carries policy B, with the indices (name to uuid), the
     * indices whose job is owned through the replica copy, policy B and the time of convergence.
     */
    private fun withStaleScheduler(
        prefix: String,
        block: (indices: Map<String, String>, replicaOwned: Set<String>, policyB: Policy, convergedAt: Instant) -> Unit,
    ) {
        updateClusterSetting(ManagedIndexSettings.JOB_INTERVAL.key, "1", escapeValue = false)
        updateClusterSetting(SWEEPER_PERIOD_SETTING, SHORT_SWEEPER_PERIOD)
        try {
            val policyA = createPolicy(neverCompletingPolicy("${prefix}_a"), "${prefix}_a")
            val policyB = createPolicy(neverCompletingPolicy("${prefix}_b"), "${prefix}_b")

            val indexNames = (1..INDEX_COUNT).map { "${prefix}_index_$it" }
            indexNames.forEach { createIndex(it, policyA.id) }
            val indices = indexNames.associateWith { getExistingManagedIndexConfig(it).indexUuid }
            waitFor(Instant.ofEpochSecond(INITIALIZATION_TIMEOUT_SECONDS)) {
                indexNames.forEach { index -> assertEquals("index $index was not initialized", policyA.id, getExplainManagedIndexMetaData(index).policyID) }
            }

            val replicaOwned = indicesOwnedThroughReplica(indices)
            logger.info("Jobs owned through the replica copy of the config index: $replicaOwned")
            assumeTrue("expected at least one job to be owned through the replica copy", replicaOwned.isNotEmpty())

            // Both policies have the same states and actions, so the change is safe and applied on the next run
            val response =
                client().makeRequest(
                    "POST", "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/${prefix}_index_*", emptyMap(),
                    ChangePolicy(policyB.id, null, emptyList(), false).toHttpEntity(),
                )
            assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
            assertEquals("not every index was updated", indices.size, response.asMap()["updated_indices"])

            // Let one short sweep deliver the pending change to every in-memory job, then stop the sweeps from refreshing
            // the in-memory jobs for longer than the job interval
            Thread.sleep(SWEEP_DELIVERY_MILLIS)
            updateClusterSetting(SWEEPER_PERIOD_SETTING, LONG_SWEEPER_PERIOD)

            waitFor(Instant.ofEpochSecond(CONVERGENCE_TIMEOUT_SECONDS)) {
                indexNames.forEach { index ->
                    val config = getExistingManagedIndexConfig(index)
                    assertEquals("job document of $index is not on the new policy yet", policyB.id, config.policyID)
                    assertNull("pending change of $index was not cleared yet", config.changePolicy)
                }
            }
            block(indices, replicaOwned, policyB, Instant.now())
        } finally {
            updateClusterSetting(SWEEPER_PERIOD_SETTING, SHORT_SWEEPER_PERIOD)
        }
    }

    /** A policy whose only transition requires an index age of 30 days, so it never completes during the test. */
    private fun neverCompletingPolicy(policyID: String): Policy =
        randomPolicy(
            id = policyID,
            states =
            listOf(
                randomState(name = "hot", transitions = listOf(Transition("archive", Conditions(indexAge = TimeValue.timeValueDays(30))))),
                randomState(name = "archive"),
            ),
        )

    /** Names of the indices whose job is scheduled on a node other than the one holding the primary copy of the config index. */
    @Suppress("UNCHECKED_CAST")
    private fun indicesOwnedThroughReplica(indices: Map<String, String>): Set<String> {
        val shards = client().makeRequest("GET", "_cat/shards/$INDEX_MANAGEMENT_INDEX", mapOf("format" to "json", "h" to "prirep,state,id"))
        val primaryHolder =
            (entityAsList(shards) as List<Map<String, Any>>)
                .first { it["prirep"] == "p" && it["state"] == "STARTED" }["id"] as String

        val jobs = client().makeRequest("GET", "/_plugins/_job_scheduler/api/jobs", mapOf("by_node" to "true")).asMap()
        val jobOwner = mutableMapOf<String, String>()
        (jobs["nodes"] as List<Map<String, Any>>).forEach { node ->
            val nodeId = node["node_id"] as String
            val scheduledJobInfo = node["scheduled_job_info"] as Map<String, Any>
            (scheduledJobInfo["jobs"] as? List<Map<String, Any>>)?.forEach { job ->
                if (job["index_name"] == INDEX_MANAGEMENT_INDEX) jobOwner[job["job_id"] as String] = nodeId
            }
        }
        indices.forEach { (index, uuid) -> assertNotNull("job of $index is not scheduled on any node", jobOwner[uuid]) }
        return indices.filter { (_, uuid) -> jobOwner[uuid] != primaryHolder }.keys
    }

    companion object {
        private const val INDEX_COUNT = 12
        private const val SWEEPER_PERIOD_SETTING = "plugins.jobscheduler.sweeper.period"
        private const val SHORT_SWEEPER_PERIOD = "10s"
        private const val LONG_SWEEPER_PERIOD = "3m"
        private const val SWEEP_DELIVERY_MILLIS = 20_000L
        private const val INITIALIZATION_TIMEOUT_SECONDS = 180L
        private const val CONVERGENCE_TIMEOUT_SECONDS = 150L
        private const val OBSERVATION_WINDOW_SECONDS = 150L
        private const val POLL_INTERVAL_MILLIS = 10_000L
        private const val VERSION_CONFLICT_MESSAGE = "There is a version conflict between your previous execution and your managed index"
    }
}
