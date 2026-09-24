/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.client.ResponseException
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_HISTORY_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Conditions
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.waitFor
import java.time.Instant

/**
 * Runs against a two data node remote-store cluster (gradle task `remoteStoreDataNodesIntegTest`).
 *
 * On such a cluster the config index has its primary copy on one node and its replica copy on the other, and each job is
 * owned by one of the two through consistent hashing. Replica copies of a remote-store backed index do not execute write
 * operations, so the node that owns a job through the replica copy never observes the delete issued by the remove policy
 * API: the job scheduler keeps running the removed job from memory ("orphaned" job). These tests verify that the runner
 * refuses to re-initialize a managed index whose job document is gone, and that the check can be switched off.
 */
class OrphanedManagedIndexJobIT : IndexStateManagementRestTestCase() {
    // The remote store repositories are system repositories and cannot be deleted by the test framework's cleanup.
    override fun preserveReposUponCompletion(): Boolean = true

    fun `test orphaned jobs do not re-initialize a removed policy`() {
        val indices = createInitializedManagedIndices("orphan_guard_on")
        val removedAt = removePolicies("orphan_guard_on", indices)
        val orphans = scheduledJobIds()?.intersect(indices.values.toSet())
        logger.info("Jobs still scheduled after remove (orphans): $orphans")

        // Every orphaned job fires at least twice within the observation window (job interval is one minute, jitter off).
        val deadline = Instant.now().plusSeconds(OBSERVATION_WINDOW_SECONDS)
        while (Instant.now().isBefore(deadline)) {
            assertEquals("metadata was recreated for a removed index", emptySet<String>(), existingMetadataDocumentIds(indices.values))
            assertEquals("a removed policy was re-initialized", 0L, historyEntriesAfter(indices.values, removedAt))
            Thread.sleep(POLL_INTERVAL_MILLIS)
        }
        if (orphans != null) {
            // The scheduler side is untouched by the runner: orphans are only made harmless, not removed.
            assertEquals("orphaned jobs should still be scheduled", orphans, scheduledJobIds()?.intersect(indices.values.toSet()))
        }
    }

    fun `test orphaned jobs re-initialize a removed policy when the job document check is disabled`() {
        updateClusterSetting(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED.key, "false", escapeValue = false)
        try {
            val indices = createInitializedManagedIndices("orphan_guard_off")
            val removedAt = removePolicies("orphan_guard_off", indices)
            val orphans = scheduledJobIds()?.intersect(indices.values.toSet())
            logger.info("Jobs still scheduled after remove (orphans): $orphans")
            if (orphans != null) {
                // With a job scheduler that deschedules deleted jobs itself there is nothing left to reproduce.
                assumeTrue("the job scheduler descheduled every removed job, nothing to reproduce", orphans.isNotEmpty())
            }

            waitFor(Instant.ofEpochSecond(OBSERVATION_WINDOW_SECONDS)) {
                val recreated = existingMetadataDocumentIds(indices.values)
                val reinitialized = historyEntriesAfter(indices.values, removedAt)
                assertTrue(
                    "expected at least one orphaned job to re-initialize its removed policy (recreated metadata: $recreated, " +
                        "history entries after remove: $reinitialized)",
                    recreated.isNotEmpty() || reinitialized > 0L,
                )
            }
        } finally {
            updateClusterSetting(ManagedIndexSettings.JOB_DOCUMENT_CHECK_ENABLED.key, "true", escapeValue = false)
        }
    }

    /**
     * Creates [INDEX_COUNT] indices managed by a policy that never completes (a single state whose only transition
     * requires an index age of 30 days) and waits until every one of them is initialized.
     *
     * The start time of the jobs is deliberately not manipulated: on a remote-store cluster the node owning a job
     * through the replica copy does not observe such an update either. With a one minute job interval and no jitter every
     * job initializes on its own within about a minute.
     *
     * @return index name to index uuid
     */
    private fun createInitializedManagedIndices(prefix: String): Map<String, String> {
        updateClusterSetting(ManagedIndexSettings.JOB_INTERVAL.key, "1", escapeValue = false)
        val policyID = "${prefix}_policy"
        val policy =
            randomPolicy(
                id = policyID,
                states =
                listOf(
                    randomState(
                        name = "hot",
                        transitions = listOf(Transition("archive", Conditions(indexAge = TimeValue.timeValueDays(30)))),
                    ),
                    randomState(name = "archive"),
                ),
            )
        createPolicy(policy, policyID)

        val indexNames = (1..INDEX_COUNT).map { "${prefix}_index_$it" }
        indexNames.forEach { createIndex(it, policyID) }
        val indices = indexNames.associateWith { getExistingManagedIndexConfig(it).indexUuid }

        waitFor(Instant.ofEpochSecond(INITIALIZATION_TIMEOUT_SECONDS)) {
            indexNames.forEach { index ->
                val metadata = getExplainManagedIndexMetaData(index)
                assertEquals("index $index was not initialized", policyID, metadata.policyID)
                assertEquals("index $index is not in the initial state", "hot", metadata.stateMetaData?.name)
            }
        }
        return indices
    }

    /** Removes the policy from every index and waits until the job and metadata documents are gone. */
    private fun removePolicies(prefix: String, indices: Map<String, String>): Long {
        val response = client().makeRequest("POST", "/_plugins/_ism/remove/${prefix}_index_*")
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())
        assertEquals("not every index had its policy removed", indices.size, response.asMap()["updated_indices"])
        waitFor {
            indices.keys.forEach { assertNull("job document of $it still exists", getManagedIndexConfig(it)) }
            assertEquals("metadata documents still exist", emptySet<String>(), existingMetadataDocumentIds(indices.values))
        }
        return Instant.now().toEpochMilli()
    }

    /** Ids of the metadata documents that currently exist for the given index uuids. */
    private fun existingMetadataDocumentIds(indexUuids: Collection<String>): Set<String> {
        val ids = indexUuids.joinToString("\",\"", "\"", "\"") { managedIndexMetadataID(it) }
        val request = """{ "size": ${indexUuids.size}, "query": { "ids": { "values": [$ids] } } }"""
        val response =
            adminClient().makeRequest(
                "POST", "$INDEX_MANAGEMENT_INDEX/_search", emptyMap(),
                StringEntity(request, ContentType.APPLICATION_JSON),
            )
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        return searchHits(response.asMap()).map { it["_id"] as String }.toSet()
    }

    /** Number of history entries written for the given index uuids after [epochMillis]. */
    @Suppress("UNCHECKED_CAST")
    private fun historyEntriesAfter(indexUuids: Collection<String>, epochMillis: Long): Long {
        val uuids = indexUuids.joinToString("\",\"", "\"", "\"")
        val request =
            """
            {
                "size": 0,
                "track_total_hits": true,
                "query": {
                    "bool": {
                        "filter": [
                            { "terms": { "$INDEX_STATE_MANAGEMENT_HISTORY_TYPE.${ManagedIndexMetaData.INDEX_UUID}": [$uuids] } },
                            { "range": { "$INDEX_STATE_MANAGEMENT_HISTORY_TYPE.history_timestamp": { "gt": $epochMillis } } }
                        ]
                    }
                }
            }
            """.trimIndent()
        val response =
            adminClient().makeRequest(
                "POST", "${IndexManagementIndices.HISTORY_ALL}/_search", emptyMap(),
                StringEntity(request, ContentType.APPLICATION_JSON),
            )
        assertEquals("Request failed", RestStatus.OK, response.restStatus())
        val hits = response.asMap()["hits"] as Map<String, Any>
        val total = hits["total"] as Map<String, Any>
        return (total["value"] as Number).toLong()
    }

    /**
     * Ids of the jobs currently scheduled on any node according to the job scheduler, or null when the running
     * job scheduler does not expose that API.
     */
    private fun scheduledJobIds(): Set<String>? {
        val response =
            try {
                client().makeRequest("GET", "/_plugins/_job_scheduler/api/jobs", mapOf("by_node" to "true"))
            } catch (e: ResponseException) {
                logger.info("Job scheduler jobs API not available: ${e.response.statusLine}")
                return null
            }
        val jobIds = mutableSetOf<String>()
        collectJobIds(response.asMap(), jobIds)
        return jobIds
    }

    private fun collectJobIds(node: Any?, into: MutableSet<String>) {
        when (node) {
            is Map<*, *> -> {
                (node["job_id"] as? String)?.let { into.add(it) }
                node.values.forEach { collectJobIds(it, into) }
            }

            is List<*> -> node.forEach { collectJobIds(it, into) }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun searchHits(searchResponse: Map<String, Any>): List<Map<String, Any>> {
        val hits = searchResponse["hits"] as Map<String, Any>
        return hits["hits"] as List<Map<String, Any>>
    }

    companion object {
        private const val INDEX_COUNT = 16
        private const val INITIALIZATION_TIMEOUT_SECONDS = 180L
        private const val OBSERVATION_WINDOW_SECONDS = 150L
        private const val POLL_INTERVAL_MILLIS = 10_000L
    }
}
