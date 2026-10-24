/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

/**
 * Takes a snapshot of ISM's config index and the indices it manages, deletes them, and restores them: first the data
 * indices, then, in a separate request, the config index. To ISM this is a restore onto a new cluster, because every
 * restored index gets a new UUID.
 */
class ManagedIndexUuidRemapperIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test restored indices carry on from the position in the snapshot`() {
        val policyID = "${testIndexName}_carry_on"
        val templateIndex = "${testIndexName}_template_index"
        val apiIndex = "${testIndexName}_api_index"
        val template = ISMTemplate(listOf("${testIndexName}_template_*"), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))
        createPolicy(warmPolicy(policyID, template), policyID)
        // One index attached to the policy by its ISM template, one with the add policy API.
        createIndex(templateIndex, policyID = null)
        createIndex(apiIndex, policyID)
        val indices = listOf(templateIndex, apiIndex)
        indices.forEach { moveToWarm(it, policyID) }
        val uuidsInSnapshot = indices.associateWith { getUuid(it) }
        val metadataInSnapshot = indices.associateWith { getExplainManagedIndexMetaData(it) }
        val repository = "${testIndexName}_carry_on_repo"
        takeSnapshot(repository, indices + INDEX_MANAGEMENT_INDEX)

        deleteIndices(listOf(INDEX_MANAGEMENT_INDEX) + indices)
        restore(repository, indices)
        restore(repository, listOf(INDEX_MANAGEMENT_INDEX))

        indices.forEach { index ->
            val uuid = getUuid(index)
            assertNotEquals("the restore did not give $index a new uuid", uuidsInSnapshot[index], uuid)
            // onRestore runs after the restore request returns, and explain finds jobs with a search.
            val metadata = waitFor { getExplainManagedIndexMetaData(index).also { assertEquals(uuid, it.indexUuid) } }
            assertEquals(metadataInSnapshot.getValue(index).stateMetaData, metadata.stateMetaData)
            assertEquals(metadataInSnapshot.getValue(index).actionMetaData, metadata.actionMetaData)
            val job = getManagedIndexConfigByDocId(uuid)!!
            assertEquals(policyID, job.policyID)

            // One run completes the policy only if the job carries on in "warm". A job that started over would only be
            // initialized in "hot" by that run.
            updateManagedIndexConfigStartTime(job)
            waitFor { assertEquals(true, getExplainManagedIndexMetaData(index).policyCompleted) }
        }
    }

    private fun warmPolicy(id: String, template: ISMTemplate) = Policy(
        id = id,
        description = "$testIndexName description",
        schemaVersion = 1L,
        lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
        errorNotification = null,
        defaultState = "hot",
        states =
        listOf(
            State("hot", listOf(), listOf(Transition("warm", conditions = null))),
            State("warm", listOf(IndexPriorityAction(WARM_INDEX_PRIORITY, 0)), listOf()),
        ),
        ismTemplate = listOf(template),
    )

    /** Runs the index's job until the index is in state "warm" with its index priority set. */
    private fun moveToWarm(index: String, policyID: String) {
        val job = initialize(index, policyID)
        updateManagedIndexConfigStartTime(job)
        waitFor { assertEquals("warm", getExplainManagedIndexMetaData(index).transitionTo) }
        updateManagedIndexConfigStartTime(job)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(index)
            assertEquals("warm", metadata.stateMetaData?.name)
            assertEquals(Step.StepStatus.COMPLETED, metadata.stepMetaData?.stepStatus)
        }
        assertEquals(WARM_INDEX_PRIORITY, getIndexPrioritySetting(index))
    }

    /** Runs the index's job once, which saves its progress, and returns the job. */
    private fun initialize(index: String, policyID: String): ManagedIndexConfig {
        val job = getExistingManagedIndexConfig(index)
        updateManagedIndexConfigStartTime(job)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index).policyID) }
        return job
    }

    // The snapshot, delete and restore requests include the config index, a system index, so they use the admin client.
    private fun takeSnapshot(repository: String, indices: List<String>) {
        createRepository(repository)
        val response = adminClient().makeRequest("PUT", "_snapshot/$repository/$SNAPSHOT", mapOf("wait_for_completion" to "true"), indicesBody(indices))
        assertEquals("Snapshot failed", RestStatus.OK, response.restStatus())
        assertSnapshotFinishedWithSuccess(repository, SNAPSHOT)
    }

    @Suppress("UNCHECKED_CAST")
    private fun restore(repository: String, indices: List<String>) {
        val response =
            adminClient().makeRequest("POST", "_snapshot/$repository/$SNAPSHOT/_restore", mapOf("wait_for_completion" to "true"), indicesBody(indices))
        assertEquals("Restore failed", RestStatus.OK, response.restStatus())
        val shards = (response.asMap()["snapshot"] as Map<String, Any>)["shards"] as Map<String, Any>
        assertEquals("Restore failed for some shards of $indices", 0, shards["failed"])
    }

    private fun indicesBody(indices: List<String>) =
        StringEntity("""{"indices": "${indices.joinToString(",")}", "include_global_state": false}""", ContentType.APPLICATION_JSON)

    private fun deleteIndices(indices: List<String>) = indices.forEach { adminClient().makeRequest("DELETE", it) }

    companion object {
        private const val SNAPSHOT = "snapshot"
        private const val WARM_INDEX_PRIORITY = 42
    }
}
