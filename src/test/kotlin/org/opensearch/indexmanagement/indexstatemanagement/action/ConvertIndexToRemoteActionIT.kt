/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.ResponseException
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import org.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ConvertIndexToRemoteActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test snapshot then convert to remote index`() {
        val indexName = "${testIndexName}_index_snapshot_and_convert"
        val policyID = "${testIndexName}_policy_snapshot_and_convert"
        val repository = "repository"

        createIndex(indexName, null)
        indexDoc(indexName, "1", """{"field": "value1"}""")

        createRepository(repository)

        val snapshotAction = SnapshotAction(
            repository = repository,
            snapshot = indexName,
            index = 0,
        )

        val convertAction = ConvertIndexToRemoteAction(
            repository = repository,
            snapshot = indexName,
            numberOfReplicas = 0,
            index = 0,
        )

        val snapshotState = State(
            name = "snapshotState",
            actions = listOf(snapshotAction),
            transitions = listOf(Transition(stateName = "convertToRemoteState", conditions = null)),
        )

        val convertToRemoteState = State(
            name = "convertToRemoteState",
            actions = listOf(convertAction),
            transitions = listOf(),
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = snapshotState.name,
            states = listOf(snapshotState, convertToRemoteState),
        )

        createPolicy(policy, policyID)
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                "Successfully initialized policy: convertindextoremoteactionit_policy_snapshot_and_convert",
                explainMetaData.info?.get("message"),
            )
        }
        // Change the start time so attempt snapshot step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        // Change the start time so wait for snapshot step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                WaitForSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        // Change the start time so transition will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            // Check if we've transitioned to convertToRemoteState or restore step has started
            val message = explainMetaData.info?.get("message") as? String
            val stateName = explainMetaData.stateMetaData?.name
            assertTrue(
                "Expected transition to convertToRemoteState or restore step, but got message: $message, state: $stateName",
                message == AttemptRestoreStep.getSuccessMessage(indexName) ||
                    stateName == "convertToRemoteState" ||
                    message == "Transitioning to convertToRemoteState [index=convertindextoremoteactionit_index_snapshot_and_convert]",
            )
        }

        // If we're in convertToRemoteState but restore hasn't started yet, trigger another execution
        // Note: The original index may be deleted after restore, so we need to handle that case
        try {
            val explainAfterTransition = getExplainManagedIndexMetaData(indexName)
            if (explainAfterTransition.info?.get("message") != AttemptRestoreStep.getSuccessMessage(indexName)) {
                updateManagedIndexConfigStartTime(managedIndexConfig)
                waitFor {
                    try {
                        val explainMetaData = getExplainManagedIndexMetaData(indexName)
                        assertEquals(
                            AttemptRestoreStep.getSuccessMessage(indexName),
                            explainMetaData.info?.get("message"),
                        )
                    } catch (e: ResponseException) {
                        handleIndexDeletedException(e)
                        // Index was deleted, which is expected - restore succeeded
                        // Just verify remote index exists below
                        return@waitFor
                    }
                }
            }
        } catch (e: ResponseException) {
            handleIndexDeletedException(e)
            // Index was deleted, which is expected - restore succeeded
            // Continue to verify remote index exists
        }

        val remoteIndexName = "${indexName}_remote"
        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)
    }

    private fun handleIndexDeletedException(e: ResponseException) {
        // If we get a 400 "no documents to get", the index was deleted (expected after restore)
        if (e.response.restStatus() == RestStatus.BAD_REQUEST) {
            val errorBody = e.response.asMap()
            val error = errorBody["error"] as? Map<*, *>
            val reason = error?.get("reason") as? String
            if (reason?.contains("no documents to get") != true) {
                throw e // Re-throw if it's a different error
            }
        } else {
            throw e // Re-throw if it's not a 400
        }
    }
}
