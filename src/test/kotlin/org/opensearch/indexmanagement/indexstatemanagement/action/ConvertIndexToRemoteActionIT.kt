/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
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
import org.opensearch.indexmanagement.makeRequest
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
            deleteOriginalIndex = false,
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
        val remoteIndexName = "${indexName}_remote"
        try {
            val explainAfterTransition = getExplainManagedIndexMetaData(indexName)
            val message = explainAfterTransition.info?.get("message") as? String
            // The message could be either the success message or the "Waiting for remote index" message
            // If it's not the success message, trigger execution and wait for restore to complete
            if (message != AttemptRestoreStep.getSuccessMessage(indexName)) {
                updateManagedIndexConfigStartTime(managedIndexConfig)
                // Wait for either the success message or the waiting message (which means restore was accepted)
                waitFor {
                    try {
                        val explainMetaData = getExplainManagedIndexMetaData(indexName)
                        val currentMessage = explainMetaData.info?.get("message") as? String
                        // Accept either the success message or the waiting message
                        assertTrue(
                            "Expected success or waiting message, but got: $currentMessage",
                            currentMessage == AttemptRestoreStep.getSuccessMessage(indexName) ||
                                currentMessage == "Waiting for remote index [$remoteIndexName] to be created",
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

        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)
    }

    fun `test convert to remote with all options`() {
        val indexName = "${testIndexName}_index_all_options"
        val policyID = "${testIndexName}_policy_all_options"
        val repository = "repository_all_options"
        val aliasName = "${indexName}_alias"

        createIndex(indexName, null)
        indexDoc(indexName, "1", """{"field": "value1"}""")

        // Add an alias to the source index
        client().makeRequest(
            "POST",
            "/_aliases",
            entity = StringEntity(
                """{"actions":[{"add":{"index":"$indexName","alias":"$aliasName"}}]}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        createRepository(repository)

        val snapshotAction = SnapshotAction(
            repository = repository,
            snapshot = indexName,
            index = 0,
        )

        val convertAction = ConvertIndexToRemoteAction(
            repository = repository,
            snapshot = indexName,
            includeAliases = true,
            numberOfReplicas = 1,
            deleteOriginalIndex = true,
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
                "Successfully initialized policy: $policyID",
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                WaitForSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        // Transition to convert state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            val message = explainMetaData.info?.get("message") as? String
            val stateName = explainMetaData.stateMetaData?.name
            assertTrue(
                "Expected transition or restore step, but got: $message, state: $stateName",
                message == AttemptRestoreStep.getSuccessMessage(indexName) ||
                    stateName == "convertToRemoteState" ||
                    message == "Transitioning to convertToRemoteState [index=$indexName]",
            )
        }

        val remoteIndexName = "${indexName}_remote"
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            try {
                val explainMetaData = getExplainManagedIndexMetaData(indexName)
                val currentMessage = explainMetaData.info?.get("message") as? String
                assertTrue(
                    "Expected success or waiting message, but got: $currentMessage",
                    currentMessage == AttemptRestoreStep.getSuccessMessage(indexName) ||
                        currentMessage == "Waiting for remote index [$remoteIndexName] to be created",
                )
            } catch (e: ResponseException) {
                // Index may have been deleted (expected when deleteOriginalIndex is true)
                handleIndexDeletedException(e)
                return@waitFor
            }
        }

        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)

        // Verify alias was restored (include_aliases = true)
        val aliasResponse = client().makeRequest("GET", "/$remoteIndexName/_alias/$aliasName")
        assertEquals("Alias should exist on remote index", RestStatus.OK, aliasResponse.restStatus())

        // Verify number_of_replicas was applied
        val settingsResponse = client().makeRequest("GET", "/$remoteIndexName/_settings")
        val settingsBody = settingsResponse.asMap()

        @Suppress("UNCHECKED_CAST")
        val indexSettings = (settingsBody[remoteIndexName] as Map<String, Any>)["settings"] as Map<String, Any>

        @Suppress("UNCHECKED_CAST")
        val indexBlock = indexSettings["index"] as Map<String, Any>
        assertEquals("number_of_replicas should be 1", "1", indexBlock["number_of_replicas"])

        // Trigger another ISM cycle for deletion of original index
        try {
            updateManagedIndexConfigStartTime(managedIndexConfig)
        } catch (_: Exception) {
            // managedIndexConfig may already be gone if index was deleted
        }
        waitFor { assertIndexDoesNotExist(indexName) }
    }

    private fun handleIndexDeletedException(e: ResponseException) {
        if (e.response.restStatus() == RestStatus.BAD_REQUEST) {
            val errorBody = e.response.asMap()
            val error = errorBody["error"] as? Map<*, *>
            val reason = error?.get("reason") as? String
            if (reason?.contains("no documents to get") != true) {
                throw e
            }
        } else {
            throw e
        }
    }

    fun `test convert to remote with add_original_name_as_alias`() {
        val indexName = "${testIndexName}_index_alias_original"
        val policyID = "${testIndexName}_policy_alias_original"
        val repository = "repository_alias_original"
        val aliasName = "${indexName}_alias"

        createIndex(indexName, null)
        indexDoc(indexName, "1", """{"field": "value1"}""")

        // Add an alias to the source index
        client().makeRequest(
            "POST",
            "/_aliases",
            entity = StringEntity(
                """{"actions":[{"add":{"index":"$indexName","alias":"$aliasName"}}]}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        createRepository(repository)

        val snapshotAction = SnapshotAction(
            repository = repository,
            snapshot = indexName,
            index = 0,
        )

        val convertAction = ConvertIndexToRemoteAction(
            repository = repository,
            snapshot = indexName,
            includeAliases = true,
            numberOfReplicas = 0,
            deleteOriginalIndex = true,
            addOriginalNameAsAlias = true,
            renamePattern = "remote_\$1",
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
                "Successfully initialized policy: $policyID",
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                WaitForSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        // Transition to convert state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            val message = explainMetaData.info?.get("message") as? String
            val stateName = explainMetaData.stateMetaData?.name
            assertTrue(
                "Expected transition or restore step, but got: $message, state: $stateName",
                message == AttemptRestoreStep.getSuccessMessage(indexName) ||
                    stateName == "convertToRemoteState" ||
                    message == "Transitioning to convertToRemoteState [index=$indexName]",
            )
        }

        val remoteIndexName = "remote_$indexName"
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            try {
                val explainMetaData = getExplainManagedIndexMetaData(indexName)
                val currentMessage = explainMetaData.info?.get("message") as? String
                assertTrue(
                    "Expected success or waiting message, but got: $currentMessage",
                    currentMessage == AttemptRestoreStep.getSuccessMessage(indexName) ||
                        currentMessage == "Waiting for remote index [$remoteIndexName] to be created",
                )
            } catch (e: ResponseException) {
                // Index may have been deleted (expected when deleteOriginalIndex is true)
                handleIndexDeletedException(e)
                return@waitFor
            }
        }

        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)

        // May need multiple ISM cycles for the full flow to complete:
        // cycle 1: detect remote index exists, delete original, add alias
        // cycle 2: in case the first cycle only advanced the step state
        try {
            updateManagedIndexConfigStartTime(managedIndexConfig)
        } catch (_: Exception) {
            // managedIndexConfig may already be gone if index was deleted
        }
        try {
            updateManagedIndexConfigStartTime(managedIndexConfig)
        } catch (_: Exception) {
            // managedIndexConfig may already be gone if index was deleted
        }

        // Wait for the original index name to become an alias on the remote index
        // (confirms delete + alias addition completed successfully)
        // Note: we cannot use assertIndexDoesNotExist(indexName) here because the alias
        // with the original index name will resolve as if the index exists
        waitFor {
            val originalNameAliasResponse = client().makeRequest("GET", "/$remoteIndexName/_alias/$indexName")
            assertEquals("Original index name should exist as alias on remote index", RestStatus.OK, originalNameAliasResponse.restStatus())
        }

        // Verify original alias was also restored (include_aliases = true)
        val aliasResponse = client().makeRequest("GET", "/$remoteIndexName/_alias/$aliasName")
        assertEquals("Original alias should exist on remote index", RestStatus.OK, aliasResponse.restStatus())
    }

    fun `test convert to remote with custom rename_pattern`() {
        val indexName = "${testIndexName}_index_custom_rename"
        val policyID = "${testIndexName}_policy_custom_rename"
        val repository = "repository_custom"

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
            includeAliases = false,
            ignoreIndexSettings = "",
            numberOfReplicas = 0,
            deleteOriginalIndex = false,
            renamePattern = "remote_\$1",
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
                "Successfully initialized policy: $policyID",
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                WaitForSnapshotStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                "Transitioning to convertToRemoteState [index=$indexName]",
                explainMetaData.info?.get("message"),
            )
        }
        // With rename_pattern = "remote_$1", the new index should be "remote_<indexName>"
        val remoteIndexName = "remote_$indexName"
        updateManagedIndexConfigStartTime(managedIndexConfig)
        // Wait for either the success message or the waiting message (which means restore was accepted)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            val currentMessage = explainMetaData.info?.get("message") as? String
            // Accept either the success message or the waiting message
            assertTrue(
                "Expected success or waiting message, but got: $currentMessage",
                currentMessage == AttemptRestoreStep.getSuccessMessage(indexName) ||
                    currentMessage == "Waiting for remote index [$remoteIndexName] to be created",
            )
        }
        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)
    }
}
