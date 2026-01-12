/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
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

class DeleteSearchableSnapshotActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    @Suppress("UNCHECKED_CAST")
    fun `test delete action with delete_snapshot deletes searchable snapshot index and its source snapshot`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_policy"
        val repository = "repository"

        createIndex(indexName, null)
        indexDoc(indexName, "1", """{"field": "value1"}""")

        createRepository(repository)

        // State 1: Create snapshot
        val snapshotAction = SnapshotAction(repository = repository, snapshot = indexName, index = 0)
        val snapshotState = State(
            name = "snapshotState",
            actions = listOf(snapshotAction),
            transitions = listOf(Transition(stateName = "convertToRemoteState", conditions = null)),
        )

        // State 2: Convert to searchable snapshot (remote_snapshot)
        val convertAction = ConvertIndexToRemoteAction(repository = repository, snapshot = indexName, index = 0)
        val convertToRemoteState = State(
            name = "convertToRemoteState",
            actions = listOf(convertAction),
            transitions = listOf(Transition(stateName = "deleteState", conditions = null)),
        )

        // State 3: Delete with delete_snapshot=true
        val deleteAction = DeleteAction(index = 0, deleteSnapshot = true)
        val deleteState = State(
            name = "deleteState",
            actions = listOf(deleteAction),
            transitions = listOf(),
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = snapshotState.name,
            states = listOf(snapshotState, convertToRemoteState, deleteState),
        )

        createPolicy(policy, policyID)
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Initialize policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                "Successfully initialized policy: $policyID",
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // Execute snapshot action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptSnapshotStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // Wait for snapshot completion
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForSnapshotStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // Transition to convert state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val message = getExplainManagedIndexMetaData(indexName).info?.get("message") as String?
            assertTrue("Expected transition message", message?.contains("Transitioning to convertToRemoteState") == true)
        }

        // Execute convert action (restore as searchable snapshot)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptRestoreStep.getSuccessMessage(indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        val remoteIndexName = "${indexName}_remote"
        waitFor { assertIndexExists(remoteIndexName) }

        // Verify it's a searchable snapshot
        assertTrue("Index $remoteIndexName is not a remote index", isIndexRemote(remoteIndexName))

        // Verify snapshot exists before delete
        assertSnapshotExists(repository, indexName)

        // Create a delete-only policy for the remote index
        val deletePolicyID = "${testIndexName}_delete_policy"
        val deleteOnlyPolicy = Policy(
            id = deletePolicyID,
            description = "Delete searchable snapshot",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = "deleteState",
            states = listOf(deleteState),
        )
        createPolicy(deleteOnlyPolicy, deletePolicyID)
        addPolicyToIndex(remoteIndexName, deletePolicyID)

        val deleteIndexConfig = getExistingManagedIndexConfig(remoteIndexName)

        // Initialize delete policy
        updateManagedIndexConfigStartTime(deleteIndexConfig)
        waitFor {
            assertEquals(
                "Successfully initialized policy: $deletePolicyID",
                getExplainManagedIndexMetaData(remoteIndexName).info?.get("message"),
            )
        }

        // Execute delete action
        updateManagedIndexConfigStartTime(deleteIndexConfig)
        waitFor {
            assertIndexDoesNotExist(remoteIndexName)
        }

        // Verify snapshot was also deleted
        waitFor {
            val snapshots = getSnapshotList(repository)
            assertFalse("Snapshot should have been deleted", snapshots.any { it.startsWith(indexName) })
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getSnapshotList(repository: String): List<String> {
        val response = client().makeRequest("GET", "_cat/snapshots/$repository?format=json", emptyMap<String, String>())
        val snapshots =
            jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, response.entity.content)
                .use { parser -> parser.list() } as List<Map<String, Any>>
        return snapshots.map { it["id"] as String }
    }
}
