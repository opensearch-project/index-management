/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.WaitForRestoreStep
import org.opensearch.indexmanagement.waitFor
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ConvertIndexToRemoteActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic conversion to remote index`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val repository = "repository"

        // Step 1: Create an index and index some data
        createIndex(indexName, null)
        indexDoc(indexName, "1", """{"field": "value1"}""")

        // Step 2: Create a snapshot repository and take a snapshot of the index
        createRepository(repository)
        val snapshotName = "$indexName-${OpenSearchTestCase.randomAlphaOfLength(10).lowercase()}"
        createSnapshot(repository, snapshotName, true)

        // Step 3: Assign a policy with ConvertIndexToRemoteAction to the index
        val actionConfig = ConvertIndexToRemoteAction(repository, 0)
        val states = listOf(State("ConvertToRemote", listOf(actionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        addPolicyToIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Step 4: Trigger the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Trigger AttemptRestoreStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(AttemptRestoreStep.getSuccessMessage(indexName), explainMetaData.info?.get("message"))
        }

        // Trigger WaitForRestoreStep
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(WaitForRestoreStep.getSuccessMessage(indexName), explainMetaData.info?.get("message"))
        }

        // Step 5: Verify that the remote index exists and contains the expected data
        val remoteIndexName = "${indexName}_remote"
        waitFor { assertIndexExists(remoteIndexName) }

        // Verify that the restored index is a remote index
        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)
    }
}
