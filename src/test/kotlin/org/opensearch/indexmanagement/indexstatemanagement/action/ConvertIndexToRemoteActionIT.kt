/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

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
            0,
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
                "Transitioning to convertToRemoteState [index=convertindextoremoteactionit_index_snapshot_and_convert]",
                explainMetaData.info?.get("message"),
            )
        }
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val explainMetaData = getExplainManagedIndexMetaData(indexName)
            assertEquals(
                AttemptRestoreStep.getSuccessMessage(indexName),
                explainMetaData.info?.get("message"),
            )
        }

        val remoteIndexName = "${indexName}_remote"
        waitFor { assertIndexExists(remoteIndexName) }

        val isRemote = isIndexRemote(remoteIndexName)
        assertTrue("Index $remoteIndexName is not a remote index", isRemote)
    }
}
