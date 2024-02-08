/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.search.SearchResponse
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class IndexStateManagementHistoryIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test basic workflow`() {
        val indexName = "${testIndexName}_index_1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = ReadOnlyAction(0)
        val states =
            listOf(
                State("ReadOnlyState", listOf(actionConfig), listOf()),
            )
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)
        resetHistorySetting()

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // First run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Second run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val historySearchResponse: SearchResponse =
            waitFor {
                val historySearchResponse = getHistorySearchResponse(indexName)
                assertEquals(2, historySearchResponse.hits.totalHits!!.value)
                historySearchResponse
            }
        val actualHistory = getLatestHistory(historySearchResponse)
        val expectedHistory =
            ManagedIndexMetaData(
                indexName,
                getUuid(indexName),
                policyID,
                actualHistory.policySeqNo,
                policyPrimaryTerm = actualHistory.policyPrimaryTerm,
                policyCompleted = null,
                rolledOver = null,
                indexCreationDate = actualHistory.indexCreationDate,
                transitionTo = null,
                stateMetaData = StateMetaData("ReadOnlyState", actualHistory.stateMetaData!!.startTime),
                actionMetaData = ActionMetaData(ReadOnlyAction.name, actualHistory.actionMetaData!!.startTime, 0, false, 0, 0, null),
                stepMetaData = StepMetaData("set_read_only", actualHistory.stepMetaData!!.startTime, Step.StepStatus.COMPLETED),
                policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
                info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName)),
            )
        assertEquals(expectedHistory, actualHistory)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test history write index cannot be deleted if enabled, can be deleted if disabled`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = ReadOnlyAction(0)
        val states =
            listOf(
                State("ReadOnlyState", listOf(actionConfig), listOf()),
            )
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "true")
        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "2s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.key, "1s")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // First run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID)
        }

        // Second run
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val historySearchResponse: SearchResponse =
            waitFor {
                val historySearchResponse = getHistorySearchResponse(indexName)
                assertEquals(2, historySearchResponse.hits.totalHits!!.value)
                historySearchResponse
            }
        val actualHistory = getLatestHistory(historySearchResponse)
        val expectedHistory =
            ManagedIndexMetaData(
                indexName,
                getUuid(indexName),
                policyID,
                actualHistory.policySeqNo,
                policyPrimaryTerm = actualHistory.policyPrimaryTerm,
                policyCompleted = null,
                rolledOver = null,
                indexCreationDate = actualHistory.indexCreationDate,
                transitionTo = null,
                stateMetaData = StateMetaData("ReadOnlyState", actualHistory.stateMetaData!!.startTime),
                actionMetaData = ActionMetaData(ReadOnlyAction.name, actualHistory.actionMetaData!!.startTime, 0, false, 0, 0, null),
                stepMetaData = StepMetaData("set_read_only", actualHistory.stepMetaData!!.startTime, Step.StepStatus.COMPLETED),
                policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
                info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName)),
            )
        assertEquals(expectedHistory, actualHistory)

        var historyIndexName = getIndexNamesOfPattern(".opendistro-ism-managed-index-history")
        assert(historyIndexName.first().endsWith("-1"))
        // e.g.: .opendistro-ism-managed-index-history-2023.10.01-1
        updateClusterSetting(ManagedIndexSettings.HISTORY_MAX_DOCS.key, "1")
        waitFor {
            historyIndexName = getIndexNamesOfPattern(".opendistro-ism-managed-index-history")
            assert(historyIndexName.first().endsWith("2"))
            // e.g.: .opendistro-ism-managed-index-history-2023.10.01-000002
        }

        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "false")
        waitFor { assertFalse("History index does exist.", aliasExists(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)) }
    }

    fun `test history shard settings`() {
        deleteIndex(IndexManagementIndices.HISTORY_ALL)
        val indexName = "${testIndexName}_shard_settings"
        val policyID = "${testIndexName}_shard_settings_1"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(State("ReadOnlyState", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        resetHistorySetting()
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.key, "2")
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.key, "3")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID)
            assertIndexExists(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            val indexSettings = getIndexSettings(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            val historyIndexName =
                indexSettings.keys.firstOrNull { it.startsWith(IndexManagementIndices.HISTORY_INDEX_BASE) }
            assertNotNull("Could not find a concrete history index", historyIndexName)
            assertEquals("Wrong number of shards", 2, getNumberOfShardsSetting(historyIndexName!!))
            assertEquals("Wrong number of replicas", 3, getNumberOfReplicasSetting(historyIndexName))
        }
    }

    private fun resetHistorySetting() {
        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "true")
        updateClusterSetting(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.key, "60s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "60s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.key, "1")
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.key, "1")
    }
}
