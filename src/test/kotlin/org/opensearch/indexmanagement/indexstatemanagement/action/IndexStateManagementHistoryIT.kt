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
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
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
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)
        resetHistorySetting()

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse: SearchResponse = waitFor {
            val historySearchResponse = getHistorySearchResponse(indexName)
            assertEquals(2, historySearchResponse.hits.totalHits!!.value)
            historySearchResponse
        }

        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
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
            info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName))
        )

        assertEquals(expectedHistory, actualHistory)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test short retention period and history enabled`() {
        val indexName = "${testIndexName}_index_2"
        val policyID = "${testIndexName}_testPolicyName_2"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()
        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "true")
        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "5s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.key, "5s")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse: SearchResponse = waitFor {
            val historySearchResponse = getHistorySearchResponse(indexName)
            assertEquals(2, historySearchResponse.hits.totalHits!!.value)
            historySearchResponse
        }

        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
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
            info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName))
        )

        assertEquals(expectedHistory, actualHistory)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test small doc count rolledover index`() {
        val indexName = "${testIndexName}_index_3"
        val policyID = "${testIndexName}_testPolicyNam_3"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()
        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "true")
        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "5s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_MAX_DOCS.key, "1")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse: SearchResponse = waitFor {
            val historySearchResponse = getHistorySearchResponse(indexName)
            assertEquals(2, historySearchResponse.hits.totalHits!!.value)
            historySearchResponse
        }

        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
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
            info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName))
        )

        assertEquals(expectedHistory, actualHistory)

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test short retention period and rolledover index`() {
        val indexName = "${testIndexName}_index_4"
        val policyID = "${testIndexName}_testPolicyNam_4"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()
        resetHistorySetting()

        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "true")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse: SearchResponse = waitFor {
            val historySearchResponse = getHistorySearchResponse(indexName)
            assertEquals(1, historySearchResponse.hits.totalHits!!.value)
            historySearchResponse
        }

        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
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
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully initialized policy: $policyID")
        )

        assertEquals(expectedHistory, actualHistory)

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse1: SearchResponse = waitFor {
            val historySearchResponse1 = getHistorySearchResponse(indexName)
            assertEquals(2, historySearchResponse1.hits.totalHits!!.value)
            historySearchResponse1
        }

        val actualHistory1 = getLatestHistory(historySearchResponse1)

        val expectedHistory1 = ManagedIndexMetaData(
            indexName,
            getUuid(indexName),
            policyID,
            actualHistory1.policySeqNo,
            policyPrimaryTerm = actualHistory1.policyPrimaryTerm,
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = actualHistory1.indexCreationDate,
            transitionTo = null,
            stateMetaData = StateMetaData(states[0].name, actualHistory1.stateMetaData!!.startTime),
            actionMetaData = ActionMetaData(ReadOnlyAction.name, actualHistory1.actionMetaData!!.startTime, 0, false, 0, 0, null),
            stepMetaData = StepMetaData("set_read_only", actualHistory1.stepMetaData!!.startTime, Step.StepStatus.COMPLETED),
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to SetReadOnlyStep.getSuccessMessage(indexName))
        )

        assertEquals(expectedHistory1, actualHistory1)

        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "2s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_MAX_DOCS.key, "1")
        updateClusterSetting(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.key, "1s")

        // After updating settings, ensure all the histories are deleted.
        waitFor {
            val historySearchResponse3 = getHistorySearchResponse(indexName)
            assertEquals(0, historySearchResponse3.hits.totalHits!!.value)
        }

        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test short retention period and history disabled`() {
        val indexName = "${testIndexName}_index_5"
        val policyID = "${testIndexName}_testPolicyName_5"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        restAdminSettings()
        resetHistorySetting()

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val historySearchResponse: SearchResponse = waitFor {
            val historySearchResponse = getHistorySearchResponse(indexName)
            assertEquals(1, historySearchResponse.hits.totalHits!!.value)
            historySearchResponse
        }

        val actualHistory = getLatestHistory(historySearchResponse)

        val expectedHistory = ManagedIndexMetaData(
            indexName,
            getUuid(indexName),
            policyID,
            actualHistory.policySeqNo,
            policyPrimaryTerm = actualHistory.policyPrimaryTerm,
            policyCompleted = null,
            rolledOver = null,
            indexCreationDate = actualHistory.indexCreationDate,
            transitionTo = null,
            stateMetaData = StateMetaData(name = states[0].name, startTime = actualHistory.stateMetaData!!.startTime),
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = PolicyRetryInfoMetaData(false, 0),
            info = mapOf("message" to "Successfully initialized policy: $policyID")
        )

        assertEquals(expectedHistory, actualHistory)

        updateClusterSetting(ManagedIndexSettings.HISTORY_ENABLED.key, "false")
        updateClusterSetting(ManagedIndexSettings.HISTORY_RETENTION_PERIOD.key, "1s")
        updateClusterSetting(ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD.key, "2s")

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertFalse("History index does exist.", aliasExists(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)) }
        waitFor { assertEquals("true", getIndexBlocksWriteSetting(indexName)) }
    }

    fun `test history shard settings`() {
        val indexName = "${testIndexName}_shard_settings"
        val policyID = "${testIndexName}_shard_settings_1"
        val actionConfig = ReadOnlyAction(0)
        val states = listOf(State("ReadOnlyState", listOf(actionConfig), listOf()))

        val policy = Policy(
            id = policyID,
            description = "$testIndexName description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)
        resetHistorySetting()
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS.key, "2")
        updateClusterSetting(ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS.key, "3")

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Need to wait two cycles.
        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertIndexExists(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            val indexSettings = getIndexSettings(IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS)
            val historyIndexName = indexSettings.keys.filter { it.startsWith(IndexManagementIndices.HISTORY_INDEX_BASE) }.firstOrNull()
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
