/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.runner

import org.opensearch.client.Request
import org.opensearch.common.settings.Settings
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.OpenAction
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyAction
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.randomPolicy
import org.opensearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomState
import org.opensearch.indexmanagement.indexstatemanagement.randomTransition
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import org.opensearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep
import org.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.PolicyRetryInfoMetaData
import org.opensearch.indexmanagement.waitFor
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import java.time.Instant
import java.time.temporal.ChronoUnit

class ManagedIndexRunnerIT : IndexStateManagementRestTestCase() {
    fun `test version conflict fails job`() {
        val indexName = "version_conflict_index"
        val policyID = "version_conflict_policy"
        val actionConfig = OpenAction(0)
        val states = listOf(State("OpenState", listOf(actionConfig), listOf()))

        val policy =
            Policy(
                id = policyID,
                description = "$indexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy on managed index
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // change policy seqNo on managed index
        updateManagedIndexConfigPolicySeqNo(managedIndexConfig.copy(policySeqNo = 17))

        // start execution to see if it moves to failed because of version conflict
        updateManagedIndexConfigStartTime(managedIndexConfig)

        val expectedInfoString = mapOf("message" to "There is a version conflict between your previous execution and your managed index").toString()
        waitFor {
            assertPredicatesOnMetaData(
                listOf(
                    indexName to
                        listOf(
                            PolicyRetryInfoMetaData.RETRY_INFO to

                                fun(retryInfoMetaDataMap: Any?): Boolean =
                                    assertRetryInfoEquals(PolicyRetryInfoMetaData(true, 0), retryInfoMetaDataMap),
                            ManagedIndexMetaData.INFO to

                                fun(info: Any?): Boolean = expectedInfoString == info.toString(),
                        ),
                ),
                getExplainMap(indexName),
                strict = false,
            )
        }
    }

    fun `test job interval changing`() {
        val indexName = "job_interval_index_"

        val createdPolicy = createRandomPolicy()
        createIndex(indexName, createdPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        assertEquals(
            "Created managed index did not default to ${ManagedIndexSettings.DEFAULT_JOB_INTERVAL} minutes",
            ManagedIndexSettings.DEFAULT_JOB_INTERVAL, (managedIndexConfig.jobSchedule as IntervalSchedule).interval,
        )

        // init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(createdPolicy.id, getManagedIndexConfigByDocId(managedIndexConfig.id)?.policyID) }
        // change cluster job interval setting to 2 (minutes)
        updateClusterSetting(ManagedIndexSettings.JOB_INTERVAL.key, "2")
        // fast forward to next execution where at the end we should change the job interval time
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { (getManagedIndexConfigByDocId(managedIndexConfig.id)?.jobSchedule as? IntervalSchedule)?.interval == 2 }
        waitFor {
            assertEquals(createdPolicy.id, getManagedIndexConfigByDocId(managedIndexConfig.id)?.policyID)
            val currInterval = (getManagedIndexConfigByDocId(managedIndexConfig.id)?.jobSchedule as? IntervalSchedule)?.interval
            assertEquals("Managed index was not created with default job interval", ManagedIndexSettings.DEFAULT_JOB_INTERVAL, currInterval)
        }

        // change cluster job interval setting to 2 (minutes)
        val newJobInterval = 2
        updateClusterSetting(ManagedIndexSettings.JOB_INTERVAL.key, newJobInterval.toString())

        // Create a new index and policy to check if they have the updated interval
        val newIndexName = indexName + "new"
        val newCreatedPolicy = createRandomPolicy()
        createIndex(newIndexName, newCreatedPolicy.id)

        val newManagedIndexConfig = getExistingManagedIndexConfig(newIndexName)

        assertEquals(
            "New managed index did not have updated job schedule interval",
            newJobInterval, (newManagedIndexConfig.jobSchedule as IntervalSchedule).interval,
        )

        // init new policy
        updateManagedIndexConfigStartTime(newManagedIndexConfig)
        waitFor {
            assertEquals(newCreatedPolicy.id, getManagedIndexConfigByDocId(newManagedIndexConfig.id)?.policyID)
            val currInterval = (getManagedIndexConfigByDocId(newManagedIndexConfig.id)?.jobSchedule as? IntervalSchedule)?.interval
            assertEquals("Failed to update ManagedIndexConfig interval", newJobInterval, currInterval)
        }
    }

    fun `test allow list fails execution`() {
        val indexName = "allow_list_index"

        val firstState =
            randomState(
                name = "first_state", actions = listOf(randomReadOnlyActionConfig()),
                transitions = listOf(randomTransition(stateName = "second_state", conditions = null)),
            )
        val secondState =
            randomState(
                name = "second_state", actions = listOf(randomReadWriteActionConfig()),
                transitions = listOf(randomTransition(stateName = "first_state", conditions = null)),
            )
        val randomPolicy = randomPolicy(id = "allow_policy", states = listOf(firstState, secondState))

        val createdPolicy = createPolicy(randomPolicy, "allow_policy")
        createIndex(indexName, createdPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // init policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(createdPolicy.id, getExplainManagedIndexMetaData(indexName).policyID) }

        // speed up to first execution that should set index to read only
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(SetReadOnlyStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to second execution that should transition to second_state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, secondState.name), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to third execution that should set index back to read write
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(SetReadWriteStep.getSuccessMessage(indexName), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // speed up to fourth execution that should transition to first_state
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(AttemptTransitionStep.getSuccessMessage(indexName, firstState.name), getExplainManagedIndexMetaData(indexName).info?.get("message")) }

        // remove read_only from the allowlist
        val allowedActions =
            ISMActionsParser.instance.parsers.map { it.getActionType() }.toList()
                .filter { actionType -> actionType != ReadOnlyAction.name }
                .joinToString(prefix = "[", postfix = "]") { string -> "\"$string\"" }
        updateClusterSetting(ManagedIndexSettings.ALLOW_LIST.key, allowedActions, escapeValue = false)

        // speed up to fifth execution that should try to set index to read only and fail because the action is not allowed
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals("Attempted to execute action=read_only which is not allowed.", getExplainManagedIndexMetaData(indexName).info?.get("message")) }
    }

    fun `test jitter changing`() {
        val indexName = "jitter_index_"

        val createdPolicy = createRandomPolicy()
        createIndex(indexName, createdPolicy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        assertEquals(
            "Created managed index did not default to 0.0", 0.0, managedIndexConfig.jitter,
        )

        waitFor {
            assertEquals(createdPolicy.id, getManagedIndexConfigByDocId(managedIndexConfig.id)?.policyID)
            val currJitter = getManagedIndexConfigByDocId(managedIndexConfig.id)?.jitter
            assertEquals("Managed index was not created with 0.0 jitter", 0.0, currJitter)
        }

        // change jitter to 0.5
        val newJitter = 0.5
        updateIndexStateManagementJitterSetting(newJitter)

        // Create a new index and policy to check if they have the updated jitter
        val newIndexName = indexName + "new"
        val newCreatedPolicy = createRandomPolicy()
        createIndex(newIndexName, newCreatedPolicy.id)

        val newManagedIndexConfig = getExistingManagedIndexConfig(newIndexName)
        assertEquals(
            "New managed index did not have updated jitter", newJitter, newManagedIndexConfig.jitter,
        )

        waitFor {
            assertEquals(newCreatedPolicy.id, getManagedIndexConfigByDocId(newManagedIndexConfig.id)?.policyID)
            val currJitter = getManagedIndexConfigByDocId(newManagedIndexConfig.id)?.jitter
            assertEquals("Failed to update ManagedIndexConfig jitter", newJitter, currJitter)
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test allow running on red cluster setting can be toggled dynamically`() {
        // A successful update also verifies the setting is registered by the plugin, otherwise the
        // persistent cluster settings update would be rejected as an unknown setting.
        updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "true", escapeValue = false)

        val response = client().makeRequest("GET", "_cluster/settings", mapOf("flat_settings" to "true"))
        val persistentSettings = response.asMap()["persistent"] as Map<String, Any?>
        assertEquals(
            "Cluster setting was not applied",
            "true",
            persistentSettings[ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key],
        )

        // reset to default so it doesn't leak into other tests
        updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "false", escapeValue = false)
    }

    fun `test delete action runs on red cluster only after setting is enabled`() {
        val indexName = "red_cluster_delete_index"
        val policyID = "red_cluster_delete_policy"
        val redHealthTriggerIndex = "red_cluster_delete_trigger"
        val deleteState = randomState(name = "delete_state", actions = listOf(randomDeleteActionConfig()))
        val policy = createPolicy(randomPolicy(id = policyID, states = listOf(deleteState)), policyID)

        createIndex(indexName, policy.id, replicas = "0")
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policy.id, getExplainManagedIndexMetaData(indexName).policyID) }
        val metadataBeforeRedClusterRun = getExplainManagedIndexMetaData(indexName)

        try {
            updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "false", escapeValue = false)
            createRedHealthTrigger(redHealthTriggerIndex)
            updateManagedIndexConfigStartTime(managedIndexConfig)
            Thread.sleep(RED_CLUSTER_SKIP_ASSERTION_DELAY_MILLIS)

            assertIndexExists(indexName)
            val metadataAfterRedClusterRun = getExplainManagedIndexMetaData(indexName)
            assertEquals(metadataBeforeRedClusterRun.actionMetaData, metadataAfterRedClusterRun.actionMetaData)
            assertEquals(metadataBeforeRedClusterRun.stepMetaData, metadataAfterRedClusterRun.stepMetaData)

            updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "true", escapeValue = false)
            updateManagedIndexConfigStartTime(managedIndexConfig)

            waitFor { assertIndexDoesNotExist(indexName) }
            assertEquals("red", getClusterHealthStatus())
        } finally {
            cleanupRedClusterTest(redHealthTriggerIndex)
        }
    }

    fun `test read write action waits for red cluster to recover`() {
        val indexName = "red_cluster_read_write_index"
        val policyID = "red_cluster_read_write_policy"
        val redHealthTriggerIndex = "red_cluster_read_write_trigger"
        val readWriteState = randomState(name = "read_write_state", actions = listOf(randomReadWriteActionConfig()))
        val policy = createPolicy(randomPolicy(id = policyID, states = listOf(readWriteState)), policyID)

        createIndex(indexName, null, replicas = "0")
        updateIndexSettings(indexName, Settings.builder().put("index.blocks.write", true))
        assertEquals("true", getIndexBlocksWriteSetting(indexName))
        addPolicyToIndex(indexName, policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policy.id, getExplainManagedIndexMetaData(indexName).policyID) }
        val metadataBeforeRedClusterRun = getExplainManagedIndexMetaData(indexName)

        try {
            updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "true", escapeValue = false)
            createRedHealthTrigger(redHealthTriggerIndex)
            updateManagedIndexConfigStartTime(managedIndexConfig)
            Thread.sleep(RED_CLUSTER_SKIP_ASSERTION_DELAY_MILLIS)

            assertEquals("true", getIndexBlocksWriteSetting(indexName))
            val metadataAfterRedClusterRun = getExplainManagedIndexMetaData(indexName)
            assertEquals(metadataBeforeRedClusterRun.actionMetaData, metadataAfterRedClusterRun.actionMetaData)
            assertEquals(metadataBeforeRedClusterRun.stepMetaData, metadataAfterRedClusterRun.stepMetaData)

            deleteRedHealthTrigger(redHealthTriggerIndex)
            updateManagedIndexConfigStartTime(managedIndexConfig)

            waitFor { assertEquals("false", getIndexBlocksWriteSetting(indexName)) }
        } finally {
            cleanupRedClusterTest(redHealthTriggerIndex)
        }
    }

    private fun createRedHealthTrigger(indexName: String) {
        val request = Request("PUT", "/$indexName?wait_for_active_shards=0")
        request.setJsonEntity(
            """
            {
              "settings": {
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0,
                "index.routing.allocation.require._name": "$NONEXISTENT_NODE_NAME"
              }
            }
            """.trimIndent(),
        )
        val response = client().performRequest(request)
        assertEquals("Failed to create red-health trigger index", RestStatus.OK, response.restStatus())
        waitFor { assertEquals("red", getClusterHealthStatus()) }
    }

    private fun deleteRedHealthTrigger(indexName: String) {
        if (isIndexExists(indexName)) {
            deleteIndex(indexName)
        }
        waitFor { assertNotEquals("red", getClusterHealthStatus()) }
    }

    private fun cleanupRedClusterTest(indexName: String) {
        try {
            deleteRedHealthTrigger(indexName)
        } finally {
            updateClusterSetting(ManagedIndexSettings.ALLOW_RUNNING_ON_RED_CLUSTER.key, "null", escapeValue = false)
        }
    }

    private fun getClusterHealthStatus(): String =
        client().makeRequest("GET", "/_cluster/health").asMap()["status"] as String

    companion object {
        private const val NONEXISTENT_NODE_NAME = "nonexistent-node-for-red-cluster-test"
        private const val RED_CLUSTER_SKIP_ASSERTION_DELAY_MILLIS = 5_000L
    }
}
