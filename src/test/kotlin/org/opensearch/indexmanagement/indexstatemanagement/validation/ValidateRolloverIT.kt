/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.validation

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.Assert
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class ValidateRolloverIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    @Suppress("UNCHECKED_CAST")
    fun `test rollover no condition`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, null, null, null, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        // create index defaults
        createIndex(firstIndex, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals("Index did not rollover.", AttemptRolloverStep.getSuccessMessage(firstIndex), info["message"])
            assertNull("Should not have conditions if none specified", info["conditions"])
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    fun `test skip rollover`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add")
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).info as Map<String, Any?>
            assertEquals(
                "Index rollover not stopped by pre-check.",
                "Successfully initialized policy: validaterolloverit_precheck", info["message"]
            )
        }

        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_SKIP.key, "true")

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig)
//        waitFor {
//            val info = getExplainManagedIndexMetaData(index1).info as Map<String, Any?>
//            assertEquals(
//                    "Index rollover not stopped by pre-check.",
//                    "Successfully initialized policy: validaterolloverit_precheck", info["message"]
//            )
//        }
    }

    fun `test already rolled over`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()), State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add", true)
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
//            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
//            assertEquals(
//                "Index rollover not stopped by pre-check.",
//                ValidateRollover.getFailedWriteIndexMessage(index1), info["message"]
//            )
        }
    }

    fun `test rollover does not have alias`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add")
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
//            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
//            assertEquals(
//                "Index rollover not stopped by pre-check.",
//                ValidateRollover.getFailedWriteIndexMessage(index1), info["message"]
//            )
        }

        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_SKIP.key, "true")

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
            assertEquals(
                "Index rollover not skip.",
                ValidateRollover.getSkipRolloverMessage(index1), info["message"]
            )
        }
    }

    fun `test rollover write index`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add")
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
            assertEquals(
                "Index rollover not stopped by pre-check.",
                "did not fail", info["message"]
            )
        }

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig) // should already by rolled over
        waitFor {
//            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
//            assertEquals(
//                "Index rollover not stopped by pre-check.",
//                ValidateRollover.getFailedWriteIndexMessage(index1), info["message"]
//            )
        }
    }

    fun `test rollover does not have target`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(index1, policyID)
        changeAlias(index1, alias1, "add")
        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)
        createIndex(index2, policyID)
        changeAlias(index2, alias1, "add", true)
        updateIndexSetting(index2, ManagedIndexSettings.ROLLOVER_ALIAS.key, alias1)

        val managedIndexConfig = getExistingManagedIndexConfig(index1)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index1).policyID) }

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
            assertEquals(
                "Index rollover not stopped by pre-check.",
                "did not fail", info["message"]
            )
        }

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig) // should already by rolled over
        waitFor {
//            val info = getExplainManagedIndexMetaData(index1).validationInfo as Map<String, Any?>
//            assertEquals(
//                "Index rollover not stopped by pre-check.",
//                ValidateRollover.getFailedWriteIndexMessage(index1), info["message"]
//            )
        }
    }

    fun `test data stream rollover no condition`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollover_policy"

        // Create the rollover policy
        val rolloverAction = RolloverAction(null, null, null, null, 0)
        val states = listOf(State(name = "default", actions = listOf(rolloverAction), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "rollover policy description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS)))
        )
        createPolicy(policy, policyID)

        // Create the data stream
        val firstIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        client().makeRequest(
            "PUT",
            "/_index_template/rollover-data-stream-template",
            StringEntity("{ \"index_patterns\": [ \"$dataStreamName\" ], \"data_stream\": { } }", ContentType.APPLICATION_JSON)
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        var managedIndexConfig = getExistingManagedIndexConfig(firstIndexName)

        // Change the start time so that the job will trigger in 2 seconds. This will trigger the first initialization of the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndexName).policyID) }

        // Speed up to the second execution of the policy where it will trigger the first execution of the action.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndexName).info as Map<String, Any?>
            assertEquals(
                "Data stream did not rollover.",
                AttemptRolloverStep.getSuccessDataStreamRolloverMessage(dataStreamName, firstIndexName),
                info["message"]
            )
            assertNull("Should not have conditions if none specified", info["conditions"])
        }

        val secondIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2L)
        Assert.assertTrue("New rollover index does not exist.", indexExists(secondIndexName))

        // Ensure that that policy is applied to the newly created index as well.
        managedIndexConfig = getExistingManagedIndexConfig(secondIndexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(secondIndexName).policyID) }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollover from outside ISM doesn't fail ISM job`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, null, null, null, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        // create index defaults
        createIndex(firstIndex, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // Rollover the alias manually before ISM tries to roll it over
        rolloverIndex(aliasName)

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            val stepMetadata = getExplainManagedIndexMetaData(firstIndex).stepMetaData
            assertEquals("Index should succeed if already rolled over.", AttemptRolloverStep.getAlreadyRolledOverMessage(firstIndex, aliasName), info["message"])
            assertEquals("Index should succeed if already rolled over.", Step.StepStatus.COMPLETED, stepMetadata?.stepStatus)
        }
        assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }
}
