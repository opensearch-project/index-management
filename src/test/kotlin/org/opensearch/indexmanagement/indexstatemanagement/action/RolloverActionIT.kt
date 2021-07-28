/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.hamcrest.core.Is.isA
import org.junit.Assert
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.common.unit.ByteSizeUnit
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.waitFor
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestStatus
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RolloverActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    @Suppress("UNCHECKED_CAST")
    fun `test rollover no condition`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverActionConfig(null, null, null, 0)
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

    fun `test rollover with open distro rollover_alias setting`() {
        val indexNameBase = "bwc_index"
        val firstIndex = "$indexNameBase-1"
        val aliasName = "bwc_alias"
        client().makeRequest(
            "PUT", "/$firstIndex",
            StringEntity(
                "{\n" +
                    "  \"settings\": {\n" +
                    "    \"index\": {\n" +
                    "      \"opendistro.index_state_management.rollover_alias\": \"$aliasName\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"aliases\": {\n" +
                    "    \"$aliasName\": {\"is_write_index\": true}\n" +
                    "  }\n" +
                    "}",
                ContentType.APPLICATION_JSON
            )
        )

        val policyID = "${testIndexName}_bwc"
        val actionConfig = RolloverActionConfig(null, null, null, 0)
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

        addPolicyToIndex(firstIndex, policyID)

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

    @Suppress("UNCHECKED_CAST")
    fun `test rollover multi condition byte size`() {
        val aliasName = "${testIndexName}_byte_alias"
        val indexNameBase = "${testIndexName}_index_byte"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_byte_1"
        val actionConfig = RolloverActionConfig(ByteSizeValue(10, ByteSizeUnit.BYTES), 1000000, null, 0)
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
            assertEquals(
                "Index rollover before it met the condition.",
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"]
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min size and min doc count conditions",
                setOf(RolloverActionConfig.MIN_SIZE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minSize = conditions[RolloverActionConfig.MIN_SIZE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min size condition", "10b", minSize["condition"])
            assertThat("Did not have min size current", minSize["current"], isA(String::class.java))
            assertEquals("Did not have min doc count condition", 1000000, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 0, minDocCount["current"])
        }

        insertSampleData(index = firstIndex, docCount = 5, delay = 0)

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals("Index did not rollover", AttemptRolloverStep.getSuccessMessage(firstIndex), info["message"])
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min size and min doc count conditions",
                setOf(RolloverActionConfig.MIN_SIZE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minSize = conditions[RolloverActionConfig.MIN_SIZE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min size condition", "10b", minSize["condition"])
            assertThat("Did not have min size current", minSize["current"], isA(String::class.java))
            assertEquals("Did not have min doc count condition", 1000000, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 5, minDocCount["current"])
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollover multi condition doc size`() {
        val aliasName = "${testIndexName}_doc_alias"
        val indexNameBase = "${testIndexName}_index_doc"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_doc_1"
        val actionConfig = RolloverActionConfig(null, 3, TimeValue.timeValueDays(2), 0)
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
            assertEquals(
                "Index rollover before it met the condition.",
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"]
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverActionConfig.MIN_INDEX_AGE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minAge = conditions[RolloverActionConfig.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertThat("Did not have min age current", minAge["current"], isA(String::class.java))
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 0, minDocCount["current"])
        }

        insertSampleData(index = firstIndex, docCount = 5, delay = 0)

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals("Index did not rollover", AttemptRolloverStep.getSuccessMessage(firstIndex), info["message"])
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverActionConfig.MIN_INDEX_AGE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD), conditions.keys
            )
            val minAge = conditions[RolloverActionConfig.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertThat("Did not have min age current", minAge["current"], isA(String::class.java))
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 5, minDocCount["current"])
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    fun `test rollover pre check`() {
        // index-1 alias x
        // index-2 alias x is_write_index
        // manage index-1, expect it fail to rollover
        val index1 = "index-1"
        val index2 = "index-2"
        val alias1 = "x"
        val policyID = "${testIndexName}_precheck"
        val actionConfig = RolloverActionConfig(null, 3, TimeValue.timeValueDays(2), 0)
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
                AttemptRolloverStep.getFailedPreCheckMessage(index1), info["message"]
            )
        }

        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_SKIP.key, "true")

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1"
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).info as Map<String, Any?>
            assertEquals(
                "Index rollover not skip.",
                AttemptRolloverStep.getSkipRolloverMessage(index1), info["message"]
            )
        }
    }

    fun `test data stream rollover no condition`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollover_policy"

        // Create the rollover policy
        val rolloverActionConfig = RolloverActionConfig(null, null, null, 0)
        val states = listOf(State(name = "default", actions = listOf(rolloverActionConfig), transitions = listOf()))
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
    fun `test data stream rollover multi condition doc size`() {
        val dataStreamName = "${testIndexName}_data_stream_multi"
        val policyID = "${testIndexName}_rollover_policy_multi"

        // Create the rollover policy
        val rolloverActionConfig = RolloverActionConfig(null, 3, TimeValue.timeValueDays(2), 0)
        val states = listOf(State(name = "default", actions = listOf(rolloverActionConfig), transitions = listOf()))
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

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndexName)

        // Change the start time so that the job will trigger in 2 seconds. This will trigger the first initialization of the policy.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndexName).policyID) }

        // Speed up to the second execution of the policy where it will trigger the first execution of the action.
        // Rollover shouldn't have happened yet as the conditions aren't met.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndexName).info as Map<String, Any?>
            assertEquals(
                "Index rollover before it met the condition.",
                AttemptRolloverStep.getPendingMessage(firstIndexName),
                info["message"]
            )

            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverActionConfig.MIN_INDEX_AGE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD),
                conditions.keys
            )

            val minAge = conditions[RolloverActionConfig.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Incorrect min age condition", "2d", minAge["condition"])
            assertEquals("Incorrect min docs condition", 3, minDocCount["condition"])
            assertThat("Missing min age current", minAge["current"], isA(String::class.java))
            assertEquals("Incorrect min docs current", 0, minDocCount["current"])
        }

        insertSampleData(index = dataStreamName, docCount = 5, jsonString = "{ \"@timestamp\": \"2020-12-06T11:04:05.000Z\" }")

        // Speed up to the third execution of the policy where it will trigger the second execution of the action.
        // Rollover should have happened as the conditions were met.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndexName).info as Map<String, Any?>
            assertEquals(
                "Data stream did not rollover",
                AttemptRolloverStep.getSuccessDataStreamRolloverMessage(dataStreamName, firstIndexName),
                info["message"]
            )

            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverActionConfig.MIN_INDEX_AGE_FIELD, RolloverActionConfig.MIN_DOC_COUNT_FIELD),
                conditions.keys
            )

            val minAge = conditions[RolloverActionConfig.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverActionConfig.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Incorrect min age condition", "2d", minAge["condition"])
            assertEquals("Incorrect min docs condition", 3, minDocCount["condition"])
            assertThat("Missing min age current", minAge["current"], isA(String::class.java))
            assertEquals("Incorrect min docs current", 5, minDocCount["current"])
        }

        val secondIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2L)
        Assert.assertTrue("New rollover index does not exist.", indexExists(secondIndexName))
    }
}
