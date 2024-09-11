/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.junit.Assert
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.unit.ByteSizeUnit
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
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
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RolloverActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test rollover no condition`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, null, null, null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
                ContentType.APPLICATION_JSON,
            ),
        )

        val policyID = "${testIndexName}_bwc"
        val actionConfig = RolloverAction(null, null, null, null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        val actionConfig = RolloverAction(ByteSizeValue(10, ByteSizeUnit.BYTES), 1000000, null, null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"],
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min size and min doc count conditions",
                setOf(RolloverAction.MIN_SIZE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minSize = conditions[RolloverAction.MIN_SIZE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min size condition", "10b", minSize["condition"])
            assertTrue("Did not have min size current", minSize["current"] is String)
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
                setOf(RolloverAction.MIN_SIZE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minSize = conditions[RolloverAction.MIN_SIZE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min size condition", "10b", minSize["condition"])
            assertTrue("Did not have min size current", minSize["current"] is String)
            assertEquals("Did not have min doc count condition", 1000000, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 5, minDocCount["current"])
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollover min primary shard size`() {
        // Setup creating index with 20 primary shards and policy that rolls over on 100kb min primary shard size
        val aliasName = "${testIndexName}_primary_shard_alias"
        val indexNameBase = "${testIndexName}_index_primary_shard"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_primary_shard_1"
        val actionConfig = RolloverAction(null, null, null, ByteSizeValue(100, ByteSizeUnit.KB), false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        // create index defaults
        createIndex(
            index = firstIndex,
            policyID = policyID,
            alias = aliasName,
            replicas = "0",
            shards = "20",
            settings = Settings.builder().put("store.stats_refresh_interval", "1s").build(),
        )

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // assuming our ingestion is randomly split between the 20 primary shards
        // then 250kb/20 gives around 12.5kb per primary shard which is below our 100kb condition
        val kb250 = 250_000
        var primaryStoreSizeBytes = 0
        var count = 0
        // Ingest data into the test index until the total size of the index is greater than our min primary size condition
        while (primaryStoreSizeBytes < kb250) {
            // this count should never get as high as 10... if it does just fail the test
            if (count++ > 10) fail("Something is wrong with the data ingestion for testing rollover condition")
            insertSampleData(index = firstIndex, docCount = 20, jsonString = "{ \"test_field\": \"${OpenSearchTestCase.randomAlphaOfLength(7000)}\" }", delay = 0)
            flush(firstIndex, true)
            forceMerge(firstIndex, "1")
            val catIndex = (cat("indices/$firstIndex?format=json&bytes=b") as List<Map<String, Any>>).find { it["index"] == firstIndex }
            assertNotNull("Did not find index in cat response", catIndex)
            primaryStoreSizeBytes = (catIndex!!["pri.store.size"] as String).toInt()
        }

        // Need to speed up to second execution where it will trigger the first execution of the action
        // Confirm index is waiting to meet the rollover condition
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals(
                "Index rollover before it met the condition.",
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"],
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min primary shard size condition",
                setOf(RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD), conditions.keys,
            )
            val minPrimarySize = conditions[RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD] as Map<String, Any?>
            assertEquals("Did not have min size condition", "100kb", minPrimarySize["condition"])
            assertTrue("Did not have min size current", minPrimarySize["current"] is String)
        }

        val kb150 = 150_000
        var primaryShardSizeBytes = 0
        count = 0
        // Ingest data into the test index using custom routing so it always goes to a single shard until the size of the
        // primary shard is over 150kb
        while (primaryShardSizeBytes < kb150) {
            // this count should never get as high as 10... if it does just fail the test
            if (count++ > 10) fail("Something is wrong with the data ingestion for testing rollover condition")
            insertSampleData(index = firstIndex, docCount = 20, delay = 0, jsonString = "{ \"test_field\": \"${OpenSearchTestCase.randomAlphaOfLength(7000)}\" }", routing = "custom_routing")
            flush(firstIndex, true)
            forceMerge(firstIndex, "1")
            val primaryShards = waitFor {
                val primaryShards = (cat("shards/$firstIndex?format=json&bytes=b") as List<Map<String, Any>>).filter { it["prirep"] == "p" }
                // TODO seeing flakyness of multiple shards over 100kb, log out shards to further debug
                logger.info("cat shards result: $primaryShards")
                val primaryShardsOver100KB = primaryShards.filter { (it["store"] as String).toInt() > 100000 }
                assertTrue("Shard over 100kb is not exactly 1", primaryShardsOver100KB.size == 1)
                primaryShards
            }
            primaryShardSizeBytes = primaryShards.maxOf { (it["store"] as String).toInt() }
        }

        // Speed up to third execution
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals("Index did not rollover", AttemptRolloverStep.getSuccessMessage(firstIndex), info["message"])
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min primary shard size conditions",
                setOf(RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD), conditions.keys,
            )
            val minPrimaryShardSize = conditions[RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD] as Map<String, Any?>
            assertEquals("Did not have min primary shard size condition", "100kb", minPrimaryShardSize["condition"])
            assertTrue("Did not have min primary shard size current", minPrimaryShardSize["current"] is String)
        }
        assertTrue("New rollover index does not exist.", indexExists("$indexNameBase-000002"))
    }

    @Suppress("UNCHECKED_CAST")
    fun `test rollover multi condition doc size`() {
        val aliasName = "${testIndexName}_doc_alias"
        val indexNameBase = "${testIndexName}_index_doc"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_doc_1"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"],
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
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
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
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
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, false, 0)
        actionConfig.configRetry = ActionRetry(0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
                AttemptRolloverStep.getFailedPreCheckMessage(index1), info["message"],
            )
        }

        updateIndexSetting(index1, ManagedIndexSettings.ROLLOVER_SKIP.key, "true")

        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestRetryFailedManagedIndexAction.RETRY_BASE_URI}/$index1",
        )
        assertEquals("Unexpected RestStatus", RestStatus.OK, response.restStatus())

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(index1).info as Map<String, Any?>
            assertEquals(
                "Index rollover not skip.",
                AttemptRolloverStep.getSkipRolloverMessage(index1), info["message"],
            )
        }
    }

    fun `test data stream rollover no condition`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollover_policy"

        // Create the rollover policy
        val rolloverAction = RolloverAction(null, null, null, null, false, 0)
        val states = listOf(State(name = "default", actions = listOf(rolloverAction), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "rollover policy description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))),
        )
        createPolicy(policy, policyID)

        // Create the data stream
        val firstIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        client().makeRequest(
            "PUT",
            "/_index_template/rollover-data-stream-template",
            StringEntity("{ \"index_patterns\": [ \"$dataStreamName\" ], \"data_stream\": { } }", ContentType.APPLICATION_JSON),
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
                info["message"],
            )
            assertNull("Should not have conditions if none specified", info["conditions"])
        }

        val secondIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2L)
        Assert.assertTrue("New rollover index does not exist.", indexExists(secondIndexName))

        // Ensure that that policy is applied to the newly created index as well.
        managedIndexConfig = getExistingManagedIndexConfig(secondIndexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(secondIndexName).policyID) }

        val metadata = getExplainManagedIndexMetaData(firstIndexName)
        assertEquals(metadata.rolledOverIndexName, secondIndexName)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test data stream rollover multi condition doc size`() {
        val dataStreamName = "${testIndexName}_data_stream_multi"
        val policyID = "${testIndexName}_rollover_policy_multi"

        // Create the rollover policy
        val rolloverAction = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, false, 0)
        val states = listOf(State(name = "default", actions = listOf(rolloverAction), transitions = listOf()))
        val policy = Policy(
            id = policyID,
            description = "rollover policy description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))),
        )
        createPolicy(policy, policyID)

        // Create the data stream
        val firstIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        client().makeRequest(
            "PUT",
            "/_index_template/rollover-data-stream-template",
            StringEntity("{ \"index_patterns\": [ \"$dataStreamName\" ], \"data_stream\": { } }", ContentType.APPLICATION_JSON),
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
                info["message"],
            )

            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD),
                conditions.keys,
            )

            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Incorrect min age condition", "2d", minAge["condition"])
            assertEquals("Incorrect min docs condition", 3, minDocCount["condition"])
            assertTrue("Missing min age current", minAge["current"] is String)
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
                info["message"],
            )

            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD),
                conditions.keys,
            )

            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Incorrect min age condition", "2d", minAge["condition"])
            assertEquals("Incorrect min docs condition", 3, minDocCount["condition"])
            assertTrue("Missing min age current", minAge["current"] is String)
            assertEquals("Incorrect min docs current", 5, minDocCount["current"])
        }

        val secondIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2L)
        Assert.assertTrue("New rollover index does not exist.", indexExists(secondIndexName))

        val metadata = getExplainManagedIndexMetaData(firstIndexName)
        assertEquals(metadata.rolledOverIndexName, secondIndexName)
    }

    fun `test rollover from outside ISM doesn't fail ISM job`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, null, null, null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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

    @Suppress("UNCHECKED_CAST")
    fun `test rollover with copy alias`() {
        val aliasName = "${testIndexName}_doc_alias"
        val indexNameBase = "${testIndexName}_index_doc"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_doc_1"
        val actionConfig = RolloverAction(null, 3, TimeValue.timeValueDays(2), null, true, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        // create index defaults
        createIndex(firstIndex, policyID, aliasName)

        // Add a bunch of aliases
        changeAlias(
            firstIndex, "test_alias1", "add", routing = 0, searchRouting = 1, indexRouting = 2,
            filter = """
            { "term": { "user.id": "kimchy" } }
            """.trimIndent(),
            isHidden = false,
        )
        changeAlias(firstIndex, "test_alias2", "add")
        changeAlias(firstIndex, "test_alias3", "add")

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
                AttemptRolloverStep.getPendingMessage(firstIndex), info["message"],
            )
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 0, minDocCount["current"])
        }

        insertSampleData(index = firstIndex, docCount = 5, delay = 0)

        // Need to speed up to second execution where it will trigger the first execution of the action
        updateManagedIndexConfigStartTime(managedIndexConfig)
        val newIndex = "$indexNameBase-000002"
        waitFor {
            val metadata = getExplainManagedIndexMetaData(firstIndex)
            val info = metadata.info as Map<String, Any?>
            assertEquals("Index did not rollover", AttemptRolloverStep.getSuccessCopyAliasMessage(firstIndex, newIndex), info["message"])
            val conditions = info["conditions"] as Map<String, Any?>
            assertEquals(
                "Did not have exclusively min age and min doc count conditions",
                setOf(RolloverAction.MIN_INDEX_AGE_FIELD, RolloverAction.MIN_DOC_COUNT_FIELD), conditions.keys,
            )
            val minAge = conditions[RolloverAction.MIN_INDEX_AGE_FIELD] as Map<String, Any?>
            val minDocCount = conditions[RolloverAction.MIN_DOC_COUNT_FIELD] as Map<String, Any?>
            assertEquals("Did not have min age condition", "2d", minAge["condition"])
            assertTrue("Did not have min age current", minAge["current"] is String)
            assertEquals("Did not have min doc count condition", 3, minDocCount["condition"])
            assertEquals("Did not have min doc count current", 5, minDocCount["current"])
            assertEquals("Did not have rolled over index name", metadata.rolledOverIndexName, newIndex)
        }
        Assert.assertTrue("New rollover index does not exist.", indexExists(newIndex))

        // Check if new index has the aliases
        val alias = getAlias(newIndex, "")
        Assert.assertEquals(alias.containsKey("test_alias1"), true)
        alias["test_alias1"]!!.let {
            it as Map<String, Any>
            assertEquals(it["is_hidden"], false)
            assertEquals(it["search_routing"], "1")
            assertEquals(it["index_routing"], "2")
            assertEquals(it["filter"]!!.toString(), "{term={user.id=kimchy}}")
        }
        Assert.assertEquals(alias.containsKey("test_alias2"), true)
        Assert.assertEquals(alias.containsKey("test_alias3"), true)
    }

    fun `test rollover detects transient failure and continues executing`() {
        val aliasName = "${testIndexName}_alias"
        val indexNameBase = "${testIndexName}_index"
        val firstIndex = "$indexNameBase-1"
        val policyID = "${testIndexName}_testPolicyName_1"
        val actionConfig = RolloverAction(null, 1, null, null, false, 0)
        val states = listOf(State(name = "RolloverAction", actions = listOf(actionConfig), transitions = listOf()))
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
        createIndex(firstIndex, policyID, aliasName)

        val managedIndexConfig = getExistingManagedIndexConfig(firstIndex)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(firstIndex).policyID) }

        // Insert data to trigger rollover
        insertSampleData(index = firstIndex, docCount = 5, delay = 0)
        // Need to speed up to second execution where it will trigger the attempt rollover step
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val info = getExplainManagedIndexMetaData(firstIndex).info as Map<String, Any?>
            assertEquals("Index did not rollover.", AttemptRolloverStep.getSuccessMessage(firstIndex), info["message"])
        }
        // Manually produce transaction failure
        val response =
            adminClient().makeRequest(
                "POST", "$INDEX_MANAGEMENT_INDEX/_update/${managedIndexConfig.id}%23metadata",
                StringEntity(
                    "{\n" +
                        "    \"script\": {\n" +
                        "        \"source\": \"ctx._source.managed_index_metadata.step.step_status = params.step_status\",\n" +
                        "        \"lang\": \"painless\",\n" +
                        "        \"params\": {\n" +
                        "            \"step_status\": \"starting\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                    ContentType.APPLICATION_JSON,
                ),
            )
        assertEquals("Request failed", RestStatus.OK, response.restStatus())

        // Execute again to see the transaction failure
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(firstIndex)
            assertEquals("Executing the wrong step", "attempt_rollover", metadata.stepMetaData?.name)
            assertEquals("rollover step did not continue executing after detecting the transient failure.", Step.StepStatus.COMPLETED, metadata.stepMetaData?.stepStatus)
        }
    }
}
