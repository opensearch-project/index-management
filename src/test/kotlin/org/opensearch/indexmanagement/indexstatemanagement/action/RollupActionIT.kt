/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.waitFor
import java.lang.Thread.sleep
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RollupActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test rollup action`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val rollup = ISMRollup(
            description = "basic search test",
            targetIndex = "target_rollup_search",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                ),
                RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
            )
        )
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(
            State("rollup", listOf(actionConfig), listOf())
        )
        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        assertIndexRolledUp(indexName, policyID, rollup)
    }

    fun `test data stream rollup action`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollup_policy"

        val rollup = ISMRollup(
            description = "data stream rollup",
            targetIndex = "target_rollup_search",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count",
                    targetField = "passenger_count",
                    metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())
                ),
                RollupMetrics(
                    sourceField = "total_amount",
                    targetField = "total_amount",
                    metrics = listOf(Max(), Min())
                )
            )
        )

        // Create an ISM policy to rollup backing indices of a data stream.
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy = Policy(
            id = policyID,
            description = "data stream rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS)))
        )
        createPolicy(policy, policyID)

        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"

        // Create an index template for a data stream with the given source index mapping.
        client().makeRequest(
            "PUT",
            "/_index_template/rollup-data-stream-template",
            StringEntity(
                "{ " +
                    "\"index_patterns\": [ \"$dataStreamName\" ], " +
                    "\"data_stream\": { \"timestamp_field\": { \"name\": \"tpep_pickup_datetime\" } }, " +
                    "\"template\": { \"mappings\": { $sourceIndexMappingString } } }",
                ContentType.APPLICATION_JSON
            )
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        // Ensure rollup works on backing indices of a data stream.
        val indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        assertIndexRolledUp(indexName, policyID, rollup)
    }

    fun `test data stream rollup action with scripted targetIndex`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollup_policy"

        sleep(10000)

        val rollup = ISMRollup(
            description = "data stream rollup",
            targetIndex = "rollup_{{ctx.source_index}}",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count",
                    targetField = "passenger_count",
                    metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average())
                ),
                RollupMetrics(
                    sourceField = "total_amount",
                    targetField = "total_amount",
                    metrics = listOf(Max(), Min())
                )
            )
        )

        // Create an ISM policy to rollup backing indices of a data stream.
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy = Policy(
            id = policyID,
            description = "data stream rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS)))
        )
        createPolicy(policy, policyID)

        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"

        // Create an index template for a data stream with the given source index mapping.
        client().makeRequest(
            "PUT",
            "/_index_template/rollup-data-stream-template",
            StringEntity(
                "{ " +
                    "\"index_patterns\": [ \"$dataStreamName\" ], " +
                    "\"data_stream\": { \"timestamp_field\": { \"name\": \"tpep_pickup_datetime\" } }, " +
                    "\"template\": { \"mappings\": { $sourceIndexMappingString } } }",
                ContentType.APPLICATION_JSON
            )
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        // Ensure rollup works on backing indices of a data stream.
        val indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        assertIndexRolledUp(indexName, policyID, rollup)
        assertIndexExists("rollup_$indexName")
    }

    fun `test rollup action failure`() {
        val indexName = "${testIndexName}_index_failure"
        val policyID = "${testIndexName}_policy_failure"
        val rollup = ISMRollup(
            description = "basic search test",
            targetIndex = "target_rollup_search",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        )
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(
            State("rollup", listOf(actionConfig), listOf())
        )
        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test rollup action create failure due to wildcards in target_index`() {
        val indexName = "${testIndexName}_index_failure"
        val policyID = "${testIndexName}_policy_failure"
        val rollup = ISMRollup(
            description = "basic search test",
            targetIndex = "target_with_wildcard*",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        )
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(
            State("rollup", listOf(actionConfig), listOf())
        )
        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test rollup action failure and retry failed step`() {
        val indexName = "${testIndexName}_index_retry"
        val policyID = "${testIndexName}_policy_retry"
        val rollup = ISMRollup(
            description = "basic search test",
            targetIndex = "target_rollup_search",
            pageSize = 100,
            dimensions = listOf(
                DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                Terms("RatecodeID", "RatecodeID"),
                Terms("PULocationID", "PULocationID")
            ),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "passenger_count", targetField = "passenger_count",
                    metrics = listOf(
                        Sum(), Min(), Max(),
                        ValueCount(), Average()
                    )
                )
            )
        )
        val rollupId = rollup.toRollup(indexName).id
        val policyString = "{\"policy\":{\"description\":\"$testIndexName description\",\"default_state\":\"rollup\",\"states\":[{\"name\":\"rollup\"," +
            "\"actions\":[{\"retry\":{\"count\":2,\"backoff\":\"constant\",\"delay\":\"10ms\"},\"rollup\":{\"ism_rollup\":" +
            "${rollup.toJsonString()}}}],\"transitions\":[]}]}}"

        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
            "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
            "{ \"type\": \"double\" }}"
        createPolicyJson(policyString, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobProcessingMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Wait for few seconds and change start time so wait for rollup step will execute again - job will be failed
        Thread.sleep(60000)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    private fun assertIndexRolledUp(indexName: String, policyId: String, rollup: ISMRollup) {
        val rollupId = rollup.toRollup(indexName).id
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so that the policy will be initialized.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so that the rollup action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so that the rollup action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        val rollupJob = getRollup(rollupId = rollupId)
        waitFor {
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }
}
