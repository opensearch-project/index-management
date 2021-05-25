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

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.rollup.model.RollupMetadata
import org.opensearch.indexmanagement.rollup.model.RollupMetrics
import org.opensearch.indexmanagement.rollup.model.dimension.DateHistogram
import org.opensearch.indexmanagement.rollup.model.dimension.Terms
import org.opensearch.indexmanagement.rollup.model.metric.Average
import org.opensearch.indexmanagement.rollup.model.metric.Max
import org.opensearch.indexmanagement.rollup.model.metric.Min
import org.opensearch.indexmanagement.rollup.model.metric.Sum
import org.opensearch.indexmanagement.rollup.model.metric.ValueCount
import org.opensearch.indexmanagement.rollup.toJsonString
import org.opensearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RollupActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

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
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupActionConfig(rollup, 0)
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
        val actionConfig = RollupActionConfig(rollup, 0)
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
}
