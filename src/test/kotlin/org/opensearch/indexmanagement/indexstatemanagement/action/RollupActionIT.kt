/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.index.engine.EngineConfig
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
import org.opensearch.rest.RestRequest
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections.emptyMap
import java.util.Locale

class RollupActionIT : IndexStateManagementRestTestCase() {
    private val testIndexName = javaClass.simpleName.lowercase(Locale.ROOT)

    fun `test rollup action`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val rollup =
            ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            )
        val actionConfig = RollupAction(rollup, 0)
        val states =
            listOf(
                State("rollup", listOf(actionConfig), listOf()),
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        assertIndexRolledUp(indexName, policyID, rollup)
    }

    fun `test rollup action with specified target index settings`() {
        val indexName = "${testIndexName}_index_settings"
        val policyID = "${testIndexName}_policy_settings"
        val targetIdxTestName = "target_rollup_settings"
        val targetIndexReplicas = 0
        val targetIndexCodec = "best_compression"
        val rollup =
            ISMRollup(
                description = "basic search test",
                targetIndex = targetIdxTestName,
                targetIndexSettings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, targetIndexReplicas)
                    .put(EngineConfig.INDEX_CODEC_SETTING.key, targetIndexCodec)
                    .build(),
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                    RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min())),
                ),
            )
        val actionConfig = RollupAction(rollup, 0)
        val states =
            listOf(
                State("rollup", listOf(actionConfig), listOf()),
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        assertIndexRolledUp(indexName, policyID, rollup)
    }

    fun `test data stream rollup action`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollup_policy"

        val rollup =
            ISMRollup(
                description = "data stream rollup",
                targetIndex = "target_rollup_search",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                    RollupMetrics(
                        sourceField = "total_amount",
                        targetField = "total_amount",
                        metrics = listOf(Max(), Min()),
                    ),
                ),
            )

        // Create an ISM policy to rollup backing indices of a data stream.
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "data stream rollup policy",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
                ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))),
            )
        createPolicy(policy, policyID)

        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
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
                ContentType.APPLICATION_JSON,
            ),
        )
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")

        // Ensure rollup works on backing indices of a data stream.
        val indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        assertIndexRolledUp(indexName, policyID, rollup)
    }

    fun `test data stream rollup action with scripted targetIndex`() {
        val dataStreamName = "${testIndexName}_data_stream"
        val policyID = "${testIndexName}_rollup_policy"
        val rollup =
            ISMRollup(
                description = "data stream rollup",
                targetIndex = "rollup_{{ctx.source_index}}",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()),
                    ),
                    RollupMetrics(
                        sourceField = "total_amount",
                        targetField = "total_amount",
                        metrics = listOf(Max(), Min()),
                    ),
                ),
            )

        // Create an ISM policy to rollup backing indices of a data stream.
        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "data stream rollup policy",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
                ismTemplate = listOf(ISMTemplate(listOf(dataStreamName), 100, Instant.now().truncatedTo(ChronoUnit.MILLIS))),
            )
        createPolicy(policy, policyID)

        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
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
                ContentType.APPLICATION_JSON,
            ),
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
        val ismRollup =
            ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            )
        val rollup = ismRollup.toRollup(indexName)
        val rollupId = rollup.id
        val actionConfig = RollupAction(ismRollup, 0)
        val states =
            listOf(
                State("rollup", listOf(actionConfig), listOf()),
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time, so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup)
        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }

    fun `test rollup action create failure due to wildcards in target_index`() {
        val indexName = "${testIndexName}_index_failure"
        val policyID = "${testIndexName}_policy_failure"
        val rollup =
            ISMRollup(
                description = "basic search test",
                targetIndex = "target_with_wildcard*",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            )
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupAction(rollup, 0)
        val states =
            listOf(
                State("rollup", listOf(actionConfig), listOf()),
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
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
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time, so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }

    fun `test rollup action failure and retry failed step`() {
        val indexName = "${testIndexName}_index_retry"
        val policyID = "${testIndexName}_policy_retry"
        val ismRollup =
            ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                    Terms("PULocationID", "PULocationID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count", targetField = "passenger_count",
                        metrics =
                        listOf(
                            Sum(), Min(), Max(),
                            ValueCount(), Average(),
                        ),
                    ),
                ),
            )
        val rollup = ismRollup.toRollup(indexName)
        val rollupId = rollup.id
        val policyString =
            "{\"policy\":{\"description\":\"$testIndexName description\",\"default_state\":\"rollup\",\"states\":[{\"name\":\"rollup\"," +
                "\"actions\":[{\"retry\":{\"count\":2,\"backoff\":\"constant\",\"delay\":\"10ms\"},\"rollup\":{\"ism_rollup\":" +
                "${ismRollup.toJsonString()}}}],\"transitions\":[]}]}}"

        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
        createPolicyJson(policyString, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time, so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobProcessingMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        // Wait for rollup step job failed
        updateRollupStartTime(rollup)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobFailedMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }

    private fun assertIndexRolledUp(indexName: String, policyId: String, ismRollup: ISMRollup) {
        val rollup = ismRollup.toRollup(indexName)
        val rollupId = rollup.id
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so that the policy will be initialized.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so that the rollup action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup)
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollupId)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollupId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test multi-tier rollup from raw to 1m to 10m to 1h`() {
        val indexName = "${testIndexName}_multi_tier_raw"
        val policyID = "${testIndexName}_multi_tier_policy"
        val rollup1mTarget = "${testIndexName}_rollup_1m"
        val rollup10mTarget = "${testIndexName}_rollup_10m"
        val rollup1hTarget = "${testIndexName}_rollup_1h"

        val rollup1m = ISMRollup(
            description = "Rollup to 1m interval",
            targetIndex = rollup1mTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1m")),
            metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))),
        )

        val rollup10m = ISMRollup(
            description = "Rollup to 10m interval",
            targetIndex = rollup10mTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "10m")),
            metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))),
        )

        val rollup1h = ISMRollup(
            description = "Rollup to 1h interval",
            targetIndex = rollup1hTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h")),
            metrics = listOf(RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(), ValueCount(), Average()))),
        )

        val states = listOf(State("rollup_tier1_1m", listOf(RollupAction(rollup1m, 0)), listOf()))
        val policy = Policy(
            id = policyID,
            description = "Multi-tier rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        val mapping = "\"properties\": {\"tpep_pickup_datetime\": {\"type\": \"date\"}, \"passenger_count\": {\"type\": \"integer\"}}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = mapping)

        // Index some test data
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"tpep_pickup_datetime":"2021-01-01T00:05:00Z","passenger_count":2}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        val rollup1mId = rollup1m.toRollup(indexName).id
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollup1mId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup1m.toRollup(indexName))
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollup1mId)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("First tier rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollup1mId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message"),
            )
        }

        val policy2ID = "${policyID}_tier2"
        val states2 = listOf(State("rollup_tier2_10m", listOf(RollupAction(rollup10m, 0)), listOf()))
        val policy2 = Policy(
            id = policy2ID,
            description = "Tier 2 rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states2[0].name,
            states = states2,
        )
        createPolicy(policy2, policy2ID)
        addPolicyToIndex(rollup1mTarget, policy2ID)
        val managedIndexConfig2 = getExistingManagedIndexConfig(rollup1mTarget)
        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor { assertEquals(policy2ID, getExplainManagedIndexMetaData(rollup1mTarget).policyID) }

        val rollup10mId = rollup10m.toRollup(rollup1mTarget).id
        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollup10mId, rollup1mTarget),
                getExplainManagedIndexMetaData(rollup1mTarget).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup10m.toRollup(rollup1mTarget))
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollup10mId)
            assertNotNull("Second tier rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            if (rollupMetadata.status == RollupMetadata.Status.FAILED) {
                fail("Second tier rollup failed: ${rollupMetadata.failureReason}")
            }
            assertEquals("Second tier rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollup10mId, rollup1mTarget),
                getExplainManagedIndexMetaData(rollup1mTarget).info?.get("message"),
            )
        }

        assertIndexExists(rollup1mTarget)
        assertIndexExists(rollup10mTarget)

        val policy3ID = "${policyID}_tier3"
        val states3 = listOf(State("rollup_tier3_1h", listOf(RollupAction(rollup1h, 0)), listOf()))
        val policy3 = Policy(
            id = policy3ID,
            description = "Tier 3 rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states3[0].name,
            states = states3,
        )
        createPolicy(policy3, policy3ID)
        addPolicyToIndex(rollup10mTarget, policy3ID)
        val managedIndexConfig3 = getExistingManagedIndexConfig(rollup10mTarget)
        updateManagedIndexConfigStartTime(managedIndexConfig3)
        waitFor { assertEquals(policy3ID, getExplainManagedIndexMetaData(rollup10mTarget).policyID) }

        val rollup1hId = rollup1h.toRollup(rollup10mTarget).id
        updateManagedIndexConfigStartTime(managedIndexConfig3)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollup1hId, rollup10mTarget),
                getExplainManagedIndexMetaData(rollup10mTarget).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup1h.toRollup(rollup10mTarget))
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollup1hId)
            assertNotNull("Third tier rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            if (rollupMetadata.status == RollupMetadata.Status.FAILED) {
                fail("Third tier rollup failed: ${rollupMetadata.failureReason}")
            }
            assertEquals("Third tier rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }

        updateManagedIndexConfigStartTime(managedIndexConfig3)
        waitFor {
            assertEquals(
                WaitForRollupCompletionStep.getJobCompletionMessage(rollup1hId, rollup10mTarget),
                getExplainManagedIndexMetaData(rollup10mTarget).info?.get("message"),
            )
        }

        assertIndexExists(rollup1hTarget)

        // Validate all the metrics between source and 3rd level rollup index
        val aggReq = """
            {
                "size": 0,
                "query": { "match_all": {} },
                "aggs": {
                    "sum_passenger": { "sum": { "field": "passenger_count" } },
                    "min_passenger": { "min": { "field": "passenger_count" } },
                    "max_passenger": { "max": { "field": "passenger_count" } },
                    "value_count_passenger": { "value_count": { "field": "passenger_count" } },
                    "avg_passenger": { "avg": { "field": "passenger_count" } }
                }
            }
        """.trimIndent()

        val sourceResponse = client().makeRequest(RestRequest.Method.POST.name, "/$indexName/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))
        val rollupResponse = client().makeRequest(RestRequest.Method.POST.name, "/$rollup1hTarget/_search", emptyMap(), StringEntity(aggReq, ContentType.APPLICATION_JSON))

        val sourceAggs = sourceResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val rollupAggs = rollupResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>

        assertEquals("Sum should be consistent", sourceAggs["sum_passenger"]!!["value"], rollupAggs["sum_passenger"]!!["value"])
        assertEquals("Min should be consistent", sourceAggs["min_passenger"]!!["value"], rollupAggs["min_passenger"]!!["value"])
        assertEquals("Max should be consistent", sourceAggs["max_passenger"]!!["value"], rollupAggs["max_passenger"]!!["value"])
        assertEquals("Value count should be consistent", sourceAggs["value_count_passenger"]!!["value"], rollupAggs["value_count_passenger"]!!["value"])
        assertEquals("Average should be consistent", sourceAggs["avg_passenger"]!!["value"], rollupAggs["avg_passenger"]!!["value"])
    }
}
