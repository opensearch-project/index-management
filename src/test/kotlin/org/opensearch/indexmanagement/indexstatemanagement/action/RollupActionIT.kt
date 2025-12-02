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

    fun `test single-tier rollup with explicit source_index`() {
        val indexName = "${testIndexName}_single_tier_explicit_source"
        val sourceIndexName = "${testIndexName}_explicit_source_data"
        val policyID = "${testIndexName}_single_tier_explicit_policy"
        val targetIndexName = "${testIndexName}_single_tier_target"

        // Create source index with data
        val mapping = "\"properties\": {\"timestamp\": {\"type\": \"date\"}, \"value\": {\"type\": \"long\"}}"
        createIndex(sourceIndexName, null, mapping = mapping)

        // Index test data into source index
        client().makeRequest(
            "POST",
            "/$sourceIndexName/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:00:00Z","value":10}""",
                ContentType.APPLICATION_JSON,
            ),
        )
        client().makeRequest(
            "POST",
            "/$sourceIndexName/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:01:00Z","value":20}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Create rollup with explicit source_index
        val rollup = ISMRollup(
            description = "Rollup with explicit source_index",
            sourceIndex = sourceIndexName,
            targetIndex = targetIndexName,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "value",
                    targetField = "value",
                    metrics = listOf(Sum(), Min(), Max(), ValueCount()),
                ),
            ),
        )

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy = Policy(
            id = policyID,
            description = "Single-tier rollup with explicit source",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = mapping)

        // Execute rollup
        assertIndexRolledUp(indexName, policyID, rollup)

        // Verify the rollup job used the correct source index
        val rollupId = rollup.toRollup(indexName).id
        val rollupJob = getRollup(rollupId = rollupId)
        assertEquals("Rollup should use explicit source_index", sourceIndexName, rollupJob.sourceIndex)

        // Verify target index exists and has data
        assertIndexExists(targetIndexName)
    }

    fun `test single-tier rollup without source_index for backward compatibility`() {
        val indexName = "${testIndexName}_single_tier_backward_compat"
        val policyID = "${testIndexName}_single_tier_backward_policy"
        val targetIndexName = "${testIndexName}_single_tier_backward_target"

        // Create rollup WITHOUT source_index (backward compatibility)
        val rollup = ISMRollup(
            description = "Rollup without source_index for backward compatibility",
            targetIndex = targetIndexName,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "value",
                    targetField = "value",
                    metrics = listOf(Sum(), Min(), Max(), ValueCount()),
                ),
            ),
        )

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy = Policy(
            id = policyID,
            description = "Single-tier rollup backward compatibility",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        val mapping = "\"properties\": {\"timestamp\": {\"type\": \"date\"}, \"value\": {\"type\": \"long\"}}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = mapping)

        // Index test data into managed index
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:00:00Z","value":15}""",
                ContentType.APPLICATION_JSON,
            ),
        )
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:02:00Z","value":25}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Execute rollup
        assertIndexRolledUp(indexName, policyID, rollup)

        // Verify the rollup job used the managed index as source (backward compatibility)
        val rollupId = rollup.toRollup(indexName).id
        val rollupJob = getRollup(rollupId = rollupId)
        assertEquals("Rollup should use managed index as source when source_index not specified", indexName, rollupJob.sourceIndex)

        // Verify target index exists and has data
        assertIndexExists(targetIndexName)
    }

    fun `test single-tier rollup verifies correct source is used`() {
        val indexName = "${testIndexName}_verify_source"
        val explicitSourceIndex = "${testIndexName}_explicit_source"
        val policyID = "${testIndexName}_verify_source_policy"
        val targetIndexName = "${testIndexName}_verify_source_target"

        // Create explicit source index with specific data
        val mapping = "\"properties\": {\"timestamp\": {\"type\": \"date\"}, \"count\": {\"type\": \"long\"}}"
        createIndex(explicitSourceIndex, null, mapping = mapping)

        // Index data with value 100 in explicit source
        client().makeRequest(
            "POST",
            "/$explicitSourceIndex/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:00:00Z","count":100}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Create managed index with different data
        createIndex(indexName, null, mapping = mapping)

        // Index data with value 50 in managed index
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"timestamp":"2021-01-01T00:00:00Z","count":50}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Create rollup with explicit source_index pointing to explicitSourceIndex
        val rollup = ISMRollup(
            description = "Rollup to verify correct source usage",
            sourceIndex = explicitSourceIndex,
            targetIndex = targetIndexName,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1h")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "count",
                    targetField = "count",
                    metrics = listOf(Sum()),
                ),
            ),
        )

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy = Policy(
            id = policyID,
            description = "Verify correct source usage",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        createPolicy(policy, policyID)
        addPolicyToIndex(indexName, policyID)

        // Execute rollup
        assertIndexRolledUp(indexName, policyID, rollup)

        // Verify the rollup job used the correct source index
        val rollupId = rollup.toRollup(indexName).id
        val rollupJob = getRollup(rollupId = rollupId)
        assertEquals("Rollup should use explicit source_index", explicitSourceIndex, rollupJob.sourceIndex)

        // Verify target index exists
        assertIndexExists(targetIndexName)
    }

    @Suppress("UNCHECKED_CAST")
    fun `test 3-tier rollup with source_index field raw to 1m to 5m to 10m`() {
        val indexName = "${testIndexName}_3tier_raw"
        val policyID = "${testIndexName}_3tier_policy"
        val rollup1mTarget = "${testIndexName}_3tier_1m"
        val rollup5mTarget = "${testIndexName}_3tier_5m"
        val rollup10mTarget = "${testIndexName}_3tier_10m"

        // First tier: raw -> 1m
        val rollup1m = ISMRollup(
            description = "Rollup raw to 1m interval",
            targetIndex = rollup1mTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1m")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "count",
                    targetField = "count",
                    metrics = listOf(Sum(), Min(), Max(), ValueCount()),
                ),
            ),
        )

        val states = listOf(State("rollup_1m", listOf(RollupAction(rollup1m, 0)), listOf()))
        val policy = Policy(
            id = policyID,
            description = "First tier rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
        )

        val mapping = "\"properties\": {\"timestamp\": {\"type\": \"date\"}, \"count\": {\"type\": \"long\"}}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = mapping)

        // Index test data
        for (i in 0..9) {
            client().makeRequest(
                "POST",
                "/$indexName/_doc?refresh=true",
                StringEntity(
                    """{"timestamp":"2021-01-01T00:0$i:00Z","count":${i + 1}}""",
                    ContentType.APPLICATION_JSON,
                ),
            )
        }

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Execute first tier rollup (raw -> 1m)
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
            assertNotNull("First tier rollup job doesn't have metadata set", rollupJob.metadataID)
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

        assertIndexExists(rollup1mTarget)

        // Second tier: 1m -> 5m (using source_index field)
        val rollup5m = ISMRollup(
            description = "Rollup 1m to 5m interval",
            sourceIndex = rollup1mTarget,
            targetIndex = rollup5mTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "5m")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "count",
                    targetField = "count",
                    metrics = listOf(Sum(), Min(), Max()),
                ),
            ),
        )

        val policy2ID = "${policyID}_tier2"
        val states2 = listOf(State("rollup_5m", listOf(RollupAction(rollup5m, 0)), listOf()))
        val policy2 = Policy(
            id = policy2ID,
            description = "Second tier rollup policy",
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

        val rollup5mId = rollup5m.toRollup(rollup1mTarget).id
        updateManagedIndexConfigStartTime(managedIndexConfig2)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollup5mId, rollup1mTarget),
                getExplainManagedIndexMetaData(rollup1mTarget).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup5m.toRollup(rollup1mTarget))
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollup5mId)
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
                WaitForRollupCompletionStep.getJobCompletionMessage(rollup5mId, rollup1mTarget),
                getExplainManagedIndexMetaData(rollup1mTarget).info?.get("message"),
            )
        }

        assertIndexExists(rollup5mTarget)

        // Third tier: 5m -> 10m (using source_index field)
        val rollup10m = ISMRollup(
            description = "Rollup 5m to 10m interval",
            sourceIndex = rollup5mTarget,
            targetIndex = rollup10mTarget,
            targetIndexSettings = null,
            pageSize = 100,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "10m")),
            metrics = listOf(
                RollupMetrics(
                    sourceField = "count",
                    targetField = "count",
                    metrics = listOf(Sum()),
                ),
            ),
        )

        val policy3ID = "${policyID}_tier3"
        val states3 = listOf(State("rollup_10m", listOf(RollupAction(rollup10m, 0)), listOf()))
        val policy3 = Policy(
            id = policy3ID,
            description = "Third tier rollup policy",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states3[0].name,
            states = states3,
        )

        createPolicy(policy3, policy3ID)
        addPolicyToIndex(rollup5mTarget, policy3ID)
        val managedIndexConfig3 = getExistingManagedIndexConfig(rollup5mTarget)
        updateManagedIndexConfigStartTime(managedIndexConfig3)
        waitFor { assertEquals(policy3ID, getExplainManagedIndexMetaData(rollup5mTarget).policyID) }

        val rollup10mId = rollup10m.toRollup(rollup5mTarget).id
        updateManagedIndexConfigStartTime(managedIndexConfig3)
        waitFor {
            assertEquals(
                AttemptCreateRollupJobStep.getSuccessMessage(rollup10mId, rollup5mTarget),
                getExplainManagedIndexMetaData(rollup5mTarget).info?.get("message"),
            )
        }

        updateRollupStartTime(rollup10m.toRollup(rollup5mTarget))
        waitFor(timeout = Instant.ofEpochSecond(60)) {
            val rollupJob = getRollup(rollupId = rollup10mId)
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
                WaitForRollupCompletionStep.getJobCompletionMessage(rollup10mId, rollup5mTarget),
                getExplainManagedIndexMetaData(rollup5mTarget).info?.get("message"),
            )
        }

        assertIndexExists(rollup10mTarget)

        // Verify the rollup jobs used the correct source indices
        val rollup5mJob = getRollup(rollupId = rollup5mId)
        assertEquals("Second tier rollup should use 1m rollup as source", rollup1mTarget, rollup5mJob.sourceIndex)

        val rollup10mJob = getRollup(rollupId = rollup10mId)
        assertEquals("Third tier rollup should use 5m rollup as source", rollup5mTarget, rollup10mJob.sourceIndex)

        // Verify data flows correctly through all tiers
        val aggReq = """
            {
                "size": 0,
                "query": { "match_all": {} },
                "aggs": {
                    "sum_count": { "sum": { "field": "count" } }
                }
            }
        """.trimIndent()

        val sourceResponse = client().makeRequest(
            RestRequest.Method.POST.name,
            "/$indexName/_search",
            emptyMap(),
            StringEntity(aggReq, ContentType.APPLICATION_JSON),
        )
        val tier1Response = client().makeRequest(
            RestRequest.Method.POST.name,
            "/$rollup1mTarget/_search",
            emptyMap(),
            StringEntity(aggReq, ContentType.APPLICATION_JSON),
        )
        val tier2Response = client().makeRequest(
            RestRequest.Method.POST.name,
            "/$rollup5mTarget/_search",
            emptyMap(),
            StringEntity(aggReq, ContentType.APPLICATION_JSON),
        )
        val tier3Response = client().makeRequest(
            RestRequest.Method.POST.name,
            "/$rollup10mTarget/_search",
            emptyMap(),
            StringEntity(aggReq, ContentType.APPLICATION_JSON),
        )

        val sourceAggs = sourceResponse.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val tier1Aggs = tier1Response.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val tier2Aggs = tier2Response.asMap()["aggregations"] as Map<String, Map<String, Any>>
        val tier3Aggs = tier3Response.asMap()["aggregations"] as Map<String, Map<String, Any>>

        val sourceSum = sourceAggs["sum_count"]!!["value"]
        val tier1Sum = tier1Aggs["sum_count"]!!["value"]
        val tier2Sum = tier2Aggs["sum_count"]!!["value"]
        val tier3Sum = tier3Aggs["sum_count"]!!["value"]

        assertEquals("Sum should be consistent between raw and 1m tier", sourceSum, tier1Sum)
        assertEquals("Sum should be consistent between 1m and 5m tier", tier1Sum, tier2Sum)
        assertEquals("Sum should be consistent between 5m and 10m tier", tier2Sum, tier3Sum)
    }

    fun `test rollup action with source_index template using ctx index`() {
        val indexName = "${testIndexName}_template_source_ctx_index"
        val policyID = "${testIndexName}_policy_template_source"
        val rollup =
            ISMRollup(
                description = "rollup with source_index template",
                sourceIndex = "{{ctx.index}}",
                targetIndex = "rollup_{{ctx.index}}",
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

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "rollup policy with source_index template",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        // Index test data
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"tpep_pickup_datetime":"2021-01-01T00:00:00Z","RatecodeID":"1","PULocationID":"100","passenger_count":2,"total_amount":15.5}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Execute the rollup action and wait for completion
        assertIndexRolledUp(indexName, policyID, rollup)

        // Verify the rollup index was created with the correct name
        assertIndexExists("rollup_$indexName")
    }

    fun `test rollup action with both source_index and target_index templates`() {
        val indexName = "${testIndexName}_template_both_fields"
        val policyID = "${testIndexName}_policy_template_both"
        val rollup =
            ISMRollup(
                description = "rollup with both templates",
                sourceIndex = "{{ctx.index}}",
                targetIndex = "target_{{ctx.index}}_rollup",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Average()),
                    ),
                ),
            )

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "rollup policy with both templates",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        // Index test data
        client().makeRequest(
            "POST",
            "/$indexName/_doc?refresh=true",
            StringEntity(
                """{"tpep_pickup_datetime":"2021-01-01T00:00:00Z","RatecodeID":"1","passenger_count":3}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Execute the rollup action and wait for completion
        assertIndexRolledUp(indexName, policyID, rollup)

        // Verify the rollup index was created with the correct name
        assertIndexExists("target_${indexName}_rollup")
    }

    fun `test multi-tier rollup with templated source_index`() {
        val rawIndexName = "${testIndexName}_multi_tier_raw"
        val tier1PolicyID = "${testIndexName}_tier1_policy"
        val tier2PolicyID = "${testIndexName}_tier2_policy"

        // First tier rollup: raw data -> tier1 rollup
        val tier1Rollup =
            ISMRollup(
                description = "tier 1 rollup",
                targetIndex = "rollup_tier1_{{ctx.index}}",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                    Terms("RatecodeID", "RatecodeID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum(), Average()),
                    ),
                ),
            )

        val tier1ActionConfig = RollupAction(tier1Rollup, 0)
        val tier1States = listOf(State("rollup", listOf(tier1ActionConfig), listOf()))
        val tier1Policy =
            Policy(
                id = tier1PolicyID,
                description = "tier 1 rollup policy",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = tier1States[0].name,
                states = tier1States,
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }}"
        createPolicy(tier1Policy, tier1PolicyID)
        createIndex(rawIndexName, tier1PolicyID, mapping = sourceIndexMappingString)

        // Index test data
        client().makeRequest(
            "POST",
            "/$rawIndexName/_doc?refresh=true",
            StringEntity(
                """{"tpep_pickup_datetime":"2021-01-01T00:00:00Z","RatecodeID":"1","passenger_count":5}""",
                ContentType.APPLICATION_JSON,
            ),
        )

        // Execute tier 1 rollup and wait for completion
        assertIndexRolledUp(rawIndexName, tier1PolicyID, tier1Rollup)

        val tier1TargetIndex = "rollup_tier1_$rawIndexName"
        assertIndexExists(tier1TargetIndex)

        // Second tier rollup: tier1 rollup -> tier2 rollup with templated source_index
        val tier2Rollup =
            ISMRollup(
                description = "tier 2 rollup with templated source",
                sourceIndex = "{{ctx.index}}",
                targetIndex = "rollup_tier2_{{ctx.index}}",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "2h"),
                    Terms("RatecodeID", "RatecodeID"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum()),
                    ),
                ),
            )

        val tier2ActionConfig = RollupAction(tier2Rollup, 0)
        val tier2States = listOf(State("rollup", listOf(tier2ActionConfig), listOf()))
        val tier2Policy =
            Policy(
                id = tier2PolicyID,
                description = "tier 2 rollup policy with templated source",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = tier2States[0].name,
                states = tier2States,
            )
        createPolicy(tier2Policy, tier2PolicyID)

        // Add policy to tier1 rollup index and execute tier 2 rollup
        addPolicyToIndex(tier1TargetIndex, tier2PolicyID)
        assertIndexRolledUp(tier1TargetIndex, tier2PolicyID, tier2Rollup)

        // Verify tier 2 rollup index was created
        assertIndexExists("rollup_tier2_$tier1TargetIndex")
    }

    fun `test rollup action fails when source equals target after resolution`() {
        val indexName = "${testIndexName}_source_equals_target"
        val policyID = "${testIndexName}_policy_source_equals_target"
        val rollup =
            ISMRollup(
                description = "rollup with source equals target",
                sourceIndex = "{{ctx.index}}",
                targetIndex = "{{ctx.index}}",
                targetIndexSettings = null,
                pageSize = 100,
                dimensions =
                listOf(
                    DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                ),
                metrics =
                listOf(
                    RollupMetrics(
                        sourceField = "passenger_count",
                        targetField = "passenger_count",
                        metrics = listOf(Sum()),
                    ),
                ),
            )

        val actionConfig = RollupAction(rollup, 0)
        val states = listOf(State("rollup", listOf(actionConfig), listOf()))
        val policy =
            Policy(
                id = policyID,
                description = "rollup policy with source equals target",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states,
            )
        val sourceIndexMappingString =
            "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"passenger_count\": { \"type\": \"integer\" }}"
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Speed up to second execution
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            val metadata = getExplainManagedIndexMetaData(indexName)
            assertEquals("Step should fail", AttemptCreateRollupJobStep.name, metadata.stepMetaData?.name)
            assertEquals("Step should be in failed state", "failed", metadata.stepMetaData?.stepStatus.toString())
            val info = metadata.info as? Map<String, Any?>
            val cause = info?.get("cause")?.toString() ?: ""
            assertTrue(
                "Error message should indicate source and target must be different, got: $cause",
                cause.contains("Source and target") || cause.contains("same index") || cause.contains("cannot be the same"),
            )
        }
    }
}
