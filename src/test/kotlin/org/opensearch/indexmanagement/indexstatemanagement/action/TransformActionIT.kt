/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.opensearch.cluster.metadata.DataStream
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import org.opensearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.State
import org.opensearch.indexmanagement.indexstatemanagement.randomErrorNotification
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.AttemptCreateTransformJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.WaitForTransformCompletionStep
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.transform.avgAggregation
import org.opensearch.indexmanagement.transform.maxAggregation
import org.opensearch.indexmanagement.transform.minAggregation
import org.opensearch.indexmanagement.transform.model.ISMTransform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.sumAggregation
import org.opensearch.indexmanagement.transform.valueCountAggregation
import org.opensearch.indexmanagement.waitFor
import org.opensearch.search.aggregations.AggregatorFactories
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class TransformActionIT : IndexStateManagementRestTestCase() {

    private val testPrefix = javaClass.simpleName.lowercase(Locale.ROOT)

    companion object {
        const val SOURCE_INDEX_MAPPING = """
              "properties": {
                "timestamp": {
                  "type": "date",
                  "format": "strict_date_optional_time||epoch_millis"
                },
                "category": {
                  "type": "keyword"
                },
                "value": {
                  "type": "long"
                }
              }
        """
    }

    fun `test transform action`() {
        val indexName = "${testPrefix}_index_basic"
        val targetIndex = "${testPrefix}_target"
        val policyId = "${testPrefix}_policy_basic"

        val ismTransform = prepareISMTransform(targetIndex)
        val policy = preparePolicyContainingTransform(indexName, ismTransform, policyId)
        createPolicy(policy, policyId)
        createIndex(indexName, policyId, mapping = SOURCE_INDEX_MAPPING)

        assertIndexTransformSucceeded(indexName, policyId, ismTransform)
    }

    fun `test data stream transform action`() {
        val dataStreamName = "${testPrefix}_data_stream"
        val targetIndex = "${testPrefix}_target_data_stream"
        val policyId = "${testPrefix}_policy_data_stream"

        val ismTransform = prepareISMTransform(targetIndex)
        val policy = preparePolicyContainingTransform(dataStreamName, ismTransform, policyId)
        createPolicy(policy, policyId)
        createDataStream(dataStreamName)

        // assert transform works on backing indices of a data stream
        val indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1L)
        assertIndexTransformSucceeded(indexName, policyId, ismTransform)
    }

    fun `test transform action failure due to wrong source field`() {
        val indexName = "${testPrefix}_index_failure"
        val targetIndex = "${testPrefix}_target_failure"
        val policyId = "${testPrefix}_policy_failure"

        val ismTransform = ISMTransform(
            description = "test transform",
            targetIndex = targetIndex,
            pageSize = 100,
            dataSelectionQuery = MatchAllQueryBuilder(),
            groups = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1d"),
                Terms(sourceField = "wrong_field", targetField = "wrong_field")
            ),
            aggregations = AggregatorFactories.builder()
                .addAggregator(sumAggregation())
                .addAggregator(maxAggregation())
                .addAggregator(minAggregation())
                .addAggregator(avgAggregation())
                .addAggregator(valueCountAggregation())
        )
        val policy = preparePolicyContainingTransform(indexName, ismTransform, policyId)
        createPolicy(policy, policyId)
        createIndex(indexName, policyId, mapping = SOURCE_INDEX_MAPPING)

        assertIndexTransformFailedInAttemptCreateTransformStep(indexName, policyId, ismTransform)
    }

    fun `test transform action failed step got retried`() {
        val indexName = "${testPrefix}_index_retry"
        val targetIndex = "${testPrefix}_target_retry"
        val policyId = "${testPrefix}_policy_retry"

        val ismTransform = ISMTransform(
            description = "test transform",
            targetIndex = targetIndex,
            pageSize = 100,
            dataSelectionQuery = MatchAllQueryBuilder(),
            groups = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1d"),
                Terms(sourceField = "wrong_field", targetField = "wrong_field")
            ),
            aggregations = AggregatorFactories.builder()
                .addAggregator(sumAggregation())
                .addAggregator(maxAggregation())
                .addAggregator(minAggregation())
                .addAggregator(avgAggregation())
                .addAggregator(valueCountAggregation())
        )
        val policy = preparePolicyContainingTransform(indexName, ismTransform, policyId, retry = 1)
        createPolicy(policy, policyId)
        createIndex(indexName, policyId, mapping = SOURCE_INDEX_MAPPING)

        assertIndexTransformFailedInAttemptCreateTransformStep(indexName, policyId, ismTransform)

        // verify the wait for transform completion step will be retried and failed again.
        Thread.sleep(60000)
        updateManagedIndexConfigStartTime(getExistingManagedIndexConfig(indexName))
        waitFor {
            assertEquals(
                AttemptCreateTransformJobStep.getFailedMessage(ismTransform.toTransform(indexName).id, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    // create an ISMTransform that matches SOURCE_INDEX_MAPPING
    private fun prepareISMTransform(targetIndex: String): ISMTransform {
        return ISMTransform(
            description = "test transform",
            targetIndex = targetIndex,
            pageSize = 100,
            dataSelectionQuery = MatchAllQueryBuilder(),
            groups = listOf(
                DateHistogram(sourceField = "timestamp", fixedInterval = "1d"),
                Terms(sourceField = "category", targetField = "category")
            ),
            aggregations = AggregatorFactories.builder()
                .addAggregator(sumAggregation())
                .addAggregator(maxAggregation())
                .addAggregator(minAggregation())
                .addAggregator(avgAggregation())
                .addAggregator(valueCountAggregation())
        )
    }

    private fun preparePolicyContainingTransform(indexName: String, ismTransform: ISMTransform, policyId: String, retry: Long = 0): Policy {
        val actionConfig = TransformAction(ismTransform, 0)
        actionConfig.configRetry = ActionRetry(retry)
        val states = listOf(
            State("transform", listOf(actionConfig), listOf())
        )
        return Policy(
            id = policyId,
            description = "test description",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            errorNotification = randomErrorNotification(),
            defaultState = states[0].name,
            states = states,
            ismTemplate = listOf(
                ISMTemplate(
                    indexPatterns = listOf(indexName),
                    priority = 100,
                    lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS)
                )
            )
        )
    }

    private fun createDataStream(dataStreamName: String) {
        // create an index template for data stream
        client().makeRequest(
            "PUT",
            "/_index_template/${dataStreamName}_template",
            StringEntity(
                "{ " +
                    "\"index_patterns\": [ \"$dataStreamName\" ], " +
                    "\"data_stream\": { \"timestamp_field\": { \"name\": \"timestamp\" } }, " +
                    "\"template\": { \"mappings\": { $SOURCE_INDEX_MAPPING } } }",
                ContentType.APPLICATION_JSON
            )
        )
        // create data stream
        client().makeRequest("PUT", "/_data_stream/$dataStreamName")
    }

    private fun assertIndexTransformSucceeded(indexName: String, policyId: String, ismTransform: ISMTransform) {
        val transformId = ismTransform.toTransform(indexName).id
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so that the policy will be initialized.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so that the transform action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateTransformJobStep.getSuccessMessage(transformId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so that the transform action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                WaitForTransformCompletionStep.getJobCompletionMessage(transformId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        val transformJob = getTransform(transformId = transformId)
        waitFor {
            assertNotNull("Transform job doesn't have metadata set", transformJob.metadataId)
            val transformMetadata = getTransformMetadata(transformJob.metadataId!!)
            assertEquals("Transform is not finished", TransformMetadata.Status.FINISHED, transformMetadata.status)
        }
    }

    private fun assertIndexTransformFailedInAttemptCreateTransformStep(indexName: String, policyId: String, ismTransform: ISMTransform) {
        val transformId = ismTransform.toTransform(indexName).id
        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so that the policy will be initialized.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyId, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so that the transform action will be attempted.
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                AttemptCreateTransformJobStep.getFailedMessage(transformId, indexName),
                getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }
}
