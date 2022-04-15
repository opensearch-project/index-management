/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.anyVararg
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.anyBoolean
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.client.AdminClient
import org.opensearch.client.Client
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.collect.ImmutableOpenMap
import org.opensearch.common.xcontent.XContentType
import org.opensearch.indexmanagement.rollup.model.RollupJobValidationResult
import org.opensearch.test.OpenSearchTestCase
import java.time.Instant

class RollupMapperServiceTests : OpenSearchTestCase() {

    fun `test source index validation`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "order_date"
            )
        )
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "total_quantity"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex),
                    getMappingsException = null
                )
            )
        )

        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation with custom type`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "order_date"
            )
        )
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "total_quantity"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex),
                    getMappingsException = null
                )
            )
        )
        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation with empty mappings`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "order_date"
            )
        )
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "total_quantity"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex, true),
                    getMappingsException = null
                )
            )
        )
        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Invalid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation with subfield`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "category.keyword"
            )
        )
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "total_quantity"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex),
                    getMappingsException = null
                )
            )
        )

        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation with nested field`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "order_date"
            )
        )
        val metrics = listOf(
            randomRollupMetrics().copy(
                sourceField = "products.quantity"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = metrics
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex),
                    getMappingsException = null
                )
            )
        )

        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) { "Source index validation returned unexpected results" }
        }
    }

    fun `test source index validation when field is not in mapping`() {
        val sourceIndex = "test-index"

        val dimensions = listOf(
            randomDateHistogram().copy(
                sourceField = "nonexistent_field"
            )
        )
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            dimensions = dimensions,
            metrics = emptyList()
        )

        val client = getClient(
            getAdminClient(
                getIndicesAdminClient(
                    getMappingsResponse = getMappingResponse(sourceIndex),
                    getMappingsException = null
                )
            )
        )

        val clusterService = getClusterService()
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            require(sourceIndexValidationResult is RollupJobValidationResult.Invalid) { "Source index validation returned unexpected results" }

            assertEquals("Invalid mappings for index [$sourceIndex] because [missing field nonexistent_field]", sourceIndexValidationResult.reason)
        }
    }

    // TODO: Test scenarios:
    //  - Source index validation when GetMappings throws an exception
    //  - Source index validation for correct/incorrect field types for different dimensions/metrics when that is added

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(
        getMappingsResponse: GetMappingsResponse?,
        getMappingsException: Exception?
    ): IndicesAdminClient {
        assertTrue(
            "Must provide either a getMappingsResponse or getMappingsException",
            (getMappingsResponse != null).xor(getMappingsException != null)
        )

        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetMappingsResponse>>(1)
                if (getMappingsResponse != null) listener.onResponse(getMappingsResponse)
                else listener.onFailure(getMappingsException)
            }.whenever(this.mock).getMappings(any(), any())
        }
    }

    private fun getClusterService(): ClusterService = mock { on { state() } doReturn mock() }

    // Returns a mocked IndexNameExpressionResolver which returns the provided list of concrete indices
    private fun getIndexNameExpressionResolver(concreteIndices: List<String>): IndexNameExpressionResolver =
        mock { on { concreteIndexNames(any(), any(), anyBoolean(), anyVararg<String>()) } doReturn concreteIndices.toTypedArray() }

    private fun getMappingResponse(indexName: String, emptyMapping: Boolean = false): GetMappingsResponse {
        val mappings = if (emptyMapping) {
            ImmutableOpenMap.Builder<String, MappingMetadata>().build()
        } else {
            val mappingSourceMap = createParser(
                XContentType.JSON.xContent(),
                javaClass.classLoader.getResource("mappings/kibana-sample-data.json").readText()
            ).map()
            val mappingMetadata = MappingMetadata("_doc", mappingSourceMap) // it seems it still expects a type, i.e. _doc now
            ImmutableOpenMap.Builder<String, MappingMetadata>()
                .fPut(indexName, mappingMetadata)
                .build()
        }

        return GetMappingsResponse(mappings)
    }
}
