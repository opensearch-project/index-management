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
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.rollup.model.RollupJobValidationResult
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import java.time.Instant

class RollupMapperServiceTests : OpenSearchTestCase() {
    fun `test source index validation`() {
        val sourceIndex = "test-index"

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "order_date",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "total_quantity",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex),
                        getMappingsException = null,
                    ),
                ),
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

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "order_date",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "total_quantity",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex),
                        getMappingsException = null,
                    ),
                ),
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

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "order_date",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "total_quantity",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex, true),
                        getMappingsException = null,
                    ),
                ),
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

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "category.keyword",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "total_quantity",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex),
                        getMappingsException = null,
                    ),
                ),
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

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "order_date",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "products.quantity",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex),
                        getMappingsException = null,
                    ),
                ),
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

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "nonexistent_field",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = emptyList(),
            )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponse(sourceIndex),
                        getMappingsException = null,
                    ),
                ),
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

    fun `test source rollup index validation with only HLL mappings and no dimension fields`() {
        val sourceIndex = "rollup-index-1"

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "timestamp",
                ),
                randomTerms().copy(
                    sourceField = "category",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "value",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        // Create mapping with only HLL field (no dimension fields)
        // This simulates an empty rollup index where HLL mappings were added upfront but no data has been indexed yet
        val mappingWithOnlyHLL = mapOf(
            "properties" to mapOf(
                "value" to mapOf(
                    "properties" to mapOf(
                        "hll" to mapOf(
                            "type" to "hll",
                            "precision" to 18,
                            "doc_values" to true,
                        ),
                    ),
                ),
            ),
        )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponseWithCustomMapping(sourceIndex, mappingWithOnlyHLL),
                        getMappingsException = null,
                    ),
                ),
            )

        // Mock cluster service to indicate this is a rollup index
        val clusterService = getRollupClusterService(sourceIndex)
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            // Should be Valid because empty rollup indices with only HLL mappings should skip validation
            require(sourceIndexValidationResult is RollupJobValidationResult.Valid) {
                "Source rollup index with only HLL mappings should be valid, but got: $sourceIndexValidationResult"
            }
        }
    }

    fun `test source rollup index validation with HLL and some dimension fields`() {
        val sourceIndex = "rollup-index-1"

        val dimensions =
            listOf(
                randomDateHistogram().copy(
                    sourceField = "timestamp",
                ),
                randomTerms().copy(
                    sourceField = "category",
                ),
            )
        val metrics =
            listOf(
                randomRollupMetrics().copy(
                    sourceField = "value",
                ),
            )
        val rollup =
            randomRollup().copy(
                enabled = true,
                jobEnabledTime = Instant.now(),
                dimensions = dimensions,
                metrics = metrics,
            )

        // Create mapping with HLL field AND one dimension field (partial data)
        val mappingWithPartialData = mapOf(
            "properties" to mapOf(
                "value" to mapOf(
                    "properties" to mapOf(
                        "hll" to mapOf(
                            "type" to "hll",
                            "precision" to 18,
                            "doc_values" to true,
                        ),
                    ),
                ),
                "timestamp" to mapOf(
                    "properties" to mapOf(
                        "date_histogram" to mapOf(
                            "type" to "date",
                        ),
                    ),
                ),
                // Note: "category" dimension field is missing
            ),
        )

        val client =
            getClient(
                getAdminClient(
                    getIndicesAdminClient(
                        getMappingsResponse = getMappingResponseWithCustomMapping(sourceIndex, mappingWithPartialData),
                        getMappingsException = null,
                    ),
                ),
            )

        // Mock cluster service to indicate this is a rollup index
        val clusterService = getRollupClusterService(sourceIndex)
        val indexNameExpressionResolver = getIndexNameExpressionResolver(listOf(sourceIndex))
        val mapperService = RollupMapperService(client, clusterService, indexNameExpressionResolver)

        runBlocking {
            val sourceIndexValidationResult = mapperService.isSourceIndexValid(rollup)
            // Should be Invalid because dimension field "category" is missing
            require(sourceIndexValidationResult is RollupJobValidationResult.Invalid) {
                "Source rollup index with missing dimension fields should be invalid, but got: $sourceIndexValidationResult"
            }
            assertTrue(
                "Error message should mention missing field",
                sourceIndexValidationResult.reason.contains("missing field category"),
            )
        }
    }

    // TODO: Test scenarios:
    //  - Source index validation when GetMappings throws an exception
    //  - Source index validation for correct/incorrect field types for different dimensions/metrics when that is added

    private fun getClient(adminClient: AdminClient): Client = mock { on { admin() } doReturn adminClient }

    private fun getAdminClient(indicesAdminClient: IndicesAdminClient): AdminClient = mock { on { indices() } doReturn indicesAdminClient }

    private fun getIndicesAdminClient(
        getMappingsResponse: GetMappingsResponse?,
        getMappingsException: Exception?,
    ): IndicesAdminClient {
        assertTrue(
            "Must provide either a getMappingsResponse or getMappingsException",
            (getMappingsResponse != null).xor(getMappingsException != null),
        )

        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetMappingsResponse>>(1)
                if (getMappingsResponse != null) {
                    listener.onResponse(getMappingsResponse)
                } else {
                    listener.onFailure(getMappingsException)
                }
            }.whenever(this.mock).getMappings(any(), any())
        }
    }

    private fun getClusterService(): ClusterService {
        val mockMetadata = mock<Metadata>()
        val mockClusterState = mock<ClusterState> {
            on { metadata } doReturn mockMetadata
        }
        return mock {
            on { state() } doReturn mockClusterState
        }
    }

    private fun getRollupClusterService(rollupIndexName: String): ClusterService {
        val mockIndexMetadata = mock<org.opensearch.cluster.metadata.IndexMetadata> {
            on { settings } doReturn org.opensearch.common.settings.Settings.builder()
                .put(org.opensearch.indexmanagement.rollup.settings.RollupSettings.ROLLUP_INDEX.key, true)
                .build()
        }
        val mockMetadata = mock<Metadata> {
            on { index(rollupIndexName) } doReturn mockIndexMetadata
        }
        val mockClusterState = mock<ClusterState> {
            on { metadata } doReturn mockMetadata
        }
        return mock {
            on { state() } doReturn mockClusterState
        }
    }

    // Returns a mocked IndexNameExpressionResolver which returns the provided list of concrete indices
    private fun getIndexNameExpressionResolver(concreteIndices: List<String>): IndexNameExpressionResolver =
        mock { on { concreteIndexNames(any(), any(), anyBoolean(), anyVararg<String>()) } doReturn concreteIndices.toTypedArray() }

    private fun getMappingResponse(indexName: String, emptyMapping: Boolean = false): GetMappingsResponse {
        val mappings =
            if (emptyMapping) {
                mapOf<String, MappingMetadata>()
            } else {
                val mappingSourceMap =
                    createParser(
                        XContentType.JSON.xContent(),
                        javaClass.classLoader.getResource("mappings/kibana-sample-data.json").readText(),
                    ).map()
                val mappingMetadata = MappingMetadata("_doc", mappingSourceMap) // it seems it still expects a type, i.e. _doc now
                mapOf(indexName to mappingMetadata)
            }

        return GetMappingsResponse(mappings)
    }

    private fun getMappingResponseWithCustomMapping(indexName: String, mappingSourceMap: Map<String, Any>): GetMappingsResponse {
        val mappingMetadata = MappingMetadata("_doc", mappingSourceMap)
        val mappings = mapOf(indexName to mappingMetadata)
        return GetMappingsResponse(mappings)
    }
}
