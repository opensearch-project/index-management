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

package org.opensearch.indexmanagement.transform.resthandler

import org.junit.AfterClass
import org.junit.Before
import org.opensearch.client.ResponseException
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.makeRequest
import org.opensearch.indexmanagement.transform.TransformRestTestCase
import org.opensearch.indexmanagement.transform.randomTransform
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories

@Suppress("UNCHECKED_CAST")
class RestPreviewTransformActionIT : TransformRestTestCase() {

    private val factories = AggregatorFactories.builder()
        .addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))
        .addAggregator(AggregationBuilders.percentiles("passengerCount").field("passenger_count").percentiles(90.0, 95.0))
    private val transform = randomTransform().copy(
        sourceIndex = sourceIndex,
        groups = listOf(
            Terms(sourceField = "PULocationID", targetField = "location")
        ),
        aggregations = factories
    )

    @Before
    fun setupData() {
        var indexExists = false
        try {
            indexExists = indexExists(sourceIndex)
        } catch (e: IndexNotFoundException) {
        }
        if (!indexExists) {
            generateNYCTaxiData(sourceIndex)
        }
    }

    companion object {
        private const val sourceIndex = "transform-preview-api"

        @AfterClass
        @JvmStatic
        fun deleteData() {
            deleteIndex(sourceIndex)
        }
    }

    fun `test preview`() {
        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        val expectedKeys = setOf("revenue", "passengerCount", "location", "transform._doc_count")
        assertEquals("Preview transform failed", RestStatus.OK, response.restStatus())
        val transformedDocs = response.asMap()["documents"] as List<Map<String, Any>>
        assertEquals("Transformed docs have unexpected schema", expectedKeys, transformedDocs.first().keys)
    }

    // TODO: Not sure if we should validate on source indices instead of returning empty result.
    fun `test mismatched columns`() {
        val factories = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.sum("revenue").field("total_amountdzdfd"))
        val transform = transform.copy(
            groups = listOf(Terms(sourceField = "non-existent", targetField = "non-existent")),
            aggregations = factories
        )
        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        assertEquals("Unexpected status", RestStatus.OK, response.restStatus())
    }

    fun `test nonexistent source index`() {
        val transform = transform.copy(sourceIndex = "non-existent-index")
        try {
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/_preview",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("expected exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected failure code", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}
