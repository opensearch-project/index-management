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

package org.opensearch.indexmanagement.transform

import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomSchedule
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.rollup.randomAfterKey
import org.opensearch.indexmanagement.rollup.randomDimension
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.TransformStats
import java.util.Locale
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.indexmanagement.transform.model.ExplainTransform
import org.opensearch.search.SearchModule
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.test.rest.OpenSearchRestTestCase


fun randomGroups(): List<Dimension> {
    val dimensions = mutableListOf<Dimension>()
    for (i in 0..OpenSearchRestTestCase.randomIntBetween(1, 10)) {
        dimensions.add(randomDimension())
    }
    return dimensions
}

fun sumAggregation(): AggregationBuilder = AggregationBuilders.sum(
    OpenSearchRestTestCase.randomAlphaOfLength(10)).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun maxAggregation(): AggregationBuilder = AggregationBuilders.max(
    OpenSearchRestTestCase.randomAlphaOfLength(10)).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun minAggregation(): AggregationBuilder = AggregationBuilders.min(
    OpenSearchRestTestCase.randomAlphaOfLength(10)).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun valueCountAggregation(): AggregationBuilder = AggregationBuilders.count(
    OpenSearchRestTestCase.randomAlphaOfLength(10)).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun avgAggregation(): AggregationBuilder = AggregationBuilders.avg(
    OpenSearchRestTestCase.randomAlphaOfLength(10)).field(OpenSearchRestTestCase.randomAlphaOfLength(10))

fun randomAggregationBuilder(): AggregationBuilder {
    val aggregations = listOf(sumAggregation(), maxAggregation(), minAggregation(), valueCountAggregation(), avgAggregation())
    return OpenSearchRestTestCase.randomSubsetOf(1, aggregations).first()
}

fun randomAggregationFactories(): AggregatorFactories.Builder {
    val factories = AggregatorFactories.builder()
    for (i in 1..OpenSearchRestTestCase.randomIntBetween(1, 10)) {
        factories.addAggregator(randomAggregationBuilder())
    }
    return factories
}

fun randomTransform(): Transform {
    val enabled = OpenSearchRestTestCase.randomBoolean()
    return Transform(
        id = OpenSearchRestTestCase.randomAlphaOfLength(10),
        seqNo = OpenSearchRestTestCase.randomNonNegativeLong(),
        primaryTerm = OpenSearchRestTestCase.randomNonNegativeLong(),
        schemaVersion = OpenSearchRestTestCase.randomLongBetween(1, 1000),
        jobSchedule = randomSchedule(),
        metadataId = if (OpenSearchRestTestCase.randomBoolean()) null else OpenSearchRestTestCase.randomAlphaOfLength(10),
        updatedAt = randomInstant(),
        enabled = enabled,
        enabledAt = if (enabled) randomInstant() else null,
        description = OpenSearchRestTestCase.randomAlphaOfLength(10),
        sourceIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        targetIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
        roles = OpenSearchRestTestCase.randomList(10) { OpenSearchRestTestCase.randomAlphaOfLength(10) },
        pageSize = OpenSearchRestTestCase.randomIntBetween(1, 10000),
        groups = randomGroups(),
        aggregations = randomAggregationFactories()
    )
}

fun randomTransformMetadata(): TransformMetadata {
    val status = randomTransformMetadataStatus()
    return TransformMetadata(
        id = OpenSearchRestTestCase.randomAlphaOfLength(10),
        seqNo = OpenSearchRestTestCase.randomNonNegativeLong(),
        primaryTerm = OpenSearchRestTestCase.randomNonNegativeLong(),
        transformId = OpenSearchRestTestCase.randomAlphaOfLength(10),
        afterKey = randomAfterKey(),
        lastUpdatedAt = randomInstant(),
        status = status,
        failureReason = if (status == TransformMetadata.Status.FAILED) OpenSearchRestTestCase.randomAlphaOfLength(10) else null,
        stats = randomTransformStats()
    )
}

fun randomTransformStats(): TransformStats {
    return TransformStats(
        pagesProcessed = OpenSearchRestTestCase.randomNonNegativeLong(),
        documentsProcessed = OpenSearchRestTestCase.randomNonNegativeLong(),
        documentsIndexed = OpenSearchRestTestCase.randomNonNegativeLong(),
        indexTimeInMillis = OpenSearchRestTestCase.randomNonNegativeLong(),
        searchTimeInMillis = OpenSearchRestTestCase.randomNonNegativeLong()
    )
}

fun randomTransformMetadataStatus(): TransformMetadata.Status {
    return OpenSearchRestTestCase.randomFrom(TransformMetadata.Status.values().toList())
}

fun randomExplainTransform(): ExplainTransform {
    val metadata = randomTransformMetadata()
    return ExplainTransform(metadataID = metadata.id, metadata = metadata)
}

fun Transform.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params).string()

fun TransformMetadata.toJsonString(params: ToXContent.Params = ToXContent.EMPTY_PARAMS): String = this.toXContent(XContentFactory.jsonBuilder(), params)
    .string()

// Builds the required stream input for transforms by wrapping the stream input with required NamedWriteableRegistry.
fun buildStreamInputForTransforms(out: BytesStreamOutput): NamedWriteableAwareStreamInput {
    val namedWriteableRegistry = NamedWriteableRegistry(SearchModule(Settings.EMPTY, false, emptyList()).namedWriteables)
    return NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
}
