/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.index.Index
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.opensearchapi.string
import org.opensearch.indexmanagement.randomInstant
import org.opensearch.indexmanagement.randomSchedule
import org.opensearch.indexmanagement.randomUser
import org.opensearch.indexmanagement.rollup.randomAfterKey
import org.opensearch.indexmanagement.rollup.randomDimension
import org.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import org.opensearch.indexmanagement.transform.model.ExplainTransform
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.TransformStats
import org.opensearch.search.SearchModule
import org.opensearch.search.aggregations.AggregationBuilder
import org.opensearch.search.aggregations.AggregationBuilders
import org.opensearch.search.aggregations.AggregatorFactories
import org.opensearch.test.rest.OpenSearchRestTestCase
import java.util.Locale

fun randomGroups(): List<Dimension> {
    val dimensions = mutableListOf<Dimension>()
    repeat(OpenSearchRestTestCase.randomIntBetween(1, 10)) {
        dimensions.add(randomDimension())
    }
    return dimensions
}

fun sumAggregation(): AggregationBuilder = AggregationBuilders.sum(
    OpenSearchRestTestCase.randomAlphaOfLength(10)
).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun maxAggregation(): AggregationBuilder = AggregationBuilders.max(
    OpenSearchRestTestCase.randomAlphaOfLength(10)
).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun minAggregation(): AggregationBuilder = AggregationBuilders.min(
    OpenSearchRestTestCase.randomAlphaOfLength(10)
).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun valueCountAggregation(): AggregationBuilder = AggregationBuilders.count(
    OpenSearchRestTestCase.randomAlphaOfLength(10)
).field(OpenSearchRestTestCase.randomAlphaOfLength(10))
fun avgAggregation(): AggregationBuilder = AggregationBuilders.avg(
    OpenSearchRestTestCase.randomAlphaOfLength(10)
).field(OpenSearchRestTestCase.randomAlphaOfLength(10))

fun randomAggregationBuilder(): AggregationBuilder {
    val aggregations = listOf(sumAggregation(), maxAggregation(), minAggregation(), valueCountAggregation(), avgAggregation())
    return OpenSearchRestTestCase.randomSubsetOf(1, aggregations).first()
}

fun randomAggregationFactories(): AggregatorFactories.Builder {
    val factories = AggregatorFactories.builder()
    repeat(OpenSearchRestTestCase.randomIntBetween(1, 10)) {
        factories.addAggregator(randomAggregationBuilder())
    }
    return factories
}

fun randomTransform(): Transform {
    val enabled = OpenSearchRestTestCase.randomBoolean()
    val isContinuous = OpenSearchRestTestCase.randomBoolean()
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
        sourceIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT),
        targetIndex = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT),
        roles = OpenSearchRestTestCase.randomList(10) { OpenSearchRestTestCase.randomAlphaOfLength(10) },
        pageSize = if (isContinuous) OpenSearchRestTestCase.randomIntBetween(1, 1000) else OpenSearchRestTestCase.randomIntBetween(1, 10000),
        groups = randomGroups(),
        aggregations = randomAggregationFactories(),
        continuous = isContinuous,
        user = randomUser()
    )
}

fun randomTransformMetadata(): TransformMetadata {
    val status = randomTransformMetadataStatus()
    val isContinuous = OpenSearchRestTestCase.randomBoolean()
    return TransformMetadata(
        id = OpenSearchRestTestCase.randomAlphaOfLength(10),
        seqNo = OpenSearchRestTestCase.randomNonNegativeLong(),
        primaryTerm = OpenSearchRestTestCase.randomNonNegativeLong(),
        transformId = OpenSearchRestTestCase.randomAlphaOfLength(10),
        afterKey = randomAfterKey(),
        lastUpdatedAt = randomInstant(),
        status = status,
        failureReason = if (status == TransformMetadata.Status.FAILED) OpenSearchRestTestCase.randomAlphaOfLength(10) else null,
        stats = randomTransformStats(),
        shardIDToGlobalCheckpoint = if (isContinuous) randomShardIDToGlobalCheckpoint() else null,
        continuousStats = if (isContinuous) randomContinuousStats() else null
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

fun randomShardIDToGlobalCheckpoint(): Map<ShardId, Long> {
    val numIndices = OpenSearchRestTestCase.randomIntBetween(1, 10)
    val randomIndices = (1..numIndices).map { randomShardID() }
    return randomIndices.associateWith { OpenSearchRestTestCase.randomNonNegativeLong() }
}

fun randomShardID(): ShardId {
    val indexName: String = OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT)
    // We lose the index uuid in an XContent round trip, but we don't use it anyways
    val testIndex = Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE)
    val shardNumber: Int = OpenSearchRestTestCase.randomIntBetween(0, 100)
    return ShardId(testIndex, shardNumber)
}

fun randomContinuousStats(): ContinuousTransformStats {
    return ContinuousTransformStats(
        lastTimestamp = randomInstant(),
        documentsBehind = randomDocumentsBehind()
    )
}

fun randomDocumentsBehind(): Map<String, Long> {
    val numIndices = OpenSearchRestTestCase.randomIntBetween(1, 10)
    val randomIndices = (1..numIndices).map { OpenSearchRestTestCase.randomAlphaOfLength(10).lowercase(Locale.ROOT) }
    return randomIndices.associateWith { OpenSearchRestTestCase.randomNonNegativeLong() }
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
    val namedWriteableRegistry = NamedWriteableRegistry(SearchModule(Settings.EMPTY, emptyList()).namedWriteables)
    return NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
}
