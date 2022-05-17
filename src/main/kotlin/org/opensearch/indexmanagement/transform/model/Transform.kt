/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.action.admin.indices.stats.IndicesStatsAction
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse
import org.opensearch.client.Client
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.authuser.User
import org.opensearch.index.query.AbstractQueryBuilder
import org.opensearch.index.query.MatchAllQueryBuilder
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.common.model.dimension.DateHistogram
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.common.model.dimension.Histogram
import org.opensearch.indexmanagement.common.model.dimension.Terms
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.optionalUserField
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.transform.TransformSearchService
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.schedule.CronSchedule
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule
import org.opensearch.jobscheduler.spi.schedule.Schedule
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser
import org.opensearch.rest.RestStatus
import org.opensearch.search.aggregations.AggregatorFactories
import java.io.IOException
import java.time.Instant

@Suppress("TooManyFunctions")
data class Transform(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val schemaVersion: Long,
    val jobSchedule: Schedule,
    val metadataId: String?,
    val updatedAt: Instant,
    val enabled: Boolean,
    val enabledAt: Instant?,
    val description: String,
    val sourceIndex: String,
    val dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder(),
    val targetIndex: String,
    @Deprecated("Will be ignored, to check the roles use user field") val roles: List<String> = emptyList(),
    val pageSize: Int,
    val groups: List<Dimension>,
    val aggregations: AggregatorFactories.Builder = AggregatorFactories.builder(),
    val continuous: Boolean = false,
    val user: User? = null
) : ScheduledJobParameter, Writeable {

    init {
        aggregations.aggregatorFactories.forEach {
            require(supportedAggregations.contains(it.type)) { "Unsupported aggregation [${it.type}]" }
        }
        when (jobSchedule) {
            is CronSchedule -> {
                // Job scheduler already correctly throws errors for this
            }
            is IntervalSchedule -> {
                require(jobSchedule.interval >= MINIMUM_JOB_INTERVAL) { "Transform job schedule interval must be greater than 0" }
            }
        }
        require(groups.isNotEmpty()) { "Groupings are Empty" }
        require(sourceIndex != targetIndex) { "Source and target indices cannot be the same" }
        if (continuous) {
            require(pageSize in MINIMUM_PAGE_SIZE..MAXIMUM_PAGE_SIZE_CONTINUOUS) { "Page size must be between 1 and 1,000" }
        } else {
            require(pageSize in MINIMUM_PAGE_SIZE..MAXIMUM_PAGE_SIZE) { "Page size must be between 1 and 10,000" }
        }
    }

    override fun getName() = id

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = updatedAt

    override fun getEnabledTime() = enabledAt

    override fun isEnabled() = enabled

    override fun getLockDurationSeconds(): Long = LOCK_DURATION_SECONDS

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(TRANSFORM_TYPE)
        builder.field(TRANSFORM_ID_FIELD, id)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(JOB_SCHEDULE_FIELD, schedule)
            .field(METADATA_ID_FIELD, metadataId)
            .optionalTimeField(UPDATED_AT_FIELD, updatedAt)
            .field(ENABLED_FIELD, enabled)
            .optionalTimeField(ENABLED_AT_FIELD, enabledAt)
            .field(DESCRIPTION_FIELD, description)
            .field(SOURCE_INDEX_FIELD, sourceIndex)
            .field(DATA_SELECTION_QUERY_FIELD, dataSelectionQuery)
            .field(TARGET_INDEX_FIELD, targetIndex)
            .field(PAGE_SIZE_FIELD, pageSize)
            .field(GROUPS_FIELD, groups.toTypedArray())
            .field(AGGREGATIONS_FIELD, aggregations)
            .field(CONTINUOUS_FIELD, continuous)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(USER_FIELD, user)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        builder.endObject()
        return builder
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeLong(schemaVersion)
        if (schedule is CronSchedule) {
            out.writeEnum(ScheduleType.CRON)
        } else {
            out.writeEnum(ScheduleType.INTERVAL)
        }
        schedule.writeTo(out)
        out.writeOptionalString(metadataId)
        out.writeInstant(updatedAt)
        out.writeBoolean(enabled)
        out.writeOptionalInstant(enabledAt)
        out.writeString(description)
        out.writeString(sourceIndex)
        out.writeOptionalNamedWriteable(dataSelectionQuery)
        out.writeString(targetIndex)
        out.writeStringArray(roles.toTypedArray())
        out.writeInt(pageSize)
        out.writeVInt(groups.size)
        for (group in groups) {
            out.writeEnum(group.type)
            when (group) {
                is DateHistogram -> group.writeTo(out)
                is Terms -> group.writeTo(out)
                is Histogram -> group.writeTo(out)
            }
        }
        out.writeOptionalWriteable(aggregations)
        out.writeBoolean(continuous)
        out.writeBoolean(user != null)
        user?.writeTo(out)
    }

    fun convertToDoc(docCount: Long, includeId: Boolean = true): MutableMap<String, Any?> {
        return if (includeId) {
            mutableMapOf(
                TRANSFORM_DOC_ID_FIELD to this.id,
                TRANSFORM_DOC_COUNT_FIELD to docCount
            )
        } else {
            mutableMapOf(
                TRANSFORM_DOC_COUNT_FIELD to docCount
            )
        }
    }

    suspend fun getContinuousStats(client: Client, metadata: TransformMetadata): ContinuousTransformStats? {
        val indicesStatsRequest = IndicesStatsRequest().indices(sourceIndex).clear()
        val response: IndicesStatsResponse = client.suspendUntil { execute(IndicesStatsAction.INSTANCE, indicesStatsRequest, it) }
        val shardIDsToGlobalCheckpoint = if (response.status == RestStatus.OK) {
            TransformSearchService.convertIndicesStatsResponse(response)
        } else return null
        return ContinuousTransformStats(
            metadata.continuousStats?.lastTimestamp,
            getDocumentsBehind(
                metadata.shardIDToGlobalCheckpoint,
                shardIDsToGlobalCheckpoint
            )
        )
    }

    private fun getDocumentsBehind(
        oldShardIDsToGlobalCheckpoint: Map<ShardId, Long>?,
        newShardIDsToGlobalCheckpoint: Map<ShardId, Long>?
    ): MutableMap<String, Long> {
        val documentsBehind: MutableMap<String, Long> = HashMap()
        if (newShardIDsToGlobalCheckpoint == null) {
            return documentsBehind
        }
        newShardIDsToGlobalCheckpoint.forEach { (shardID, globalCheckpoint) ->
            val indexName = shardID.indexName
            val newGlobalCheckpoint = java.lang.Long.max(0, globalCheckpoint)
            // global checkpoint may be -1 or -2 if not initialized, just set to 0 in those cases
            val oldGlobalCheckpoint = java.lang.Long.max(0, oldShardIDsToGlobalCheckpoint?.get(shardID) ?: 0)
            val localDocsBehind = newGlobalCheckpoint - oldGlobalCheckpoint
            documentsBehind[indexName] = (documentsBehind[indexName] ?: 0) + localDocsBehind
        }

        return documentsBehind
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        schemaVersion = sin.readLong(),
        jobSchedule = sin.let {
            when (requireNotNull(sin.readEnum(ScheduleType::class.java)) { "ScheduleType cannot be null" }) {
                ScheduleType.CRON -> CronSchedule(sin)
                ScheduleType.INTERVAL -> IntervalSchedule(sin)
            }
        },
        metadataId = sin.readOptionalString(),
        updatedAt = sin.readInstant(),
        enabled = sin.readBoolean(),
        enabledAt = sin.readOptionalInstant(),
        description = sin.readString(),
        sourceIndex = sin.readString(),
        dataSelectionQuery = requireNotNull(sin.readOptionalNamedWriteable(QueryBuilder::class.java)) { "Query cannot be null" },
        targetIndex = sin.readString(),
        roles = sin.readStringArray().toList(),
        pageSize = sin.readInt(),
        groups = sin.let {
            val dimensionList = mutableListOf<Dimension>()
            val size = it.readVInt()
            repeat(size) { _ ->
                val type = it.readEnum(Dimension.Type::class.java)
                dimensionList.add(
                    when (requireNotNull(type) { "Dimension type cannot be null" }) {
                        Dimension.Type.DATE_HISTOGRAM -> DateHistogram(sin)
                        Dimension.Type.TERMS -> Terms(sin)
                        Dimension.Type.HISTOGRAM -> Histogram(sin)
                    }
                )
            }
            dimensionList.toList()
        },
        aggregations = requireNotNull(sin.readOptionalWriteable { AggregatorFactories.Builder(it) }) { "Aggregations cannot be null" },
        continuous = sin.readBoolean(),
        user = if (sin.readBoolean()) {
            User(sin)
        } else null
    )

    companion object {
        enum class ScheduleType {
            CRON, INTERVAL;
        }

        val supportedAggregations = listOf("sum", "max", "min", "value_count", "avg", "scripted_metric", "percentiles")
        const val LOCK_DURATION_SECONDS = 1800L
        const val TRANSFORM_TYPE = "transform"
        const val TRANSFORM_ID_FIELD = "transform_id"
        const val ENABLED_FIELD = "enabled"
        const val UPDATED_AT_FIELD = "updated_at"
        const val ENABLED_AT_FIELD = "enabled_at"
        const val SOURCE_INDEX_FIELD = "source_index"
        const val TARGET_INDEX_FIELD = "target_index"
        const val DESCRIPTION_FIELD = "description"
        const val DATA_SELECTION_QUERY_FIELD = "data_selection_query"
        const val METADATA_ID_FIELD = "metadata_id"
        const val PAGE_SIZE_FIELD = "page_size"
        const val ROLES_FIELD = "roles"
        const val JOB_SCHEDULE_FIELD = "schedule"
        const val GROUPS_FIELD = "groups"
        const val AGGREGATIONS_FIELD = "aggregations"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val MINIMUM_PAGE_SIZE = 1
        const val MAXIMUM_PAGE_SIZE = 10_000
        const val MAXIMUM_PAGE_SIZE_CONTINUOUS = 1_000
        const val MINIMUM_JOB_INTERVAL = 1
        const val TRANSFORM_DOC_ID_FIELD = "$TRANSFORM_TYPE._id"
        const val TRANSFORM_DOC_COUNT_FIELD = "$TRANSFORM_TYPE._doc_count"
        const val CONTINUOUS_FIELD = "continuous"
        const val USER_FIELD = "user"

        @Suppress("ComplexMethod", "LongMethod")
        @JvmStatic
        @JvmOverloads
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): Transform {
            var schedule: Schedule? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            var updatedAt: Instant? = null
            var enabledAt: Instant? = null
            var enabled = true
            var description: String? = null
            var sourceIndex: String? = null
            var dataSelectionQuery: QueryBuilder = MatchAllQueryBuilder()
            var targetIndex: String? = null
            var metadataId: String? = null
            var pageSize: Int? = null
            val groups = mutableListOf<Dimension>()
            var aggregations: AggregatorFactories.Builder = AggregatorFactories.builder()
            var continuous = false
            var user: User? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    TRANSFORM_ID_FIELD -> {
                        requireNotNull(xcp.text()) { "The transform_id field is null" }
                    }
                    JOB_SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    UPDATED_AT_FIELD -> updatedAt = xcp.instant()
                    ENABLED_AT_FIELD -> enabledAt = xcp.instant()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    DESCRIPTION_FIELD -> description = xcp.text()
                    SOURCE_INDEX_FIELD -> sourceIndex = xcp.text()
                    DATA_SELECTION_QUERY_FIELD -> {
                        val registry = xcp.xContentRegistry
                        val source = xcp.mapOrdered()
                        val xContentBuilder = XContentFactory.jsonBuilder().map(source)
                        val sourceParser = XContentType.JSON.xContent().createParser(
                            registry, LoggingDeprecationHandler.INSTANCE,
                            BytesReference
                                .bytes(xContentBuilder).streamInput()
                        )
                        dataSelectionQuery = AbstractQueryBuilder.parseInnerQueryBuilder(sourceParser)
                    }
                    TARGET_INDEX_FIELD -> targetIndex = xcp.text()
                    METADATA_ID_FIELD -> metadataId = xcp.textOrNull()
                    ROLES_FIELD -> {
                        // Parsing but not storing the field, deprecated
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            xcp.text()
                        }
                    }
                    PAGE_SIZE_FIELD -> pageSize = xcp.intValue()
                    GROUPS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            groups.add(Dimension.parse(xcp))
                        }
                    }
                    AGGREGATIONS_FIELD -> aggregations = AggregatorFactories.parseAggregators(xcp)
                    CONTINUOUS_FIELD -> continuous = xcp.booleanValue()
                    USER_FIELD -> {
                        user = if (xcp.currentToken() == Token.VALUE_NULL) null else User.parse(xcp)
                    }
                    else -> throw IllegalArgumentException("Invalid field [$fieldName] found in Transforms.")
                }
            }

            if (enabled && enabledAt == null) {
                enabledAt = Instant.now()
            } else if (!enabled) {
                enabledAt = null
            }

            // If the seqNo/primaryTerm are unassigned this job hasn't been created yet
            if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || primaryTerm == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                // we instantiate the start time
                if (schedule is IntervalSchedule) {
                    schedule = IntervalSchedule(Instant.now(), schedule.interval, schedule.unit)
                }

                // we clear out metadata if its a new job
                metadataId = null
            }

            return Transform(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                schemaVersion = schemaVersion,
                jobSchedule = requireNotNull(schedule) { "Transform schedule is null" },
                metadataId = metadataId,
                updatedAt = updatedAt ?: Instant.now(),
                enabled = enabled,
                enabledAt = enabledAt,
                description = requireNotNull(description) { "Transform description is null" },
                sourceIndex = requireNotNull(sourceIndex) { "Transform source index is null" },
                dataSelectionQuery = dataSelectionQuery,
                targetIndex = requireNotNull(targetIndex) { "Transform target index is null" },
                pageSize = requireNotNull(pageSize) { "Transform page size is null" },
                groups = groups,
                aggregations = aggregations,
                continuous = continuous,
                user = user
            )
        }
    }
}
