/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class RolloverConditionGroup(
    val minSize: ByteSizeValue?,
    val minDocs: Long?,
    val minAge: TimeValue?,
    val minPrimaryShardSize: ByteSizeValue?,
) : ToXContentObject,
    Writeable {
    init {
        require(minSize != null || minDocs != null || minAge != null || minPrimaryShardSize != null) {
            "A rollover condition group must contain at least one condition"
        }
        if (minSize != null) require(minSize.bytes > 0) { "RolloverConditionGroup minSize value must be greater than 0" }
        if (minPrimaryShardSize != null) {
            require(minPrimaryShardSize.bytes > 0) { "RolloverConditionGroup minPrimaryShardSize value must be greater than 0" }
        }
        if (minDocs != null) require(minDocs > 0) { "RolloverConditionGroup minDocs value must be greater than 0" }
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        minSize = sin.readOptionalWriteable(::ByteSizeValue),
        minDocs = sin.readOptionalLong(),
        minAge = sin.readOptionalTimeValue(),
        minPrimaryShardSize = sin.readOptionalWriteable(::ByteSizeValue),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalWriteable(minSize)
        out.writeOptionalLong(minDocs)
        out.writeOptionalTimeValue(minAge)
        out.writeOptionalWriteable(minPrimaryShardSize)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (minSize != null) builder.field(RolloverAction.MIN_SIZE_FIELD, minSize.stringRep)
        if (minDocs != null) builder.field(RolloverAction.MIN_DOC_COUNT_FIELD, minDocs)
        if (minAge != null) builder.field(RolloverAction.MIN_INDEX_AGE_FIELD, minAge.stringRep)
        if (minPrimaryShardSize != null) builder.field(RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD, minPrimaryShardSize.stringRep)
        return builder.endObject()
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): RolloverConditionGroup {
            var minSize: ByteSizeValue? = null
            var minDocs: Long? = null
            var minAge: TimeValue? = null
            var minPrimaryShardSize: ByteSizeValue? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val conditionName = xcp.currentName()
                xcp.nextToken()

                when (conditionName) {
                    RolloverAction.MIN_SIZE_FIELD ->
                        minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), RolloverAction.MIN_SIZE_FIELD)

                    RolloverAction.MIN_DOC_COUNT_FIELD -> minDocs = xcp.longValue()

                    RolloverAction.MIN_INDEX_AGE_FIELD ->
                        minAge = TimeValue.parseTimeValue(xcp.text(), RolloverAction.MIN_INDEX_AGE_FIELD)

                    RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD ->
                        minPrimaryShardSize =
                            ByteSizeValue.parseBytesSizeValue(xcp.text(), RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD)

                    else -> throw IllegalArgumentException(
                        "Invalid field: [$conditionName] found in rollover condition group.",
                    )
                }
            }

            return RolloverConditionGroup(minSize, minDocs, minAge, minPrimaryShardSize)
        }
    }
}
