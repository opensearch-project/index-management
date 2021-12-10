/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.ByteSizeValue
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.script.ScriptService
import java.io.IOException

data class RolloverActionConfig(
    val minSize: ByteSizeValue?,
    val minDocs: Long?,
    val minAge: TimeValue?,
    val index: Int
) : ToXContentObject, ActionConfig(ActionType.ROLLOVER, index) {

    init {
        if (minSize != null) require(minSize.bytes > 0) { "RolloverActionConfig minSize value must be greater than 0" }

        if (minDocs != null) require(minDocs > 0) { "RolloverActionConfig minDocs value must be greater than 0" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        super.toXContent(builder, params)
            .startObject(ActionType.ROLLOVER.type)
        if (minSize != null) builder.field(MIN_SIZE_FIELD, minSize.stringRep)
        if (minDocs != null) builder.field(MIN_DOC_COUNT_FIELD, minDocs)
        if (minAge != null) builder.field(MIN_INDEX_AGE_FIELD, minAge.stringRep)
        return builder.endObject().endObject()
    }

    override fun isFragment(): Boolean = super<ToXContentObject>.isFragment()

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = RolloverAction(clusterService, client, managedIndexMetaData, this)

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        minSize = sin.readOptionalWriteable(::ByteSizeValue),
        minDocs = sin.readOptionalLong(),
        minAge = sin.readOptionalTimeValue(),
        index = sin.readInt()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalWriteable(minSize)
        out.writeOptionalLong(minDocs)
        out.writeOptionalTimeValue(minAge)
        out.writeInt(index)
    }

    companion object {
        const val MIN_SIZE_FIELD = "min_size"
        const val MIN_DOC_COUNT_FIELD = "min_doc_count"
        const val MIN_INDEX_AGE_FIELD = "min_index_age"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, index: Int): RolloverActionConfig {
            var minSize: ByteSizeValue? = null
            var minDocs: Long? = null
            var minAge: TimeValue? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    MIN_SIZE_FIELD -> minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), MIN_SIZE_FIELD)
                    MIN_DOC_COUNT_FIELD -> minDocs = xcp.longValue()
                    MIN_INDEX_AGE_FIELD -> minAge = TimeValue.parseTimeValue(xcp.text(), MIN_INDEX_AGE_FIELD)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RolloverActionConfig.")
                }
            }

            return RolloverActionConfig(
                minSize = minSize,
                minDocs = minDocs,
                minAge = minAge,
                index = index
            )
        }
    }
}
