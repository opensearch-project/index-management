/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.Version
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.unit.ByteSizeValue
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RolloverActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val minSize = sin.readOptionalWriteable(::ByteSizeValue)
        val minDocs = sin.readOptionalLong()
        val minAge = sin.readOptionalTimeValue()
        val minPrimaryShardSize = sin.readOptionalWriteable(::ByteSizeValue)
        val copyAlias = sin.readBoolean()
        val preventEmptyRollover = if (sin.version.onOrAfter(Version.V_3_4_0)) {
            sin.readBoolean()
        } else {
            false
        }
        val index = sin.readInt()

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, copyAlias, preventEmptyRollover, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var minSize: ByteSizeValue? = null
        var minDocs: Long? = null
        var minAge: TimeValue? = null
        var minPrimaryShardSize: ByteSizeValue? = null
        var copyAlias = false
        var preventEmptyRollover = false

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                RolloverAction.MIN_SIZE_FIELD -> minSize = ByteSizeValue.parseBytesSizeValue(xcp.text(), RolloverAction.MIN_SIZE_FIELD)
                RolloverAction.MIN_DOC_COUNT_FIELD -> minDocs = xcp.longValue()
                RolloverAction.MIN_INDEX_AGE_FIELD -> minAge = TimeValue.parseTimeValue(xcp.text(), RolloverAction.MIN_INDEX_AGE_FIELD)
                RolloverAction.MIN_PRIMARY_SHARD_SIZE_FIELD ->
                    minPrimaryShardSize =
                        ByteSizeValue.parseBytesSizeValue(
                            xcp.text(),
                            RolloverAction
                                .MIN_PRIMARY_SHARD_SIZE_FIELD,
                        )
                RolloverAction.COPY_ALIAS_FIELD -> copyAlias = xcp.booleanValue()

                RolloverAction.PREVENT_EMPTY_ROLLOVER_FIELD -> preventEmptyRollover = xcp.booleanValue()

                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RolloverAction.")
            }
        }

        return RolloverAction(minSize, minDocs, minAge, minPrimaryShardSize, copyAlias, preventEmptyRollover, index)
    }

    override fun getActionType(): String = RolloverAction.name
}
