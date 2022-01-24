/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeAction.Companion.MAX_NUM_SEGMENTS_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ForceMergeActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val maxNumSegments = sin.readInt()
        val index = sin.readInt()
        return ForceMergeAction(maxNumSegments, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var maxNumSegments: Int? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                MAX_NUM_SEGMENTS_FIELD -> maxNumSegments = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ForceMergeActionConfig.")
            }
        }

        return ForceMergeAction(
            requireNotNull(maxNumSegments) { "ForceMergeActionConfig maxNumSegments is null" },
            index
        )
    }

    override fun getActionType(): String {
        return ForceMergeAction.name
    }
}
