/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityAction.Companion.INDEX_PRIORITY_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class IndexPriorityActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val indexPriority = sin.readInt()
        val index = sin.readInt()
        return IndexPriorityAction(indexPriority, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var indexPriority: Int? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                INDEX_PRIORITY_FIELD -> indexPriority = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in IndexPriorityActionConfig.")
            }
        }

        return IndexPriorityAction(
            indexPriority = requireNotNull(indexPriority) { "$INDEX_PRIORITY_FIELD is null" },
            index = index
        )
    }

    override fun getActionType(): String {
        return IndexPriorityAction.name
    }
}
