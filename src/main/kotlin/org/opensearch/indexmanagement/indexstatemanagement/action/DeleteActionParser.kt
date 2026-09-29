/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class DeleteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val index = sin.readInt()
        val deleteSnapshot = sin.readBoolean()
        return DeleteAction(index, deleteSnapshot)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var deleteSnapshot = false

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                DeleteAction.DELETE_SNAPSHOT_FIELD -> deleteSnapshot = xcp.booleanValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in DeleteAction.")
            }
        }

        return DeleteAction(index, deleteSnapshot)
    }

    override fun getActionType(): String = DeleteAction.name
}
