/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction.Companion.REPOSITORY_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotAction.Companion.SNAPSHOT_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class SnapshotActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val repository = sin.readString()
        val snapshot = sin.readString()
        val index = sin.readInt()

        return SnapshotAction(repository, snapshot, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var repository: String? = null
        var snapshot: String? = null

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                REPOSITORY_FIELD -> repository = xcp.text()
                SNAPSHOT_FIELD -> snapshot = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in SnapshotAction.")
            }
        }

        return SnapshotAction(
            repository = requireNotNull(repository) { "SnapshotAction repository must be specified" },
            snapshot = requireNotNull(snapshot) { "SnapshotAction snapshot must be specified" },
            index = index
        )
    }

    override fun getActionType(): String {
        return SnapshotAction.name
    }
}
