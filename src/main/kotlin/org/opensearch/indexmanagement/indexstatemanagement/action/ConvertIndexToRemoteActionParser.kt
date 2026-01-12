/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.DEFAULT_RENAME_PATTERN
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.RENAME_PATTERN_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.REPOSITORY_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.SNAPSHOT_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ConvertIndexToRemoteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val repository = sin.readString()
        val snapshot = sin.readString()
        val renamePattern = sin.readString()
        val index = sin.readInt()
        return ConvertIndexToRemoteAction(repository, snapshot, renamePattern, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var repository: String? = null
        var snapshot: String? = null
        var renamePattern: String = DEFAULT_RENAME_PATTERN

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                REPOSITORY_FIELD -> repository = xcp.text()
                SNAPSHOT_FIELD -> snapshot = xcp.text()
                RENAME_PATTERN_FIELD -> renamePattern = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ConvertIndexToRemoteAction.")
            }
        }

        return ConvertIndexToRemoteAction(
            repository = requireNotNull(repository) { "ConvertIndexToRemoteAction repository must be specified" },
            snapshot = requireNotNull(snapshot) { "ConvertIndexToRemoteAction snapshot must be specified" },
            renamePattern = renamePattern,
            index = index,
        )
    }

    override fun getActionType(): String = ConvertIndexToRemoteAction.name
}
