/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.IGNORE_INDEX_SETTINGS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.INCLUDE_ALIASES_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.NUMBER_OF_REPLICAS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.REPOSITORY_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.SNAPSHOT_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ConvertIndexToRemoteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val repository = sin.readString()
        val snapshot = sin.readString()
        val includeAliases = sin.readBoolean()
        val ignoreIndexSettings = sin.readString()
        val numberOfReplicas = sin.readInt()
        val index = sin.readInt()
        return ConvertIndexToRemoteAction(repository, snapshot, includeAliases, ignoreIndexSettings, numberOfReplicas, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var repository: String? = null
        var snapshot: String? = null
        var includeAliases: Boolean = false
        var ignoreIndexSettings: String = ""
        var numberOfReplicas: Int = 0

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                REPOSITORY_FIELD -> repository = xcp.text()
                SNAPSHOT_FIELD -> snapshot = xcp.text()
                INCLUDE_ALIASES_FIELD -> includeAliases = xcp.booleanValue()
                IGNORE_INDEX_SETTINGS_FIELD -> ignoreIndexSettings = xcp.text()
                NUMBER_OF_REPLICAS_FIELD -> numberOfReplicas = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ConvertIndexToRemoteAction.")
            }
        }

        return ConvertIndexToRemoteAction(
            repository = requireNotNull(repository) { "ConvertIndexToRemoteAction repository must be specified" },
            snapshot = requireNotNull(snapshot) { "ConvertIndexToRemoteAction snapshot must be specified" },
            includeAliases = includeAliases,
            ignoreIndexSettings = ignoreIndexSettings,
            numberOfReplicas = numberOfReplicas,
            index = index,
        )
    }

    override fun getActionType(): String = ConvertIndexToRemoteAction.name
}
