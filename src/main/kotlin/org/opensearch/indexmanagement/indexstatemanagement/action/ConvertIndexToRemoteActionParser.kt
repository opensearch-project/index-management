/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.ADD_ORIGINAL_NAME_AS_ALIAS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.DEFAULT_RENAME_PATTERN
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.DELETE_ORIGINAL_INDEX_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.IGNORE_INDEX_SETTINGS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.INCLUDE_ALIASES_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.NUMBER_OF_REPLICAS_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.RENAME_PATTERN_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.REPOSITORY_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.SNAPSHOT_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.VERSION_WITH_ADD_ORIGINAL_ALIAS
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.VERSION_WITH_RENAME_PATTERN
import org.opensearch.indexmanagement.indexstatemanagement.action.ConvertIndexToRemoteAction.Companion.VERSION_WITH_RESTORE_OPTIONS
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class ConvertIndexToRemoteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val repository = sin.readString()
        val snapshot = sin.readString()
        val includeAliases = if (sin.version.onOrAfter(VERSION_WITH_RESTORE_OPTIONS)) {
            sin.readBoolean()
        } else {
            false
        }
        val ignoreIndexSettings = if (sin.version.onOrAfter(VERSION_WITH_RESTORE_OPTIONS)) {
            sin.readString()
        } else {
            ""
        }
        val numberOfReplicas = if (sin.version.onOrAfter(VERSION_WITH_RESTORE_OPTIONS)) {
            val v = sin.readInt()
            if (v < 0) null else v
        } else {
            null
        }
        val deleteOriginalIndex = if (sin.version.onOrAfter(VERSION_WITH_RESTORE_OPTIONS)) {
            sin.readBoolean()
        } else {
            false
        }
        val addOriginalNameAsAlias = if (sin.version.onOrAfter(VERSION_WITH_ADD_ORIGINAL_ALIAS)) {
            sin.readBoolean()
        } else {
            false
        }
        val renamePattern = if (sin.version.onOrAfter(VERSION_WITH_RENAME_PATTERN)) {
            sin.readString()
        } else {
            DEFAULT_RENAME_PATTERN
        }
        val index = sin.readInt()
        return ConvertIndexToRemoteAction(
            repository = repository,
            snapshot = snapshot,
            includeAliases = includeAliases,
            ignoreIndexSettings = ignoreIndexSettings,
            numberOfReplicas = numberOfReplicas,
            deleteOriginalIndex = deleteOriginalIndex,
            addOriginalNameAsAlias = addOriginalNameAsAlias,
            renamePattern = renamePattern,
            index = index,
        )
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var repository: String? = null
        var snapshot: String? = null
        var includeAliases: Boolean = false
        var ignoreIndexSettings: String = ""
        var numberOfReplicas: Int? = null
        var deleteOriginalIndex: Boolean = false
        var addOriginalNameAsAlias: Boolean = false
        var renamePattern: String = DEFAULT_RENAME_PATTERN

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                REPOSITORY_FIELD -> repository = xcp.text()
                SNAPSHOT_FIELD -> snapshot = xcp.text()
                INCLUDE_ALIASES_FIELD -> includeAliases = xcp.booleanValue()
                IGNORE_INDEX_SETTINGS_FIELD -> ignoreIndexSettings = xcp.text()
                NUMBER_OF_REPLICAS_FIELD -> numberOfReplicas = xcp.intValue().let { if (it < 0) null else it }
                DELETE_ORIGINAL_INDEX_FIELD -> deleteOriginalIndex = xcp.booleanValue()
                ADD_ORIGINAL_NAME_AS_ALIAS_FIELD -> addOriginalNameAsAlias = xcp.booleanValue()
                RENAME_PATTERN_FIELD -> renamePattern = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ConvertIndexToRemoteAction.")
            }
        }

        return ConvertIndexToRemoteAction(
            repository = requireNotNull(repository) { "ConvertIndexToRemoteAction repository must be specified" },
            snapshot = requireNotNull(snapshot) { "ConvertIndexToRemoteAction snapshot must be specified" },
            includeAliases = includeAliases,
            ignoreIndexSettings = ignoreIndexSettings,
            numberOfReplicas = numberOfReplicas,
            deleteOriginalIndex = deleteOriginalIndex,
            addOriginalNameAsAlias = addOriginalNameAsAlias,
            renamePattern = renamePattern,
            index = index,
        )
    }

    override fun getActionType(): String = ConvertIndexToRemoteAction.name
}
