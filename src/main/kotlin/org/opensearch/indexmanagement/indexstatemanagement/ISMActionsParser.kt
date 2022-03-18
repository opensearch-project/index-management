/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.CloseActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ForceMergeActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.IndexPriorityActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.NotificationActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.OpenActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadOnlyActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadWriteActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ReplicaCountActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.RolloverActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.RollupActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ShrinkActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout

class ISMActionsParser private constructor() {

    private object HOLDER {
        val instance = ISMActionsParser()
    }

    val parsers = mutableListOf(
        AllocationActionParser(),
        CloseActionParser(),
        DeleteActionParser(),
        ForceMergeActionParser(),
        IndexPriorityActionParser(),
        NotificationActionParser(),
        OpenActionParser(),
        ReadOnlyActionParser(),
        ReadWriteActionParser(),
        ReplicaCountActionParser(),
        RollupActionParser(),
        RolloverActionParser(),
        ShrinkActionParser(),
        SnapshotActionParser()
    )

    val customActionExtensionMap = mutableMapOf<String, String>()

    /*
     * This method is used for adding custom action parsers. Action parsers from ISM should be added directly
     * to the parsers list.
     */
    fun addParser(parser: ActionParser, extensionName: String) {
        if (parsers.map { it.getActionType() }.contains(parser.getActionType())) {
            throw IllegalArgumentException(getDuplicateActionTypesMessage(parser.getActionType()))
        }
        // Set the parser as custom to make sure that the custom actions are written with the "custom" wrapper
        parser.customAction = true
        parsers.add(parser)
        customActionExtensionMap[parser.getActionType()] = extensionName
    }

    fun fromStreamInput(sin: StreamInput): Action {
        val type: String = sin.readString()
        val configTimeout = sin.readOptionalWriteable(::ActionTimeout)
        val configRetry = sin.readOptionalWriteable(::ActionRetry)
        val parser = parsers.firstOrNull { it.getActionType() == type }
        val action: Action = parser?.fromStreamInput(sin) ?: throw IllegalArgumentException("Invalid field: [$type] found in Actions.")

        action.configTimeout = configTimeout
        action.configRetry = configRetry

        return action
    }

    fun parse(xcp: XContentParser, totalActions: Int): Action {
        var action: Action? = null
        var timeout: ActionTimeout? = null
        var retry: ActionRetry? = ActionRetry(Action.DEFAULT_RETRIES)
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val type = xcp.currentName()
            xcp.nextToken()
            when (type) {
                ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                Action.CUSTOM_ACTION_FIELD -> {
                    // The "custom" wrapper allows extensions to create arbitrary actions without updating the config mappings
                    // We consume the full custom wrapper and parse the action in this step
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, xcp.nextToken(), xcp)
                    val customActionType = xcp.currentName()
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                    action = parseAction(xcp, totalActions, customActionType)
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)
                }
                else -> {
                    action = parseAction(xcp, totalActions, type)
                }
            }
        }

        requireNotNull(action) { "Action inside state is null" }

        action.configTimeout = timeout
        action.configRetry = retry

        return action
    }

    private fun parseAction(xcp: XContentParser, totalActions: Int, type: String): Action {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        val action: Action?
        val parser = parsers.firstOrNull { it.getActionType() == type }
        if (parser != null) {
            action = parser.fromXContent(xcp, totalActions)
            action.customAction = parser.customAction
        } else {
            throw IllegalArgumentException("Invalid field: [$type] found in Actions.")
        }
        return action
    }

    companion object {
        val instance: ISMActionsParser by lazy { HOLDER.instance }
        fun getDuplicateActionTypesMessage(actionType: String) = "Multiple action parsers attempted to register the same action type [$actionType]"
    }
}
