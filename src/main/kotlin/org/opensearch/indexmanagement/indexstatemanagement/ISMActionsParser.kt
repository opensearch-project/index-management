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
import org.opensearch.indexmanagement.indexstatemanagement.action.SnapshotActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout

class ISMActionsParser private constructor() {

    private object HOLDER {
        val instance = ISMActionsParser()
    }

    val parsers = mutableListOf<ActionParser>(
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
        SnapshotActionParser()
    )

    fun addParser(parser: ActionParser) {
        if (parsers.map { it.getActionType() }.contains(parser.getActionType())) {
            throw IllegalArgumentException(getDuplicateActionTypesMessage(parser.getActionType()))
        }
        parsers.add(parser)
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
                    xcp.nextToken()
                    val customActionType = xcp.currentName()
                    xcp.nextToken()
                    val customParser = parsers.firstOrNull { it.getActionType() == customActionType }
                    if (customParser != null) {
                        action = customParser.fromXContent(xcp, totalActions)
                        action.customAction = customParser.customAction
                    } else {
                        throw IllegalArgumentException("Invalid field: [$customActionType] found in custom Actions.")
                    }
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.currentToken(), xcp)
                    xcp.nextToken()
                }
                else -> {
                    val parser = parsers.firstOrNull { it.getActionType() == type }
                    if (parser != null) {
                        action = parser.fromXContent(xcp, totalActions)
                        action.customAction = parser.customAction
                    } else {
                        throw IllegalArgumentException("Invalid field: [$type] found in Actions.")
                    }
                }
            }
        }

        requireNotNull(action) { "Action inside state is null" }

        action.configTimeout = timeout
        action.configRetry = retry

        return action
    }

    companion object {
        val instance: ISMActionsParser by lazy { HOLDER.instance }
        fun getDuplicateActionTypesMessage(actionType: String) = "Multiple action parsers attempted to register the same action type [$actionType]"
    }
}
