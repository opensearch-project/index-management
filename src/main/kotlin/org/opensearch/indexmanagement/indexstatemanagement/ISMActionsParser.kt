/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.indexmanagement.indexstatemanagement.action.CloseActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteActionParser
import org.opensearch.indexmanagement.indexstatemanagement.action.ReadWriteActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionRetry
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ActionTimeout

class ISMActionsParser private constructor() {

    private object HOLDER {
        val instance = ISMActionsParser()
    }

    // TODO: Add other action parsers as they are implemented
    val parsers = mutableListOf<ActionParser>(
        CloseActionParser(),
        DeleteActionParser(),
        ReadWriteActionParser()
    )

    fun addParser(parser: ActionParser) {
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
        var retry: ActionRetry? = null
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val type = xcp.currentName()
            xcp.nextToken()
            when (type) {
                ActionTimeout.TIMEOUT_FIELD -> timeout = ActionTimeout.parse(xcp)
                ActionRetry.RETRY_FIELD -> retry = ActionRetry.parse(xcp)
                else -> {
                    val parser = parsers.firstOrNull { it.getActionType() == type }
                    if (parser != null) {
                        action = parser.fromXContent(xcp, totalActions)
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
    }
}
