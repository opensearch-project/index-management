/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.AliasAction.Companion.ACTIONS
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class AliasActionParser : ActionParser() {
    private val logger = LogManager.getLogger(javaClass)

    override fun fromStreamInput(sin: StreamInput): Action {
        val actions = sin.readList(IndicesAliasesRequest::AliasActions)
        val index = sin.readInt()
        return AliasAction(actions, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        val actions: MutableList<IndicesAliasesRequest.AliasActions> = mutableListOf()

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()
            when (fieldName) {
                ACTIONS -> {
                    ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                    while (xcp.nextToken() != Token.END_ARRAY) {
                        actions.add(IndicesAliasesRequest.AliasActions.fromXContent(xcp))
                    }
                }

                else -> {
                    logger.error("Invalid field: [$fieldName] found in AliasAction.")
                    throw IllegalArgumentException("Invalid field: [$fieldName] found in AliasAction.")
                }
            }
        }
        return AliasAction(actions, index)
    }

    override fun getActionType(): String = AliasAction.name
}
