/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction.Companion.EXCLUDE
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction.Companion.INCLUDE
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction.Companion.REQUIRE
import org.opensearch.indexmanagement.indexstatemanagement.action.AllocationAction.Companion.WAIT_FOR
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook.Companion.suppressWarning
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class AllocationActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val require = suppressWarning(sin.readMap())
        val include = suppressWarning(sin.readMap())
        val exclude = suppressWarning(sin.readMap())
        val waitFor = sin.readBoolean()
        val index = sin.readInt()

        return AllocationAction(require, include, exclude, waitFor, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        val require: MutableMap<String, String> = mutableMapOf()
        val include: MutableMap<String, String> = mutableMapOf()
        val exclude: MutableMap<String, String> = mutableMapOf()
        var waitFor = false

        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()
            when (fieldName) {
                REQUIRE -> assignObject(xcp, require)
                INCLUDE -> assignObject(xcp, include)
                EXCLUDE -> assignObject(xcp, exclude)
                WAIT_FOR -> waitFor = xcp.booleanValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in AllocationAction.")
            }
        }
        return AllocationAction(require, include, exclude, waitFor, index)
    }

    override fun getActionType(): String {
        return AllocationAction.name
    }

    private fun assignObject(xcp: XContentParser, objectMap: MutableMap<String, String>) {
        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()
            objectMap[fieldName] = xcp.text()
        }
    }
}
