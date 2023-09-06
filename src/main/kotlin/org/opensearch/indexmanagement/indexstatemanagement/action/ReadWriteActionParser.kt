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

class ReadWriteActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val index = sin.readInt()
        return ReadWriteAction(index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        ensureExpectedToken(XContentParser.Token.END_OBJECT, xcp.nextToken(), xcp)

        return ReadWriteAction(index)
    }

    override fun getActionType(): String {
        return ReadWriteAction.name
    }
}
