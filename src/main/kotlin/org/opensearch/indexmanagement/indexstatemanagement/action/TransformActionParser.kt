/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import org.opensearch.indexmanagement.transform.model.ISMTransform

class TransformActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val ismTransform = ISMTransform(sin)
        val index = sin.readInt()
        return TransformAction(ismTransform, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var ismTransform: ISMTransform? = null

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                TransformAction.ISM_TRANSFORM_FIELD -> ismTransform = ISMTransform.parse(xcp)
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in TransformAction.")
            }
        }

        return TransformAction(ismTransform = requireNotNull(ismTransform) { "TransformAction transform is null." }, index)
    }

    override fun getActionType(): String = TransformAction.name
}
