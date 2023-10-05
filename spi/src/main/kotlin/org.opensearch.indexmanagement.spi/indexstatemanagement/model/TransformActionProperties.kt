/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser

data class TransformActionProperties(
    val transformId: String?
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(transformId)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        if (transformId != null) builder.field(Properties.TRANSFORM_ID.key, transformId)
        return builder
    }

    companion object {
        const val TRANSFORM_ACTION_PROPERTIES = "transform_action_properties"

        fun fromStreamInput(sin: StreamInput): TransformActionProperties {
            val transformId: String? = sin.readOptionalString()
            return TransformActionProperties(transformId)
        }

        fun parse(xcp: XContentParser): TransformActionProperties {
            var transformId: String? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Properties.TRANSFORM_ID.key -> transformId = xcp.text()
                }
            }

            return TransformActionProperties(transformId)
        }
    }

    enum class Properties(val key: String) {
        TRANSFORM_ID("transform_id")
    }
}
