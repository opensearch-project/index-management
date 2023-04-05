/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.spi.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentFragment
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser

data class TransformActionProperties(
    val transformId: String?,
    val hasTransformFailed: Boolean?
) : Writeable, ToXContentFragment {

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(transformId)
        out.writeOptionalBoolean(hasTransformFailed)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        if (transformId != null) builder.field(Properties.TRANSFORM_ID.key, transformId)
        if (hasTransformFailed != null) builder.field(Properties.HAS_TRANSFORM_FAILED.key, hasTransformFailed)
        return builder
    }

    companion object {
        const val TRANSFORM_ACTION_PROPERTIES = "transform_action_properties"

        fun fromStreamInput(sin: StreamInput): TransformActionProperties {
            val transformId: String? = sin.readOptionalString()
            val hasTransformFailed: Boolean? = sin.readOptionalBoolean()
            return TransformActionProperties(transformId, hasTransformFailed)
        }

        fun parse(xcp: XContentParser): TransformActionProperties {
            var transformId: String? = null
            var hasTransformFailed: Boolean? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    Properties.TRANSFORM_ID.key -> transformId = xcp.text()
                    Properties.HAS_TRANSFORM_FAILED.key -> hasTransformFailed = xcp.booleanValue()
                }
            }

            return TransformActionProperties(requireNotNull(transformId), requireNotNull(hasTransformFailed))
        }
    }

    enum class Properties(val key: String) {
        TRANSFORM_ID("transformId"),
        HAS_TRANSFORM_FAILED("has_transform_failed")
    }
}
