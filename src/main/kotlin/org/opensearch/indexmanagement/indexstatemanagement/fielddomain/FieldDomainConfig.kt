/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.fielddomain

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken

/**
 * Policy configuration for one field domain that ISM should compute and publish.
 */
data class FieldDomainConfig(
    val field: String,
    val type: String,
) : ToXContentObject,
    Writeable {
    init {
        require(field.isNotBlank()) { "Field domain field must not be empty" }
        require(type.isNotBlank()) { "Field domain type must not be empty" }
    }

    constructor(sin: StreamInput) : this(
        field = sin.readString(),
        type = sin.readString(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(field)
        out.writeString(type)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder = builder
        .startObject()
        .field(FIELD_FIELD, field)
        .field(TYPE_FIELD, type)
        .endObject()

    companion object {
        const val FIELD_FIELD = "field"
        const val TYPE_FIELD = "type"

        fun parse(xcp: XContentParser): FieldDomainConfig {
            var field: String? = null
            var type: String? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FIELD_FIELD -> field = xcp.text()
                    TYPE_FIELD -> type = xcp.text()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in FieldDomainConfig.")
                }
            }

            return FieldDomainConfig(
                field = requireNotNull(field) { "$FIELD_FIELD is null" },
                type = requireNotNull(type) { "$TYPE_FIELD is null" },
            )
        }
    }
}
