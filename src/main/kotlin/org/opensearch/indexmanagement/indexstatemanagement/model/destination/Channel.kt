/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * com.maddyhome.idea.copyright.pattern.CommentInfo@3cc71a1b
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.destination

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class Channel(val id: String) : ToXContent, Writeable {

    init {
        require(id.isNotEmpty()) { "Channel ID cannot be empty" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(ID, id)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
    }

    companion object {
        const val ID = "id"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Channel {
            var id: String? = null

            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    ID -> id = xcp.text()
                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing Channel destination")
                    }
                }
            }

            return Channel(requireNotNull(id) { "Channel ID is null" })
        }
    }
}
