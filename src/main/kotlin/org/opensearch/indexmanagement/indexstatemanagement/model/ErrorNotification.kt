/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.opensearch.script.Script
import java.io.IOException

data class ErrorNotification(
    val destination: Destination?,
    val channel: Channel?,
    val messageTemplate: Script
) : ToXContentObject, Writeable {

    init {
        require(destination != null || channel != null) { "ErrorNotification must contain a destination or channel" }
        require(destination == null || channel == null) { "ErrorNotification can only contain a single destination or channel" }
        require(messageTemplate.lang == MUSTACHE) { "ErrorNotification message template must be a mustache script" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (destination != null) builder.field(DESTINATION_FIELD, destination)
        if (channel != null) builder.field(CHANNEL_FIELD, channel)
        return builder
            .field(MESSAGE_TEMPLATE_FIELD, messageTemplate)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readOptionalWriteable(::Destination),
        sin.readOptionalWriteable(::Channel),
        Script(sin)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalWriteable(destination)
        out.writeOptionalWriteable(channel)
        messageTemplate.writeTo(out)
    }

    companion object {
        const val DESTINATION_FIELD = "destination"
        const val CHANNEL_FIELD = "channel"
        const val MESSAGE_TEMPLATE_FIELD = "message_template"
        const val MUSTACHE = "mustache"
        const val CHANNEL_TITLE = "Index Management-ISM-Error Notification"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ErrorNotification {
            var destination: Destination? = null
            var channel: Channel? = null
            var messageTemplate: Script? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DESTINATION_FIELD -> destination = if (xcp.currentToken() == Token.VALUE_NULL) null else Destination.parse(xcp)
                    CHANNEL_FIELD -> channel = if (xcp.currentToken() == Token.VALUE_NULL) null else Channel.parse(xcp)
                    MESSAGE_TEMPLATE_FIELD -> messageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ErrorNotification.")
                }
            }

            return ErrorNotification(
                destination = destination,
                channel = channel,
                messageTemplate = requireNotNull(messageTemplate) { "ErrorNotification message template is null" }
            )
        }
    }
}
