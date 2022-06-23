/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.destination

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.destination.message.LegacyChimeMessage
import org.opensearch.commons.destination.message.LegacyCustomWebhookMessage
import org.opensearch.commons.destination.message.LegacySlackMessage
import org.opensearch.indexmanagement.opensearchapi.convertToMap
import java.io.IOException

enum class DestinationType(val value: String) {
    CHIME("chime"),
    SLACK("slack"),
    CUSTOM_WEBHOOK("custom_webhook"),
}

data class Destination(
    val type: DestinationType,
    val chime: Chime?,
    val slack: Slack?,
    val customWebhook: CustomWebhook?
) : ToXContentObject, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(type.value, constructResponseForDestinationType(type))
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readEnum(DestinationType::class.java),
        sin.readOptionalWriteable(::Chime),
        sin.readOptionalWriteable(::Slack),
        sin.readOptionalWriteable(::CustomWebhook)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeEnum(type)
        out.writeOptionalWriteable(chime)
        out.writeOptionalWriteable(slack)
        out.writeOptionalWriteable(customWebhook)
    }

    companion object {
        const val CHIME = "chime"
        const val SLACK = "slack"
        const val CUSTOMWEBHOOK = "custom_webhook"

        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Destination {
            var slack: Slack? = null
            var chime: Chime? = null
            var customWebhook: CustomWebhook? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    CHIME -> chime = Chime.parse(xcp)
                    SLACK -> slack = Slack.parse(xcp)
                    CUSTOMWEBHOOK -> customWebhook = CustomWebhook.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Destination.")
                }
            }

            val type = when {
                chime != null -> DestinationType.CHIME
                slack != null -> DestinationType.SLACK
                customWebhook != null -> DestinationType.CUSTOM_WEBHOOK
                else -> throw IllegalArgumentException("Must specify a destination type")
            }

            return Destination(
                type,
                chime,
                slack,
                customWebhook
            )
        }
    }

    @Throws(IOException::class)
    fun buildLegacyBaseMessage(compiledSubject: String?, compiledMessage: String): LegacyBaseMessage {
        val destinationMessage: LegacyBaseMessage
        when (type) {
            DestinationType.CHIME -> {
                val messageContent = chime?.constructMessageContent(compiledSubject, compiledMessage)
                destinationMessage = LegacyChimeMessage.Builder("chime_message")
                    .withUrl(chime?.url)
                    .withMessage(messageContent)
                    .build()
            }
            DestinationType.SLACK -> {
                val messageContent = slack?.constructMessageContent(compiledSubject, compiledMessage)
                destinationMessage = LegacySlackMessage.Builder("slack_message")
                    .withUrl(slack?.url)
                    .withMessage(messageContent)
                    .build()
            }
            DestinationType.CUSTOM_WEBHOOK -> {
                destinationMessage = LegacyCustomWebhookMessage.Builder("custom_webhook")
                    .withUrl(getLegacyCustomWebhookMessageURL(customWebhook, compiledMessage))
                    .withHeaderParams(customWebhook?.headerParams)
                    .withMessage(compiledMessage).build()
            }
        }
        return destinationMessage
    }

    fun constructResponseForDestinationType(type: DestinationType): Any {
        var content: Any? = null
        when (type) {
            DestinationType.CHIME -> content = chime?.convertToMap()?.get(type.value)
            DestinationType.SLACK -> content = slack?.convertToMap()?.get(type.value)
            DestinationType.CUSTOM_WEBHOOK -> content = customWebhook?.convertToMap()?.get(type.value)
        }
        if (content == null) {
            throw IllegalArgumentException("Content is NULL for destination type ${type.value}")
        }
        return content
    }

    private fun getLegacyCustomWebhookMessageURL(customWebhook: CustomWebhook?, compiledMessage: String): String {
        return LegacyCustomWebhookMessage.Builder("custom_webhook")
            .withUrl(customWebhook?.url)
            .withScheme(customWebhook?.scheme)
            .withHost(customWebhook?.host)
            .withPort(customWebhook?.port)
            .withPath(customWebhook?.path)
            .withQueryParams(customWebhook?.queryParams)
            .withMessage(compiledMessage)
            .build().uri.toString()
    }
}
