/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.commons.authuser.User
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.opensearchapi.optionalUserField
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.script.Script
import java.io.IOException

data class LRONConfig(
    val destination: Destination?,
    val channel: Channel?,
    val user: User?,
    val enabled: Boolean,
    val successMessageTemplate: Script,
    val failedMessageTemplate: Script
) : ToXContentObject, Writeable {

    init {
        if (enabled)
            require(destination != null || channel != null) { "Enabled LRONConfig must contain a destination or channel" }
        require(destination == null || channel == null) { "LRONConfig can only contain a single destination or channel" }
        require(successMessageTemplate.lang == MUSTACHE && failedMessageTemplate.lang == MUSTACHE) { "LRONConfig message template must be a mustache script" }
    }

    fun toXContent(builder: XContentBuilder): XContentBuilder {
        return toXContent(builder, ToXContent.EMPTY_PARAMS)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(LRON_CONFIG_FIELD)
        if (destination != null) builder.field(DESTINATION_FIELD, destination)
        if (channel != null) builder.field(CHANNEL_FIELD, channel)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(Policy.USER_FIELD, user)
        builder.field(ENABLED_FIELD, enabled)
            .field(SUCCESS_MESSAGE_TEMPLATE_FIELD, successMessageTemplate)
            .field(FAILED_MESSAGE_TEMPLATE_FIELD, failedMessageTemplate)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        destination = sin.readOptionalWriteable(::Destination),
        channel = sin.readOptionalWriteable(::Channel),
        user = sin.readOptionalWriteable(::User),
        enabled = sin.readBoolean(),
        successMessageTemplate = Script(sin),
        failedMessageTemplate = Script(sin)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalWriteable(destination)
        out.writeOptionalWriteable(channel)
        out.writeOptionalWriteable(user)
        out.writeBoolean(enabled)
        successMessageTemplate.writeTo(out)
        failedMessageTemplate.writeTo(out)
    }

    companion object {
        const val LRON_CONFIG_FIELD = "lron_config"
        const val DESTINATION_FIELD = "destination"
        const val CHANNEL_FIELD = "channel"
        const val USER_FIELD = "user"
        const val ENABLED_FIELD = "enabled"
        const val SUCCESS_MESSAGE_TEMPLATE_FIELD = "success_message_template"
        const val FAILED_MESSAGE_TEMPLATE_FIELD = "failed_message_template"
        const val MUSTACHE = "mustache"
        const val CHANNEL_TITLE = "Long Running Operation Notification"
        const val DEFAULT_ENABLED = true

        /* to fit with ISM XContentParser.parseWithType function */
        @JvmStatic
        @Throws(IOException::class)
        @Suppress("UNUSED_PARAMETER")
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        ): LRONConfig {
            return parse(xcp)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): LRONConfig {
            var destination: Destination? = null
            var channel: Channel? = null
            var user: User? = null
            var enabled: Boolean = DEFAULT_ENABLED
            var successMessageTemplate: Script? = null
            var failedMessageTemplate: Script? = null

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    DESTINATION_FIELD -> destination = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else Destination.parse(xcp)
                    CHANNEL_FIELD -> channel = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else Channel.parse(xcp)
                    USER_FIELD -> user = if (xcp.currentToken() == XContentParser.Token.VALUE_NULL) null else User.parse(xcp)
                    SUCCESS_MESSAGE_TEMPLATE_FIELD -> successMessageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    FAILED_MESSAGE_TEMPLATE_FIELD -> failedMessageTemplate = Script.parse(xcp, Script.DEFAULT_TEMPLATE_LANG)
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in LRONConfig.")
                }
            }

            return LRONConfig(
                destination = destination,
                channel = channel,
                user = user,
                enabled = enabled,
                successMessageTemplate = requireNotNull(successMessageTemplate) { "LRONConfig success message template is null" },
                failedMessageTemplate = requireNotNull(failedMessageTemplate) { "LRONConfig failed message template is null" }
            )
        }
    }
}
