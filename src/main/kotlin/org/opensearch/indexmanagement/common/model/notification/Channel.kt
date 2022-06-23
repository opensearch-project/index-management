/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.common.model.notification

import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.generateUserString
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

    /**
     * Extension function for publishing a notification to a channel in the Notification plugin.
     */
    suspend fun sendNotification(
        client: Client,
        eventSource: EventSource,
        message: String,
        user: User?
    ) {
        val channel = this
        client.threadPool().threadContext.stashContext().use {
            // We need to set the user context information in the thread context for notification plugin to correctly resolve the user object
            client.threadPool().threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, generateUserString(user))
            val res: SendNotificationResponse = NotificationsPluginInterface.suspendUntil {
                this.sendNotification(
                    (client as NodeClient),
                    eventSource,
                    ChannelMessage(message, null, null),
                    listOf(channel.id),
                    it
                )
            }
            validateResponseStatus(res.getStatus(), res.notificationEvent.eventSource.referenceId)
        }
    }
}
