/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.apache.logging.log4j.Logger
import org.opensearch.client.Client
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.commons.authuser.User
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.common.model.notification.Channel
import java.io.IOException

/*
 * The data model for the configuration of notifications within a Snapshot Management policy definition.
 */
data class NotificationConfig(
    val channel: Channel,
    val conditions: Conditions,
) : ToXContentObject, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(CHANNEL_FIELD, channel)
            .field(CONDITIONS_FIELD, conditions)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        Channel(sin),
        Conditions(sin)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        channel.writeTo(out)
        conditions.writeTo(out)
    }

    suspend fun sendCreationNotification(client: Client, policyName: String, message: String, user: User?, log: Logger) {
        if (this.conditions.creation) {
            try {
                val eventSource = EventSource(CREATION_TITLE, policyName, SeverityType.INFO)
                channel.sendNotification(client, eventSource, message, user)
            } catch (e: Exception) {
                // If we succeeded to create the snapshot but failed to send the notification, just log
                log.error("Failed to send snapshot management creation notification for [$policyName]", e)
            }
        }
    }

    suspend fun sendDeletionNotification(client: Client, policyName: String, message: String, user: User?, log: Logger) {
        if (this.conditions.deletion) {
            try {
                val eventSource = EventSource(DELETION_TITLE, policyName, SeverityType.INFO)
                channel.sendNotification(client, eventSource, message, user)
            } catch (e: Exception) {
                // If we succeeded to delete the snapshot but failed to send the notification, just log
                log.error("Failed to send snapshot management deletion notification for [$policyName]", e)
            }
        }
    }

    suspend fun sendTimeLimitExceededNotification(
        client: Client,
        policyName: String,
        message: String,
        user: User?,
        log: Logger
    ) {
        if (this.conditions.timeLimitExceeded) {
            try {
                val eventSource = EventSource(TIME_LIMIT_EXCEEDED_TITLE, policyName, SeverityType.INFO)
                channel.sendNotification(client, eventSource, message, user)
            } catch (e: Exception) {
                log.error("Failed to send snapshot management time limit exceeded notification for [$policyName]", e)
            }
        }
    }

    suspend fun sendFailureNotification(client: Client, policyName: String, message: String, user: User?, log: Logger) {
        if (this.conditions.failure) {
            try {
                val eventSource = EventSource(FAILURE_TITLE, policyName, SeverityType.INFO)
                channel.sendNotification(client, eventSource, message, user)
            } catch (e: Exception) {
                // We already failed, just log the notification error
                log.error("Failed to send snapshot management failure notification for [$policyName]", e)
            }
        }
    }

    companion object {
        const val CHANNEL_FIELD = "channel"
        const val CONDITIONS_FIELD = "conditions"
        const val CREATION_TITLE = "Snapshot Management - Snapshot Creation Notification"
        const val DELETION_TITLE = "Snapshot Management - Snapshot Deletion Notification"
        const val FAILURE_TITLE = "Snapshot Management - Snapshot Failure Notification"
        const val TIME_LIMIT_EXCEEDED_TITLE = "Snapshot Management - Snapshot Time Limit Notification"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): NotificationConfig {
            var channel: Channel? = null
            var conditions: Conditions? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    CHANNEL_FIELD -> channel = Channel.parse(xcp)
                    CONDITIONS_FIELD -> conditions = Conditions.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Snapshot Management notification config.")
                }
            }

            return NotificationConfig(
                channel = requireNotNull(channel) { "Snapshot Management notification channel must not be null" },
                conditions = conditions ?: Conditions()
            )
        }
    }

    data class Conditions(
        val creation: Boolean = true,
        val deletion: Boolean = false,
        val failure: Boolean = false,
        val timeLimitExceeded: Boolean = false
    ) : Writeable, ToXContent {

        override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
            return builder.startObject()
                .field(CREATION_FIELD, creation)
                .field(DELETION_FIELD, deletion)
                .field(FAILURE_FIELD, failure)
                .field(TIME_LIMIT_EXCEEDED_FIELD, timeLimitExceeded)
                .endObject()
        }

        companion object {
            const val CREATION_FIELD = "creation"
            const val DELETION_FIELD = "deletion"
            const val FAILURE_FIELD = "failure"
            const val TIME_LIMIT_EXCEEDED_FIELD = "time_limit_exceeded"

            fun parse(xcp: XContentParser): Conditions {
                var creation = true
                var deletion = false
                var failure = false
                var timeLimitExceeded = false

                ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                while (xcp.nextToken() != Token.END_OBJECT) {
                    val fieldName = xcp.currentName()
                    xcp.nextToken()

                    when (fieldName) {
                        CREATION_FIELD -> creation = xcp.booleanValue()
                        DELETION_FIELD -> deletion = xcp.booleanValue()
                        FAILURE_FIELD -> failure = xcp.booleanValue()
                        TIME_LIMIT_EXCEEDED_FIELD -> timeLimitExceeded = xcp.booleanValue()
                        else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in conditions.")
                    }
                }

                return Conditions(creation, deletion, failure, timeLimitExceeded)
            }
        }

        constructor(sin: StreamInput) : this(
            creation = sin.readBoolean(),
            deletion = sin.readBoolean(),
            failure = sin.readBoolean(),
            timeLimitExceeded = sin.readBoolean(),
        )

        override fun writeTo(out: StreamOutput) {
            out.writeBoolean(creation)
            out.writeBoolean(deletion)
            out.writeBoolean(failure)
            out.writeBoolean(timeLimitExceeded)
        }
    }
}
