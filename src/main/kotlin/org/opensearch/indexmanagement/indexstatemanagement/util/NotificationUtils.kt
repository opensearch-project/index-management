/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("NotificationUtils")
package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.OpenSearchStatusException
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.LegacyPublishNotificationRequest
import org.opensearch.commons.notifications.action.LegacyPublishNotificationResponse
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Channel
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.rest.RestStatus

/**
 * Extension function for publishing a notification to a legacy destination.
 *
 * We now support the new channels from the Notification plugin. But, we still need to support
 * the old embedded legacy destinations that are directly on the policies in the error notifications
 * or notification actions. So we have a separate API in the NotificationsPluginInterface that allows
 * us to publish these old legacy ones directly.
 */
suspend fun LegacyBaseMessage.publishLegacyNotification(client: Client) {
    val baseMessage = this
    val res: LegacyPublishNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.publishLegacyNotification(
            (client as NodeClient),
            LegacyPublishNotificationRequest(baseMessage),
            it
        )
    }
    validateResponseStatus(RestStatus.fromCode(res.destinationResponse.statusCode), res.destinationResponse.responseContent)
}

/**
 * Extension function for publishing a notification to a channel in the Notification plugin.
 */
suspend fun Channel.sendNotification(client: Client, title: String, managedIndexMetaData: ManagedIndexMetaData, compiledMessage: String) {
    val channel = this
    val res: SendNotificationResponse = NotificationsPluginInterface.suspendUntil {
        this.sendNotification(
            (client as NodeClient),
            managedIndexMetaData.getEventSource(title),
            ChannelMessage(compiledMessage, null, null),
            listOf(channel.id),
            it
        )
    }
    validateResponseStatus(res.getStatus(), res.notificationEvent.eventSource.referenceId)
}

fun ManagedIndexMetaData.getEventSource(title: String): EventSource {
    return EventSource(title, indexUuid, SeverityType.INFO)
}

/**
 * all valid response status
 */
private val VALID_RESPONSE_STATUS = setOf(
    RestStatus.OK.status, RestStatus.CREATED.status, RestStatus.ACCEPTED.status,
    RestStatus.NON_AUTHORITATIVE_INFORMATION.status, RestStatus.NO_CONTENT.status,
    RestStatus.RESET_CONTENT.status, RestStatus.PARTIAL_CONTENT.status,
    RestStatus.MULTI_STATUS.status
)

@Throws(OpenSearchStatusException::class)
fun validateResponseStatus(restStatus: RestStatus, responseContent: String) {
    if (!VALID_RESPONSE_STATUS.contains(restStatus.status)) {
        throw OpenSearchStatusException("Failed: $responseContent", restStatus)
    }
}
