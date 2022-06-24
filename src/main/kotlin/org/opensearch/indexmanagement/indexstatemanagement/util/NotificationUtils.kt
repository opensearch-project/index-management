/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("NotificationUtils")
package org.opensearch.indexmanagement.indexstatemanagement.util

import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.authuser.User
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.LegacyPublishNotificationRequest
import org.opensearch.commons.notifications.action.LegacyPublishNotificationResponse
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.common.model.notification.Channel
import org.opensearch.indexmanagement.common.model.notification.validateResponseStatus
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
 * Extension function for publishing a notification to a channel in the Notification plugin. Builds the event source directly
 * from the managed index metadata.
 */
suspend fun Channel.sendNotification(
    client: Client,
    title: String,
    managedIndexMetaData: ManagedIndexMetaData,
    compiledMessage: String,
    user: User?
) {
    val eventSource = managedIndexMetaData.getEventSource(title)
    this.sendNotification(client, eventSource, compiledMessage, user)
}

fun ManagedIndexMetaData.getEventSource(title: String): EventSource {
    return EventSource(title, indexUuid, SeverityType.INFO)
}
