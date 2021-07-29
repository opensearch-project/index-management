/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

@file:JvmName("NotificationUtils")
package org.opensearch.indexmanagement.indexstatemanagement.util

import org.apache.logging.log4j.Logger
import org.opensearch.action.ActionListener
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.commons.destination.message.LegacyBaseMessage
import org.opensearch.commons.notifications.NotificationsPluginInterface
import org.opensearch.commons.notifications.action.LegacyPublishNotificationRequest
import org.opensearch.commons.notifications.action.LegacyPublishNotificationResponse
import org.opensearch.commons.notifications.action.SendNotificationResponse
import org.opensearch.commons.notifications.model.ChannelMessage
import org.opensearch.commons.notifications.model.EventSource
import org.opensearch.commons.notifications.model.Feature
import org.opensearch.commons.notifications.model.SeverityType
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Channel

/**
 * Extension function for publishing a notification to a legacy destination.
 *
 * We now support the new channels from the Notification plugin. But, we still need to support
 * the old embedded legacy destinations that are directly on the policies in the error notifications
 * or notification actions. So we have a separate API in the NotificationsPluginInterface that allows
 * us to publish these old legacy ones directly.
 */
fun LegacyBaseMessage.publishLegacyNotification(client: Client, logger: Logger) {
    val channelType = this.channelType
    NotificationsPluginInterface.publishLegacyNotification(
        (client as NodeClient),
        // TODO: Have Notification plugin handle the host deny list which they have access to in the cluster settings
        LegacyPublishNotificationRequest(channelType, this, emptyList(), Feature.INDEX_MANAGEMENT),
        object : ActionListener<LegacyPublishNotificationResponse> {
            override fun onResponse(response: LegacyPublishNotificationResponse) {
                logger.info("Message published for action type: ${channelType}, messageid: ${response.destinationResponse.responseContent}, statuscode: ${response.destinationResponse.statusCode}")
            }

            override fun onFailure(e: Exception) {
                logger.error("Notification failed to publish to destination", e)
                throw e
            }
        }
    )
}

/**
 * Extension function for publishing a notification to a channel in the Notification plugin.
 */
fun Channel.sendNotification(client: Client, logger: Logger, title: String, managedIndexMetaData: ManagedIndexMetaData, compiledMessage: String) {
    val channelId = this.id
    NotificationsPluginInterface.sendNotification(
        (client as NodeClient),
        EventSource(title, managedIndexMetaData.indexUuid, Feature.INDEX_MANAGEMENT, SeverityType.INFO),
        ChannelMessage(compiledMessage, null, null),
        listOf(channelId),
        object : ActionListener<SendNotificationResponse> {
            override fun onResponse(response: SendNotificationResponse) {
                logger.info("Error notification successfully published to channel $channelId, notificationId: ${response.notificationId}")
            }

            override fun onFailure(e: Exception) {
                logger.error("Error notification failed to publish to channel $channelId", e)
                throw e
            }
        }
    )
}