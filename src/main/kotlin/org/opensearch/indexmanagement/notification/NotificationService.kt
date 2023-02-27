/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.notification

import org.apache.logging.log4j.LogManager
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.client.Client
import org.opensearch.indexmanagement.notification.model.NotificationConf
import org.opensearch.indexmanagement.opensearchapi.suspendUntil

class NotificationService(val client: Client) {

    private val logger = LogManager.getLogger(NotificationService::class.java)

    public suspend fun getNotificationConfByAction(taskId: String?, action: String): NotificationConf? {
        logger.info("Get notification channel for task {} and action {}", taskId, action)
        var getRequest = MultiGetRequest()
            .add("", taskId)
            .add("", action)
            .add("", "default_id")

        val multiGetResponse: MultiGetResponse = client.suspendUntil {
            multiGet(getRequest, it)
        }

        val c1 = multiGetResponse.filter { it.id == taskId }.singleOrNull()
        logger.info(c1)

        return NotificationConf("SsPLbYYBwCDAm0J8Rfsr", action)
    }
}
