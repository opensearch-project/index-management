/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.indexmanagement.adminpanel.notification.filter.NotificationActionListener
import java.lang.Exception

class ForceMergeRespParser(val request: ForceMergeRequest) : ResponseParser<ForceMergeResponse> {
    override fun buildNotificationMessage(
        response: ForceMergeResponse,
        exception: Exception?,
        isTimeout: Boolean
    ): String {

        val result = StringBuilder()
        result.append(
            "force_merge for index [${request.indices().joinToString(",")}] " +
                if (response.shardFailures != null && response.shardFailures.isNotEmpty()) {
                    "${NotificationActionListener.COMPLETED_WITH_ERROR} ${
                    response.shardFailures.joinToString(",") { it.reason() }
                    }"
                } else {
                    NotificationActionListener.COMPLETED
                }
        )

        return result.toString()
    }
}
