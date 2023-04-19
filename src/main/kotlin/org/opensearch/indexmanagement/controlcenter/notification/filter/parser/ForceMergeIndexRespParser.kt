/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.filter.parser

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse
import org.opensearch.common.collect.Tuple
import org.opensearch.indexmanagement.controlcenter.notification.filter.NotificationActionListener
import org.opensearch.indexmanagement.controlcenter.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer

class ForceMergeIndexRespParser(val request: ForceMergeRequest) : ResponseParser<ForceMergeResponse> {

    override fun parseAndSendNotification(
        response: ForceMergeResponse,
        callback: Consumer<Tuple<OperationResult, String>>
    ) {
        val hasFailures = response.shardFailures != null && response.shardFailures.isNotEmpty()
        if (hasFailures) {
            callback.accept(Tuple(OperationResult.FAILED, buildNotificationMessage(response)))
        } else {
            callback.accept(Tuple(OperationResult.COMPLETE, buildNotificationMessage(response)))
        }
    }

    override fun buildNotificationMessage(
        response: ForceMergeResponse,
        exception: Exception?,
        isTimeout: Boolean
    ): String {

        val result = StringBuilder()
        result.append(
            "Force_merge for index [${request.indices().joinToString(",")}] " +
                if (response.shardFailures != null && response.shardFailures.isNotEmpty()) {
                    "${NotificationActionListener.FAILED} ${
                    response.shardFailures.joinToString(",") { it.reason() }
                    }"
                } else {
                    NotificationActionListener.COMPLETED
                }
        )

        return result.toString()
    }
}
