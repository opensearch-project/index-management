/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.filter.parser

import org.opensearch.action.ActionResponse
import org.opensearch.common.collect.Tuple
import org.opensearch.indexmanagement.adminpanel.notification.filter.OperationResult
import java.lang.Exception
import java.util.function.Consumer

interface ResponseParser<Response : ActionResponse> {
    fun parseAndSendNotification(
        response: Response,
        callback: Consumer<Tuple<OperationResult, String>>
    ) {
        callback.accept(Tuple.tuple(OperationResult.COMPLETE, buildNotificationMessage(response)))
    }

    fun buildNotificationMessage(
        response: Response,
        exception: Exception? = null,
        isTimeout: Boolean = false
    ): String
}
