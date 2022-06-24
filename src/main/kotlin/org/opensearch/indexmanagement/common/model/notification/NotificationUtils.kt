/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

@file:JvmName("NotificationUtils")
package org.opensearch.indexmanagement.common.model.notification

import org.opensearch.OpenSearchStatusException
import org.opensearch.rest.RestStatus

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
