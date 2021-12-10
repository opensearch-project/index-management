/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.util

import org.opensearch.OpenSearchException
import org.opensearch.common.Strings
import org.opensearch.common.ValidationException
import org.opensearch.index.IndexNotFoundException
import org.opensearch.rest.RestStatus
import java.lang.IllegalArgumentException

class IndexManagementException(message: String, val status: RestStatus, ex: Exception) : OpenSearchException(message, ex) {

    override fun status(): RestStatus {
        return status
    }

    companion object {
        @JvmStatic
        fun wrap(ex: Exception): OpenSearchException {

            var friendlyMsg = ex.message as String
            var status = RestStatus.INTERNAL_SERVER_ERROR
            when (ex) {
                is IndexNotFoundException -> {
                    status = ex.status()
                    friendlyMsg = "Configuration index not found"
                }
                is IllegalArgumentException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                is ValidationException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                else -> {
                    if (!Strings.isNullOrEmpty(ex.message)) {
                        friendlyMsg = ex.message as String
                    }
                }
            }

            return IndexManagementException(friendlyMsg, status, Exception("${ex.javaClass.name}: ${ex.message}"))
        }
    }
}
