/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.core.common.Strings
import org.opensearch.common.logging.DeprecationLogger
import org.opensearch.core.xcontent.MediaType
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.TYPE_PARAM_KEY
import org.opensearch.indexmanagement.indexstatemanagement.util.parseClusterManagerTimeout
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener

class RestRetryFailedManagedIndexAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, RETRY_BASE_URI,
                POST, LEGACY_RETRY_BASE_URI
            ),
            ReplacedRoute(
                POST, "$RETRY_BASE_URI/{index}",
                POST, "$LEGACY_RETRY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "retry_failed_managed_index"
    }

    @Suppress("SpreadOperator") // There is no way around dealing with java vararg without spread operator.
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing indices")
        }
        val body = if (request.hasContent()) {
            XContentHelper.convertToMap(request.requiredContent(), false, request.xContentType as (MediaType)).v2()
        } else {
            mapOf()
        }

        val indexType = request.param(TYPE_PARAM_KEY, DEFAULT_INDEX_TYPE)

        val clusterManagerTimeout = parseClusterManagerTimeout(
            request, DeprecationLogger.getLogger(RestRetryFailedManagedIndexAction::class.java), name
        )

        val retryFailedRequest = RetryFailedManagedIndexRequest(
            indices.toList(), body["state"] as String?,
            clusterManagerTimeout,
            indexType
        )

        return RestChannelConsumer { channel ->
            client.execute(RetryFailedManagedIndexAction.INSTANCE, retryFailedRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val RETRY_BASE_URI = "$ISM_BASE_URI/retry"
        const val LEGACY_RETRY_BASE_URI = "$LEGACY_ISM_BASE_URI/retry"
    }
}
