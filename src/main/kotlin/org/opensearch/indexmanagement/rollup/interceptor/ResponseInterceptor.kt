/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.interceptor

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.settings.Settings
import org.opensearch.transport.TransportInterceptor
import org.opensearch.transport.TransportResponse
import org.opensearch.transport.TransportRequest
import org.opensearch.transport.Transport
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportResponseHandler
import org.opensearch.transport.TransportException

class ResponseInterceptor(
    val clusterService: ClusterService,
    val settings: Settings,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportInterceptor {
    private val logger = LogManager.getLogger(javaClass)

    override fun interceptSender(sender: TransportInterceptor.AsyncSender): TransportInterceptor.AsyncSender {
        return CustomAsyncSender(sender)
    }

    private inner class CustomAsyncSender(private val originalSender: TransportInterceptor.AsyncSender) : TransportInterceptor.AsyncSender {

        override fun <T : TransportResponse?> sendRequest(
            connection: Transport.Connection?,
            action: String?,
            request: TransportRequest?,
            options: TransportRequestOptions?,
            handler: TransportResponseHandler<T>?
        ) {
            val interceptedHandler = CustomResponseHandler(handler)

            originalSender.sendRequest(connection, action, request, options, interceptedHandler)
        }
    }

    private inner class CustomResponseHandler<T : TransportResponse?>(
        private val originalHandler: TransportResponseHandler<T>?
    ) : TransportResponseHandler<T> {
        override fun read(inStream: StreamInput?): T {
            val response = originalHandler?.read(inStream)
            // Modify the response if necessary
            return response!!
        }

        override fun handleResponse(response: T?) {
            // Handle the response or delegate to the original handler
//            logger.error("ronsax response interceptoed!! $response")
            originalHandler?.handleResponse(response)
        }

        override fun handleException(exp: TransportException?) {
            // Handle exceptions or delegate to the original handler
            originalHandler?.handleException(exp)
        }

        override fun executor(): String {
            return originalHandler?.executor() ?: ""
        }
    }
}
