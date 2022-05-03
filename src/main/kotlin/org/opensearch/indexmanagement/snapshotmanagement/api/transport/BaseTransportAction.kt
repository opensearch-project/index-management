/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext
import org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
import org.opensearch.commons.authuser.User
import org.opensearch.commons.utils.logger
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

abstract class BaseTransportAction<Request : ActionRequest, Response : ActionResponse>(
    name: String,
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    requestReader: Writeable.Reader<Request>,
) : HandledTransportAction<Request, Response>(
    name, transportService, actionFilters, requestReader
) {

    private val log = LogManager.getLogger(javaClass)
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.IO)

    override fun doExecute(
        task: Task,
        request: Request,
        listener: ActionListener<Response>
    ) {
        val userStr: String? =
            client.threadPool().threadContext.getTransient<String>(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.info("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)
        coroutineScope.launch {
            try {
                client.threadPool().threadContext.stashContext().use { threadContext ->
                    listener.onResponse(executeRequest(request, user, threadContext))
                }
            } catch (ex: Exception) {
                log.error("Uncaught exception:", ex)
                listener.onFailure(
                    OpenSearchStatusException(
                        ex.message, RestStatus.INTERNAL_SERVER_ERROR
                    )
                )
            }
        }
    }

    // TODO could we get userStr from threadContext?
    abstract suspend fun executeRequest(request: Request, user: User?, threadContext: StoredContext): Response
}
