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
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.indexmanagement.util.SecurityUtils
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
        log.debug(
            "user and roles string from thread context: " +
                client.threadPool().threadContext.getTransient<String>(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        )
        val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
        coroutineScope.launch {
            try {
                client.threadPool().threadContext.stashContext().use { threadContext ->
                    listener.onResponse(executeRequest(request, user, threadContext))
                }
            } catch (ex: IndexManagementException) {
                listener.onFailure(ex)
            } catch (ex: VersionConflictEngineException) {
                listener.onFailure(ex)
            } catch (ex: OpenSearchStatusException) {
                listener.onFailure(ex)
            } catch (ex: Exception) {
                log.error("Uncaught exception:", ex)
                listener.onFailure(OpenSearchStatusException(ex.message, RestStatus.INTERNAL_SERVER_ERROR))
            }
        }
    }

    abstract suspend fun executeRequest(request: Request, user: User?, threadContext: StoredContext): Response
}
