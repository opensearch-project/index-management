/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.fielddomain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.launch
import org.opensearch.action.admin.indices.refresh.RefreshRequest
import org.opensearch.action.admin.indices.refresh.RefreshResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.ContextPreservingActionListener
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.Index
import org.opensearch.index.IndexNotFoundException
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainCalculatorRegistry
import org.opensearch.indexmanagement.indexstatemanagement.fielddomain.FieldDomainSearchContext
import org.opensearch.indexmanagement.indexstatemanagement.util.isIndexWriteBlocked
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import kotlin.coroutines.CoroutineContext

/** Security's action filters authorize the concrete index before this handler can dispatch any privileged search. */
class TransportComputeFieldDomainsAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val client: Client,
    private val clusterService: ClusterService,
) : HandledTransportAction<ComputeFieldDomainsRequest, ComputeFieldDomainsResponse>(
    ComputeFieldDomainsAction.NAME, transportService, actionFilters, ::ComputeFieldDomainsRequest,
) {
    override fun doExecute(task: Task, request: ComputeFieldDomainsRequest, listener: ActionListener<ComputeFieldDomainsResponse>) {
        val threadContext = client.threadPool().threadContext
        val preservedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext)
        CoroutineScope(Dispatchers.IO + CallerContext(threadContext)).launch {
            try {
                preservedListener.onResponse(compute(request))
            } catch (e: Exception) {
                preservedListener.onFailure(e)
            }
        }
    }

    private suspend fun compute(request: ComputeFieldDomainsRequest): ComputeFieldDomainsResponse {
        val indexMetadata = currentIndex(request.targetIndex)
        check(isIndexWriteBlocked(indexMetadata)) { "Field-domain computation requires an index write block" }
        check(indexMetadata.state == IndexMetadata.State.OPEN) { "Field-domain computation requires an open index" }
        val registry = FieldDomainCalculatorRegistry.defaultRegistry()
        val calculators = request.fields.map { config ->
            requireNotNull(registry.get(config.type)) { "No field-domain calculator registered for type [${config.type}]" }
        }
        val refreshed: RefreshResponse = client.admin().indices().suspendUntil {
            refresh(RefreshRequest(request.targetIndex.name), it)
        }
        check(refreshed.failedShards == 0) { "Failed to refresh index [${request.targetIndex.name}] before computing field domains" }

        val searchContext = FieldDomainSearchContext(request.targetIndex.name, client, client.threadPool().threadContext)
        val domains = request.fields.zip(calculators).mapNotNull { (config, calculator) ->
            calculator.calculate(searchContext, indexMetadata, config, true)
        }
        // A delete/recreate or an intervening settings change invalidates the computation.
        val latest = currentIndex(request.targetIndex)
        check(latest.settingsVersion == indexMetadata.settingsVersion && isIndexWriteBlocked(latest)) {
            "Index settings changed during field-domain computation"
        }
        return ComputeFieldDomainsResponse(IndexFieldDomainMetadata.getInstance().toCustomData(domains))
    }

    private fun currentIndex(index: Index): IndexMetadata {
        val metadata = clusterService.state().metadata.index(index.name) ?: throw IndexNotFoundException(index)
        require(metadata.index == index) { "Index UUID changed before field-domain computation" }
        return metadata
    }

    /** Preserve the already-authorized caller, including injected policy roles, across coroutine thread switches. */
    private class CallerContext(threadContext: ThreadContext) : ThreadContextElement<ThreadContext.StoredContext> {
        private val restore = threadContext.newRestorableContext(true)
        override val key: CoroutineContext.Key<*> = Key

        override fun updateThreadContext(context: CoroutineContext): ThreadContext.StoredContext = restore.get()

        override fun restoreThreadContext(context: CoroutineContext, oldState: ThreadContext.StoredContext) = oldState.close()

        companion object Key : CoroutineContext.Key<CallerContext>
    }
}
