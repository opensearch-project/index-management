/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchSecurityException
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListenerResponseHandler
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.ChannelActionListener
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import org.opensearch.indexmanagement.indexstatemanagement.ExecuteNowResult
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.indexmanagement.indexstatemanagement.opensearchapi.getManagedIndexConfig
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.ISMStatusResponse
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.util.IndexManagementException
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportRequestHandler
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(TransportExecuteManagedIndexAction::class.java)

@Suppress("LongParameterList")
class TransportExecuteManagedIndexAction
@Inject
constructor(
    val client: NodeClient,
    val transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    // the job scheduler plugin binds this to the instance it takes the locks of scheduled runs with
    val lockService: LockService,
    val indexMetadataProvider: IndexMetadataProvider,
) : HandledTransportAction<ExecuteManagedIndexRequest, ISMStatusResponse>(
    ExecuteManagedIndexAction.NAME, transportService, actionFilters, ::ExecuteManagedIndexRequest,
) {
    init {
        // The hop to the node that owns the job is an internal action of its own: it cannot be told to skip the permission check
        // the public action does
        transportService.registerRequestHandler(
            ExecuteManagedIndexAction.NODE_ACTION_NAME, ThreadPool.Names.SAME, ::ExecuteManagedIndexRequest,
            TransportRequestHandler<ExecuteManagedIndexRequest> { request, channel, _ ->
                executeOnThisNode(request.index, ChannelActionListener(channel, ExecuteManagedIndexAction.NODE_ACTION_NAME, request))
            },
        )
    }

    // Permissions come before anything that tells the caller whether the index exists: no access is a 403, not a 404
    override fun doExecute(task: Task, request: ExecuteManagedIndexRequest, listener: ActionListener<ISMStatusResponse>) {
        client.execute(
            ManagedIndexAction.INSTANCE,
            ManagedIndexRequest().indices(request.index),
            object : ActionListener<AcknowledgedResponse> {
                override fun onResponse(response: AcknowledgedResponse) {
                    route(request.index, listener)
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(toFailure(e))
                }
            },
        )
    }

    private fun route(indexName: String, listener: ActionListener<ISMStatusResponse>) {
        val indexMetadata = clusterService.state().metadata().index(indexName)
        if (indexMetadata == null) {
            listener.onFailure(IndexNotFoundException(indexName))
        } else {
            routeToOwner(indexName, indexUuid(indexMetadata), listener)
        }
    }

    /**
     * The scheduler runs a job on the node holding the primary of the config index shard the job document lives in, so do the same.
     */
    private fun routeToOwner(indexName: String, indexUuid: String, listener: ActionListener<ISMStatusResponse>) {
        // With nodes of different ISM versions in the cluster the node that owns the job may be one that has no such action, so do not
        // send anything to it. The run itself is paused in that case as well.
        if (ManagedIndexRunner.isExecutionPaused()) {
            listener.onResponse(toResponse(indexName, indexUuid, ExecuteNowResult.UPGRADE_IN_PROGRESS))
            return
        }

        val state = clusterService.state()
        val ownerNodeId = try {
            val shardId = clusterService.operationRouting().shardId(state, INDEX_MANAGEMENT_INDEX, indexUuid, indexUuid)
            // an initializing or relocating primary has a node but cannot serve the job document yet
            state.routingTable().shardRoutingTable(shardId).primaryShard()?.takeIf { it.started() }?.currentNodeId()
        } catch (e: IndexNotFoundException) {
            // no config index means no index is managed
            listener.onResponse(notManaged(indexName, indexUuid))
            return
        }
        val owner: DiscoveryNode? = ownerNodeId?.let { state.nodes()[it] }
        if (owner == null) {
            listener.onFailure(
                OpenSearchStatusException(
                    "The config index shard holding the job for [$indexName] is not started on any node, try again shortly",
                    RestStatus.SERVICE_UNAVAILABLE,
                ),
            )
        } else if (owner.id == state.nodes().localNodeId) {
            execute(indexName, indexUuid, listener)
        } else if (owner.version != clusterService.localNode().version) {
            // The upgrade flag is only refreshed now and then, so look at the node itself: one of another version may not have the
            // internal action.
            listener.onResponse(toResponse(indexName, indexUuid, ExecuteNowResult.UPGRADE_IN_PROGRESS))
        } else {
            transportService.sendRequest(
                owner, ExecuteManagedIndexAction.NODE_ACTION_NAME, ExecuteManagedIndexRequest(indexName),
                TransportRequestOptions.EMPTY, ActionListenerResponseHandler<ISMStatusResponse>(listener, ::ISMStatusResponse),
            )
        }
    }

    // The node that got the request has checked the caller's permissions already
    private fun executeOnThisNode(indexName: String, listener: ActionListener<ISMStatusResponse>) {
        val indexMetadata = clusterService.state().metadata().index(indexName)
        if (indexMetadata == null) {
            listener.onFailure(IndexNotFoundException(indexName))
            return
        }

        execute(indexName, indexUuid(indexMetadata), listener)
    }

    // The key of the job document: not always the index's own UUID, an extension can have its own UUID setting
    private fun indexUuid(indexMetadata: IndexMetadata): String =
        (indexMetadataProvider.services[DEFAULT_INDEX_TYPE] as DefaultIndexMetadataService).getIndexUUID(indexMetadata)

    private fun execute(indexName: String, indexUuid: String, listener: ActionListener<ISMStatusResponse>) {
        CoroutineScope(Dispatchers.IO).launch {
            try {
                // The coroutine runs on a worker thread without the caller's identity, like a scheduled run, so the job document and the
                // run itself are not subject to the caller's permissions.
                val (config, success) = client.getManagedIndexConfig(indexUuid, xContentRegistry)
                val result = when {
                    !success -> throw OpenSearchStatusException("Failed to get the job document of [$indexName]", RestStatus.INTERNAL_SERVER_ERROR)
                    config == null -> null
                    else -> ManagedIndexRunner.executeNow(config, lockService)
                }
                listener.onResponse(toResponse(indexName, indexUuid, result))
            } catch (e: Exception) {
                log.error("Failed to execute managed index job for [$indexName]", e)
                listener.onFailure(toFailure(e))
            }
        }
    }

    // Must not throw: an exception here inside the coroutine would leave the caller without any response
    private fun toFailure(e: Exception): Exception {
        val cause = ExceptionsHelper.unwrapCause(e) as? Exception ?: e
        return when {
            // wrap() words every one of these as a missing config index, which is not what is missing here
            cause is IndexNotFoundException -> cause

            cause is OpenSearchSecurityException -> IndexManagementException.wrap(
                OpenSearchStatusException(
                    "User doesn't have required index permissions on the requested index: ${cause.localizedMessage}",
                    RestStatus.FORBIDDEN,
                ),
            )

            // wrap() reads the message without a null check
            cause.message == null -> cause

            else -> IndexManagementException.wrap(cause)
        }
    }

    private fun toResponse(indexName: String, indexUuid: String, result: ExecuteNowResult?): ISMStatusResponse = when (result) {
        ExecuteNowResult.EXECUTED -> ISMStatusResponse(1, emptyList())

        null -> notManaged(indexName, indexUuid)

        ExecuteNowResult.ALREADY_RUNNING -> failed(indexName, indexUuid, "This index's job is already executing, try again shortly")

        ExecuteNowResult.UPGRADE_IN_PROGRESS ->
            failed(indexName, indexUuid, "Execution is paused until all nodes in the cluster are upgraded to the same ISM version")

        ExecuteNowResult.JOB_DISABLED ->
            failed(indexName, indexUuid, "The job of this index is disabled because its policy has completed or failed, use the retry API for a failed one")
    }

    private fun notManaged(indexName: String, indexUuid: String) = failed(indexName, indexUuid, "This index is not being managed.")

    private fun failed(indexName: String, indexUuid: String, reason: String) = ISMStatusResponse(0, listOf(FailedIndex(indexName, indexUuid, reason)))
}
