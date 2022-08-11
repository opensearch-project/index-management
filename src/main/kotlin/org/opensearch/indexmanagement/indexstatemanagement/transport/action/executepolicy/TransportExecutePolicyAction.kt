/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.ConfigConstants
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.node.NodeClient
import org.opensearch.jobscheduler.spi.utils.LockService
import org.opensearch.ExceptionsHelper
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.action.get.MultiGetRequest
import org.opensearch.action.get.MultiGetResponse
import org.opensearch.action.support.IndicesOptions
import org.opensearch.jobscheduler.spi.JobDocVersion
import org.opensearch.jobscheduler.spi.JobExecutionContext
import java.time.Instant

private val log = LogManager.getLogger(TransportExecutePolicyAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportExecutePolicyAction @Inject constructor(
    transportService: TransportService,
    val client: NodeClient,
    private val clusterService: ClusterService,
    private val runner: ManagedIndexRunner,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val request: ExecutePolicyRequest
) : HandledTransportAction<ExecutePolicyRequest, AcknowledgedResponse> (
    ExecutePolicyAction.NAME, transportService, actionFilters, ::ExecutePolicyRequest
) {

    override fun doExecute(task: Task, execPolicyRequest: ExecutePolicyRequest, actionListener: ActionListener<AcknowledgedResponse>) {
        val userStr = client.threadPool().threadContext.getTransient<String>(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)
        log.debug("User and roles string from thread context: $userStr")
        val user: User? = User.parse(userStr)
        val indices = mutableSetOf<String>()

        runner.launch {
            try {
                val lock = LockService(client, clusterService)
                // Temp context in order to pass into runJob
                val newContext = JobExecutionContext(Instant.now(), JobDocVersion(0L, 0L, 0L), lock, "", "")
                // Need to get managed index metadata to pass into runJob
                // runner.runJob(None, newContext)
            } catch (e: Exception) {
                log.error("Unexpected error trying to execute policy")
                withContext(Dispatchers.IO) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                }
            }
        }
        fun getExistingManagedIndices() {

            val multiGetReq = MultiGetRequest()

            client.multiGet(
                multiGetReq,
                object : ActionListener<MultiGetResponse> {
                    override fun onResponse(response: MultiGetResponse) {

                        response.forEach {
                            // get managed index configs
                        }
                    }

                    override fun onFailure(e: java.lang.Exception?) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }
        fun getClusterState() {
            val strictExpandOptions = IndicesOptions.strictExpand()

            val clusterStateRequest = ClusterStateRequest()
                .clear()
                .indices(*request.indices.toTypedArray())
                .metadata(true)
                .local(false)
                .indicesOptions(strictExpandOptions)

            client.threadPool().threadContext.stashContext().use {
                client.admin()
                    .cluster()
                    .state(
                        clusterStateRequest,
                        object : ActionListener<ClusterStateResponse> {
                            override fun onResponse(response: ClusterStateResponse) {
                                val indexMetadatas = response.state.metadata.indices
                                indexMetadatas.forEach {
                                    indices.add(it.value.indexUUID)
                                }

                                getExistingManagedIndices()
                            }

                            override fun onFailure(t: Exception) {
                                actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                            }
                        }
                    )
            }
        }
    }
}
