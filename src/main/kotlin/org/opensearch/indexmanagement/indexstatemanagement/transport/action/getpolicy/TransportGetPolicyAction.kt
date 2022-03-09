/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

@Suppress("ReturnCount")
class TransportGetPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPolicyRequest, GetPolicyResponse>(
    GetPolicyAction.NAME, transportService, actionFilters, ::GetPolicyRequest
) {

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetPolicyRequest, listener: ActionListener<GetPolicyResponse>) {
        GetPolicyHandler(client, listener, request).start()
    }

    inner class GetPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetPolicyResponse>,
        private val request: GetPolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {
        fun start() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .version(request.version)

            client.threadPool().threadContext.stashContext().use {
                client.get(
                    getRequest,
                    object : ActionListener<GetResponse> {
                        override fun onResponse(response: GetResponse) {
                            onGetResponse(response)
                        }

                        override fun onFailure(t: Exception) {
                            actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                        }
                    }
                )
            }
        }

        fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }

            val policy: Policy?
            try {
                policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
            } catch (e: IllegalArgumentException) {
                actionListener.onFailure(OpenSearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }
            if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                return
            } else {
                // if HEAD request don't return the policy
                val policyResponse = if (!request.fetchSrcContext.fetchSource()) {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, null)
                } else {
                    GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, policy)
                }
                actionListener.onResponse(policyResponse)
            }
        }
    }
}
