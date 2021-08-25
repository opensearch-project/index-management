/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.client.node.NodeClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.opensearchapi.parseFromGetResponse
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.IllegalArgumentException

@Suppress("ReturnCount")
class TransportDeletePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeletePolicyRequest, DeleteResponse>(
    DeletePolicyAction.NAME, transportService, actionFilters, ::DeletePolicyRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeletePolicyRequest, listener: ActionListener<DeleteResponse>) {
        DeletePolicyHandler(client, listener, request).start()
    }

    inner class DeletePolicyHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeletePolicyRequest,
        private val user: User? = buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            client.threadPool().threadContext.stashContext().use {
                getPolicy()
            }
        }

        private fun getPolicy() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(OpenSearchStatusException("Policy ${request.policyID} is not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val policy: Policy?
                        try {
                            policy = parseFromGetResponse(response, xContentRegistry, Policy.Companion::parse)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(OpenSearchStatusException("Policy ${request.policyID} is not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!userHasPermissionForResource(user, policy.user, filterByEnabled, "policy", request.policyID, actionListener)) {
                            return
                        } else {
                            delete()
                        }
                    }

                    override fun onFailure(t: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                    }
                }
            )
        }

        private fun delete() {
            val deleteRequest = DeleteRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .setRefreshPolicy(request.refreshPolicy)

            client.threadPool().threadContext.stashContext().use {
                client.delete(deleteRequest, actionListener)
            }
        }
    }
}
