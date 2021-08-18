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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.indexmanagement.rollup.action.delete

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
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.util.parseRollup
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.userHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception

@Suppress("ReturnCount")
class TransportDeleteRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val clusterService: ClusterService,
    val settings: Settings,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteRollupRequest, DeleteResponse>(
    DeleteRollupAction.NAME, transportService, actionFilters, ::DeleteRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: DeleteRollupRequest, actionListener: ActionListener<DeleteResponse>) {
        DeleteRollupHandler(client, actionListener, request).start()
    }

    inner class DeleteRollupHandler(
        private val client: Client,
        private val actionListener: ActionListener<DeleteResponse>,
        private val request: DeleteRollupRequest,
        private val user: User? = SecurityUtils.buildUser(client.threadPool().threadContext)
    ) {

        fun start() {
            client.threadPool().threadContext.stashContext().use {
                if (!filterByEnabled || user == null) {
                    // security is disabled or filter by is disabled
                    delete()
                } else {
                    getRollup()
                }
            }
        }

        private fun getRollup() {
            val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id())
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }

                        val rollup: Rollup?
                        try {
                            rollup = parseRollup(response, xContentRegistry)
                        } catch (e: IllegalArgumentException) {
                            actionListener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                            return
                        }
                        if (!userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", rollup.id, actionListener)) {
                            return
                        } else {
                            delete()
                        }
                    }

                    override fun onFailure(e: Exception) {
                        actionListener.onFailure(ExceptionsHelper.unwrapCause(e) as Exception)
                    }
                }
            )
        }

        private fun delete() {
            val deleteRequest = DeleteRequest(INDEX_MANAGEMENT_INDEX, request.id())
                .setRefreshPolicy(request.refreshPolicy)
            client.threadPool().threadContext.stashContext().use {
                client.delete(deleteRequest, actionListener)
            }
        }
    }
}
