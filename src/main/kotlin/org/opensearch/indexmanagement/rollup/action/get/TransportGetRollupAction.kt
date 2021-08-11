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

package org.opensearch.indexmanagement.rollup.action.get

import org.opensearch.OpenSearchStatusException
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.util.parseRollup
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import java.lang.Exception

class TransportGetRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupRequest, GetRollupResponse> (
    GetRollupAction.NAME, transportService, actionFilters, ::GetRollupRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetRollupRequest, listener: ActionListener<GetRollupResponse>) {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        val user = buildUser(client.threadPool().threadContext)
        client.threadPool().threadContext.stashContext().use {
            client.get(
                getRequest,
                object : ActionListener<GetResponse> {
                    override fun onResponse(response: GetResponse) {
                        if (!response.isExists) {
                            return listener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        }

                        val rollup: Rollup = parseRollup(response, xContentRegistry)
                        if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", request.id, listener)) {
                            return
                        } else {
                            // if HEAD request don't return the rollup
                            val rollupResponse = if (request.srcContext != null && !request.srcContext.fetchSource()) {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, null)
                            } else {
                                GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, rollup)
                            }
                            listener.onResponse(rollupResponse)
                        }
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                }
            )
        }
    }
}
