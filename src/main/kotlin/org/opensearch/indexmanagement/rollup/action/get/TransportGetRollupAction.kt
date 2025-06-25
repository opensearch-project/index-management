/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.ConfigConstants
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.util.parseRollup
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.lang.Exception

@Suppress("LongParameterList")
class TransportGetRollupAction
@Inject
constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val settings: Settings,
    val clusterService: ClusterService,
    val xContentRegistry: NamedXContentRegistry,
    val pluginClient: PluginClient,
) : HandledTransportAction<GetRollupRequest, GetRollupResponse>(
    GetRollupAction.NAME, transportService, actionFilters, ::GetRollupRequest,
) {
    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    @Suppress("ReturnCount")
    override fun doExecute(task: Task, request: GetRollupRequest, listener: ActionListener<GetRollupResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT,
            )}",
        )
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id).preference(request.preference)
        val user = buildUser(client.threadPool().threadContext)
        pluginClient.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        return listener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                    }

                    val rollup: Rollup?
                    try {
                        rollup = parseRollup(response, xContentRegistry)
                    } catch (e: IllegalArgumentException) {
                        listener.onFailure(OpenSearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                        return
                    }
                    if (!SecurityUtils.userHasPermissionForResource(user, rollup.user, filterByEnabled, "rollup", request.id, listener)) {
                        return
                    } else {
                        // if HEAD request don't return the rollup
                        val rollupResponse =
                            if (request.srcContext != null && !request.srcContext.fetchSource()) {
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
            },
        )
    }
}
