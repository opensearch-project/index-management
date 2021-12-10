/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.get

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionListener
import org.opensearch.action.ActionResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.ConfigConstants
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.ExistsQueryBuilder
import org.opensearch.index.query.WildcardQueryBuilder
import org.opensearch.indexmanagement.settings.IndexManagementSettings
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.addUserFilter
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.buildUser
import org.opensearch.indexmanagement.util.getJobs
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.SortOrder
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetTransformsAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    val settings: Settings,
    val clusterService: ClusterService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetTransformsRequest, GetTransformsResponse> (
    GetTransformsAction.NAME, transportService, actionFilters, ::GetTransformsRequest
) {

    @Volatile private var filterByEnabled = IndexManagementSettings.FILTER_BY_BACKEND_ROLES.get(settings)
    private val log = LogManager.getLogger(javaClass)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(IndexManagementSettings.FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override fun doExecute(task: Task, request: GetTransformsRequest, listener: ActionListener<GetTransformsResponse>) {
        log.debug(
            "User and roles string from thread context: ${client.threadPool().threadContext.getTransient<String>(
                ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT
            )}"
        )
        val searchString = request.searchString.trim()
        val from = request.from
        val size = request.size
        val sortField = request.sortField
        val sortDirection = request.sortDirection

        val boolQueryBuilder = BoolQueryBuilder().filter(ExistsQueryBuilder(Transform.TRANSFORM_TYPE))
        if (searchString.isNotEmpty()) {
            boolQueryBuilder.filter(WildcardQueryBuilder("${Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword", "*$searchString*"))
        }
        val user = buildUser(client.threadPool().threadContext)
        addUserFilter(user, boolQueryBuilder, filterByEnabled, "transform.user")
        val searchSourceBuilder = SearchSourceBuilder().query(boolQueryBuilder).from(from).size(size).seqNoAndPrimaryTerm(true)
            .sort(sortField, SortOrder.fromString(sortDirection))

        client.threadPool().threadContext.stashContext().use {
            getJobs(
                client,
                searchSourceBuilder,
                listener as ActionListener<ActionResponse>,
                Transform.TRANSFORM_TYPE,
                ::contentParser
            )
        }
    }

    private fun contentParser(bytesReference: BytesReference): XContentParser {
        return XContentHelper.createParser(
            xContentRegistry,
            LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON
        )
    }
}
