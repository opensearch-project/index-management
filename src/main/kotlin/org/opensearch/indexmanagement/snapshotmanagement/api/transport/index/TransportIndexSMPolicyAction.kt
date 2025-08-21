/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.authuser.User
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.PluginClient
import org.opensearch.indexmanagement.util.SecurityUtils
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

@Suppress("LongParameterList")
class TransportIndexSMPolicyAction
@Inject
constructor(
    client: Client,
    transportService: TransportService,
    private val indexManagementIndices: IndexManagementIndices,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
    pluginClient: PluginClient,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest, pluginClient,
) {
    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext,
    ): IndexSMPolicyResponse {
        // If filterBy is enabled and security is disabled or if filter by is enabled and backend role are empty an exception will be thrown
        SecurityUtils.validateUserConfiguration(user, filterByEnabled)

        if (indexManagementIndices.checkAndUpdateIMConfigIndex(log)) {
            log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
        }
        return indexSMPolicy(request, user)
    }

    private suspend fun indexSMPolicy(request: IndexSMPolicyRequest, user: User?): IndexSMPolicyResponse {
        val policy = request.policy.copy(schemaVersion = IndexUtils.indexManagementConfigSchemaVersion, user = user)
        val indexReq =
            request.index(INDEX_MANAGEMENT_INDEX)
                .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .id(policy.id)
                .routing(policy.id) // by default routed by id
        val indexRes: IndexResponse = pluginClient.suspendUntil { index(indexReq, it) }

        return IndexSMPolicyResponse(indexRes.id, indexRes.version, indexRes.seqNo, indexRes.primaryTerm, policy, indexRes.status())
    }
}
