/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.parseSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService

class TransportGetSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<GetSMPolicyRequest, GetSMPolicyResponse>(
    GET_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::GetSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: GetSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMPolicyResponse {
        val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
        val getResponse: GetResponse = try {
            client.suspendUntil { get(getRequest, it) }
        } catch (e: IndexNotFoundException) {
            throw OpenSearchStatusException("Snapshot management config index not found", RestStatus.NOT_FOUND)
        }
        if (!getResponse.isExists) {
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        val smPolicy = try {
            parseSMPolicy(getResponse)
        } catch (e: IllegalArgumentException) {
            log.error("Error while parsing snapshot management policy ${request.policyID}", e)
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.INTERNAL_SERVER_ERROR)
        }

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        log.debug("Get SM policy: $smPolicy")
        return GetSMPolicyResponse(getResponse.id, getResponse.version, getResponse.seqNo, getResponse.primaryTerm, smPolicy)
    }
}
