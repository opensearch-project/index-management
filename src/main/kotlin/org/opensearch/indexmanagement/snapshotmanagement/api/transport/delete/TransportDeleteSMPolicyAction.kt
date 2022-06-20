/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.index.engine.VersionConflictEngineException
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_POLICY_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService

class TransportDeleteSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<DeleteSMPolicyRequest, DeleteResponse>(
    DELETE_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::DeleteSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: DeleteSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): DeleteResponse {
        val smPolicy = client.getSMPolicy(request.id())

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        val deleteReq = request.index(INDEX_MANAGEMENT_INDEX)
        try {
            return client.suspendUntil { delete(deleteReq, it) }
        } catch (e: VersionConflictEngineException) {
            log.error("VersionConflictEngineException while trying to delete snapshot management policy id [${deleteReq.id()}]: $e")
            throw OpenSearchStatusException(conflictExceptionMessage, RestStatus.INTERNAL_SERVER_ERROR)
        } catch (e: Exception) {
            log.error("Failed trying to delete snapshot management policy id [${deleteReq.id()}]: $e")
            throw OpenSearchStatusException("Failed while trying to delete SM Policy", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }

    companion object {
        private const val conflictExceptionMessage = "Failed while trying to delete SM Policy due to a concurrent update, please try again"
    }
}
