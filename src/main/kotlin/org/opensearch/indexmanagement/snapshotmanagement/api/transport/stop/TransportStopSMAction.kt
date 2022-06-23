/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.stop

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.update.UpdateResponse
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
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings.Companion.FILTER_BY_BACKEND_ROLES
import org.opensearch.indexmanagement.util.SecurityUtils.Companion.verifyUserHasPermissionForResource
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService
import java.time.Instant

class TransportStopSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    val settings: Settings,
) : BaseTransportAction<StopSMRequest, AcknowledgedResponse>(
    SMActions.STOP_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::StopSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    @Volatile private var filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES) {
            filterByEnabled = it
        }
    }

    override suspend fun executeRequest(
        request: StopSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): AcknowledgedResponse {
        val smPolicy = client.getSMPolicy(request.id())

        // Check if the requested user has permission on the resource, throwing an exception if the user does not
        verifyUserHasPermissionForResource(user, smPolicy.user, filterByEnabled, "snapshot management policy", smPolicy.policyName)

        if (!smPolicy.jobEnabled) {
            log.debug("Snapshot management policy is already disabled")
            return AcknowledgedResponse(true)
        }
        return AcknowledgedResponse(disableSMPolicy(request))
    }

    private suspend fun disableSMPolicy(updateRequest: StopSMRequest): Boolean {
        val now = Instant.now().toEpochMilli()
        updateRequest.index(INDEX_MANAGEMENT_INDEX).doc(
            mapOf(
                SMPolicy.SM_TYPE to mapOf(
                    SMPolicy.ENABLED_FIELD to false,
                    SMPolicy.ENABLED_TIME_FIELD to null,
                    SMPolicy.LAST_UPDATED_TIME_FIELD to now
                )
            )
        )
        val updateResponse: UpdateResponse = try {
            client.suspendUntil { update(updateRequest, it) }
        } catch (e: VersionConflictEngineException) {
            log.error("VersionConflictEngineException while trying to disable snapshot management policy id [${updateRequest.id()}]: $e")
            throw OpenSearchStatusException(conflictExceptionMessage, RestStatus.INTERNAL_SERVER_ERROR)
        } catch (e: Exception) {
            log.error("Failed trying to disable snapshot management policy id [${updateRequest.id()}]: $e")
            throw OpenSearchStatusException("Failed while trying to disable SM Policy", RestStatus.INTERNAL_SERVER_ERROR)
        }
        // TODO update metadata
        return updateResponse.result == DocWriteResponse.Result.UPDATED
    }

    companion object {
        private const val conflictExceptionMessage = "Failed while trying to disable SM Policy due to a concurrent update, please try again"
    }
}
