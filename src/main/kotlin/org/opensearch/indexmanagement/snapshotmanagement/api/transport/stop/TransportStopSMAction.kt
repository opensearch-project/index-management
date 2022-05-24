/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * com.maddyhome.idea.copyright.pattern.CommentInfo@6331d08d
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.stop

import org.apache.logging.log4j.LogManager
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.action.update.UpdateResponse
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.transport.TransportService
import java.time.Instant

class TransportStopSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters
) : BaseTransportAction<StopSMRequest, AcknowledgedResponse>(
    SMActions.STOP_SM_ACTION_NAME, transportService, client, actionFilters, ::StopSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: StopSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): AcknowledgedResponse {
        val smPolicy = getSMPolicy(client, request.id())
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
        val updateResponse: UpdateResponse = client.suspendUntil { update(updateRequest, it) }
        // TODO update metadata
        return updateResponse.result == DocWriteResponse.Result.UPDATED
    }
}
