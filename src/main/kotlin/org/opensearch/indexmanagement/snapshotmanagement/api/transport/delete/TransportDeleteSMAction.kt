/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import org.apache.logging.log4j.LogManager
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.DELETE_SM_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.getSMDocId
import org.opensearch.transport.TransportService

class TransportDeleteSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<DeleteSMRequest, DeleteSMResponse>(
    DELETE_SM_ACTION_NAME, transportService, client, actionFilters, ::DeleteSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: DeleteSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): DeleteSMResponse {
        val deleteReq = DeleteRequest(INDEX_MANAGEMENT_INDEX, getSMDocId(request.policyName))
        val deleteRes: DeleteResponse = client.suspendUntil { delete(deleteReq, it) }
        log.info("Delete SM policy response: $deleteRes.")
        return DeleteSMResponse(deleteRes.status().toString())
    }
}
