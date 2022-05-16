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
import org.opensearch.indexmanagement.snapshotmanagement.smPolicyNameToDocId
import org.opensearch.transport.TransportService

class TransportDeleteSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<DeleteSMPolicyRequest, DeleteSMPolicyResponse>(
    DELETE_SM_ACTION_NAME, transportService, client, actionFilters, ::DeleteSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: DeleteSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): DeleteSMPolicyResponse {
        val deleteReq = DeleteRequest(INDEX_MANAGEMENT_INDEX, smPolicyNameToDocId(request.policyName))
        val deleteRes: DeleteResponse = client.suspendUntil { delete(deleteReq, it) }
        return DeleteSMPolicyResponse(deleteRes.status().toString())
    }
}
