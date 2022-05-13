/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.apache.logging.log4j.LogManager
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.contentParser
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.getSMDocId
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.transport.TransportService

class TransportGetSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<GetSMRequest, GetSMResponse>(
    GET_SM_ACTION_NAME, transportService, client, actionFilters, ::GetSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: GetSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): GetSMResponse {
        val getReq = GetRequest(INDEX_MANAGEMENT_INDEX, getSMDocId(request.policyName))
        val getRes: GetResponse = client.suspendUntil { get(getReq, it) }
        val xcp = contentParser(getRes.sourceAsBytesRef)
        val policy = xcp.parseWithType(getRes.id, getRes.seqNo, getRes.primaryTerm, SMPolicy.Companion::parse)
        return GetSMResponse(policy)
    }
}
