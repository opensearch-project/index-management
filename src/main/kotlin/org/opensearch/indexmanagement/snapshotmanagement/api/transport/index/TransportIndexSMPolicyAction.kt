/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_POLICY_ACTION_NAME
import org.opensearch.transport.TransportService

class TransportIndexSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_POLICY_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMPolicyResponse {
        val policy = request.policy
        val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(policy.id)
            .create(request.create)
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }
        return IndexSMPolicyResponse(policy)
    }
}
