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
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.getSMDocId
import org.opensearch.transport.TransportService

class TransportIndexSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<IndexSMRequest, IndexSMResponse>(
    SMActions.INDEX_SM_ACTION_NAME, transportService, client, actionFilters, ::IndexSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: IndexSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMResponse {
        val policy = request.smPolicy
        val indexReq = IndexRequest(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(getSMDocId(policy.policyName))
            .create(true)
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }
        log.info("Index SM policy response: $indexRes")
        return IndexSMResponse(policy)
    }
}
