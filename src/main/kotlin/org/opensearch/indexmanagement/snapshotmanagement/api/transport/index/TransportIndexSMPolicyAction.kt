/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.DocWriteRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.index.IndexResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.IndexManagementIndices
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.INDEX_SM_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService

class TransportIndexSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    val indexManagementIndices: IndexManagementIndices,
    actionFilters: ActionFilters,
) : BaseTransportAction<IndexSMPolicyRequest, IndexSMPolicyResponse>(
    INDEX_SM_ACTION_NAME, transportService, client, actionFilters, ::IndexSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: IndexSMPolicyRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): IndexSMPolicyResponse {
        updateIMConfigMappings()
        return indexSMPolicy(request)
    }

    private suspend fun updateIMConfigMappings() {
        val response: AcknowledgedResponse = indexManagementIndices.checkAndUpdateIMConfigIndex()
        if (response.isAcknowledged) {
            log.info("Successfully created or updated $INDEX_MANAGEMENT_INDEX with newest mappings.")
        } else {
            log.error("Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.")
            throw OpenSearchStatusException("Unable to create or update $INDEX_MANAGEMENT_INDEX with newest mapping.", RestStatus.INTERNAL_SERVER_ERROR)
        }
    }


    private suspend fun indexSMPolicy(request: IndexSMPolicyRequest): IndexSMPolicyResponse {
        val policy = request.policy
        val indexReq = request.index(INDEX_MANAGEMENT_INDEX)
            .source(policy.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .id(policy.id)
            .timeout(IndexRequest.DEFAULT_TIMEOUT)
        val indexRes: IndexResponse = client.suspendUntil { index(indexReq, it) }
        log.info("Index SM policy response: $indexRes")

        return IndexSMPolicyResponse(indexRes.id, indexRes.version, indexRes.seqNo, indexRes.primaryTerm, policy, indexRes.status())
    }
}
