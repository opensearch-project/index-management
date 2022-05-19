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
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.index.IndexNotFoundException
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions.GET_SM_ACTION_NAME
import org.opensearch.indexmanagement.snapshotmanagement.parseSMPolicy
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService

class TransportGetSMPolicyAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters,
) : BaseTransportAction<GetSMPolicyRequest, GetSMPolicyResponse>(
    GET_SM_ACTION_NAME, transportService, client, actionFilters, ::GetSMPolicyRequest
) {

    private val log = LogManager.getLogger(javaClass)

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
            throw OpenSearchStatusException("Snapshot management policy not found", RestStatus.NOT_FOUND)
        }
        // TODO SM security integration

        log.info("sm dev: Parsed SM policy: $smPolicy")
        return GetSMPolicyResponse(getResponse.id, getResponse.version, getResponse.seqNo, getResponse.primaryTerm, smPolicy)
    }
}
