/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * com.maddyhome.idea.copyright.pattern.CommentInfo@6331d08d
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.client.Client
import org.opensearch.common.inject.Inject
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.BaseTransportAction
import org.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import org.opensearch.indexmanagement.snapshotmanagement.getSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.RestStatus
import org.opensearch.transport.TransportService

class TransportExecuteSMAction @Inject constructor(
    client: Client,
    transportService: TransportService,
    actionFilters: ActionFilters
) : BaseTransportAction<ExecuteSMRequest, ExecuteSMResponse>(
    SMActions.EXECUTE_SM_ACTION_NAME, transportService, client, actionFilters, ::ExecuteSMRequest
) {

    private val log = LogManager.getLogger(javaClass)

    override suspend fun executeRequest(
        request: ExecuteSMRequest,
        user: User?,
        threadContext: ThreadContext.StoredContext
    ): ExecuteSMResponse {
        val smPolicy = client.getSMPolicy(request.snapshotPolicyID)

        return executeSMPolicy(smPolicy)
    }

    private fun executeSMPolicy(smPolicy: SMPolicy): ExecuteSMResponse {
        return ExecuteSMResponse(smPolicy, RestStatus.OK)
    }
}
