/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import org.opensearch.action.ActionListener
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.delete.DeleteResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportDeletePolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters
) : HandledTransportAction<DeletePolicyRequest, DeleteResponse>(
        DeletePolicyAction.NAME, transportService, actionFilters, ::DeletePolicyRequest
) {
    override fun doExecute(task: Task, request: DeletePolicyRequest, listener: ActionListener<DeleteResponse>) {
        val deleteRequest = DeleteRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .setRefreshPolicy(request.refreshPolicy)

        client.delete(deleteRequest, listener)
    }
}
