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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.opensearchapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.OpenSearchStatusException
import org.opensearch.ExceptionsHelper
import org.opensearch.action.ActionListener
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.client.node.NodeClient
import org.opensearch.common.inject.Inject
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.rest.RestStatus
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

class TransportGetPolicyAction @Inject constructor(
    val client: NodeClient,
    transportService: TransportService,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetPolicyRequest, GetPolicyResponse>(
    GetPolicyAction.NAME, transportService, actionFilters, ::GetPolicyRequest
) {
    override fun doExecute(task: Task, request: GetPolicyRequest, listener: ActionListener<GetPolicyResponse>) {
        GetPolicyHandler(client, listener, request).start()
    }

    inner class GetPolicyHandler(
        private val client: NodeClient,
        private val actionListener: ActionListener<GetPolicyResponse>,
        private val request: GetPolicyRequest
    ) {
        fun start() {
            val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, request.policyID)
                .version(request.version)
                .fetchSourceContext(request.fetchSrcContext)

            client.get(getRequest, object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    onGetResponse(response)
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(ExceptionsHelper.unwrapCause(t) as Exception)
                }
            })
        }

        fun onGetResponse(response: GetResponse) {
            if (!response.isExists) {
                actionListener.onFailure(OpenSearchStatusException("Policy not found", RestStatus.NOT_FOUND))
                return
            }

            var policy: Policy? = null
            if (!response.isSourceEmpty) {
                XContentHelper.createParser(
                    xContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef,
                    XContentType.JSON
                ).use { xcp ->
                    policy = xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, Policy.Companion::parse)
                }
            }

            actionListener.onResponse(
                GetPolicyResponse(response.id, response.version, response.seqNo, response.primaryTerm, policy)
            )
        }
    }
}
