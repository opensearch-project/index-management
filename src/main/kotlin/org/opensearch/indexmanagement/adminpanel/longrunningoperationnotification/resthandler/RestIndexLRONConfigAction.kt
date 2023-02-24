/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.resthandler

import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.index.IndexLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.index.IndexLRONConfigRequest
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.index.IndexLRONConfigResponse
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.model.LRONConfig
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.opensearchapi.parseWithType
import org.opensearch.indexmanagement.util.IF_PRIMARY_TERM
import org.opensearch.indexmanagement.util.IF_SEQ_NO
import org.opensearch.indexmanagement.util.NO_ID
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException

class RestIndexLRONConfigAction : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.PUT, IndexManagementPlugin.LRON_BASE_URI),
            RestHandler.Route(RestRequest.Method.PUT, "${IndexManagementPlugin.LRON_BASE_URI}/{taskID}")
        )
    }

    override fun getName(): String {
        return "index_lro_notification_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val taskID = request.param("taskID", NO_ID)
        val xcp = request.contentParser()
        val lronConfig = xcp.parseWithType(parse = LRONConfig.Companion::parse)
        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)

        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }

        val indexLRONConfigRequest = IndexLRONConfigRequest(taskID, lronConfig, seqNo, primaryTerm, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(
                IndexLRONConfigAction.INSTANCE, indexLRONConfigRequest,
                object : RestResponseListener<IndexLRONConfigResponse>(channel) {
                    override fun buildResponse(response: IndexLRONConfigResponse): RestResponse {
                        val restResponse = BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                        if (response.status == RestStatus.CREATED) {
                            val location = "${IndexManagementPlugin.LRON_BASE_URI}/${response.id}"
                            restResponse.addHeader("Location", location)
                        }
                        return restResponse
                    }
                }
            )
        }
    }
}
