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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.opensearchapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_PRIMARY_TERM
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IF_SEQ_NO
import com.amazon.opendistroforelasticsearch.indexmanagement.util.REFRESH
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupResponse
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.PUT
import org.opensearch.rest.RestResponse
import org.opensearch.rest.RestStatus
import org.opensearch.rest.action.RestResponseListener
import java.io.IOException
import java.time.Instant

class RestIndexRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(
            Route(PUT, ROLLUP_JOBS_BASE_URI),
            Route(PUT, "$ROLLUP_JOBS_BASE_URI/{rollupID}")
        )
    }

    override fun getName(): String {
        return "opendistro_index_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", Rollup.NO_ID)
        if (Rollup.NO_ID == id) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO)
        val primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM)
        val xcp = request.contentParser()
        val rollup = xcp.parseWithType(id = id, seqNo = seqNo, primaryTerm = primaryTerm, parse = Rollup.Companion::parse)
            .copy(jobLastUpdatedTime = Instant.now())
        val refreshPolicy = if (request.hasParam(REFRESH)) {
            WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
        } else {
            WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexRollupRequest = IndexRollupRequest(rollup, refreshPolicy)
        return RestChannelConsumer { channel ->
            client.execute(IndexRollupAction.INSTANCE, indexRollupRequest, indexRollupResponse(channel))
        }
    }

    private fun indexRollupResponse(channel: RestChannel):
        RestResponseListener<IndexRollupResponse> {
        return object : RestResponseListener<IndexRollupResponse>(channel) {
            @Throws(Exception::class)
            override fun buildResponse(response: IndexRollupResponse): RestResponse {
                val restResponse =
                    BytesRestResponse(response.status, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS))
                if (response.status == RestStatus.CREATED) {
                    val location = "$ROLLUP_JOBS_BASE_URI/${response.id}"
                    restResponse.addHeader("Location", location)
                }
                return restResponse
            }
        }
    }
}
