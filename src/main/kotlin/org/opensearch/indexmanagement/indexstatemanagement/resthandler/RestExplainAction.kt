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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.master.MasterNodeRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.model.SearchParams
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainRequest
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_JOB_SORT_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_FROM
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_PAGINATION_SIZE
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_QUERY_STRING
import org.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_SORT_ORDER
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

private val log = LogManager.getLogger(RestExplainAction::class.java)

class RestExplainAction : BaseRestHandler() {

    companion object {
        const val EXPLAIN_BASE_URI = "$ISM_BASE_URI/explain"
        const val LEGACY_EXPLAIN_BASE_URI = "$LEGACY_ISM_BASE_URI/explain"
    }

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                GET, EXPLAIN_BASE_URI,
                GET, LEGACY_EXPLAIN_BASE_URI
            ),
            ReplacedRoute(
                GET, "$EXPLAIN_BASE_URI/{index}",
                GET, "$LEGACY_EXPLAIN_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String {
        return "ism_explain_action"
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${request.path()}")

        val indices: Array<String> = Strings.splitStringByCommaToArray(request.param("index"))

        val size = request.paramAsInt("size", DEFAULT_PAGINATION_SIZE)
        val from = request.paramAsInt("from", DEFAULT_PAGINATION_FROM)
        val sortField = request.param("sortField", DEFAULT_JOB_SORT_FIELD)
        val sortOrder = request.param("sortOrder", DEFAULT_SORT_ORDER)
        val queryString = request.param("queryString", DEFAULT_QUERY_STRING)

        val explainRequest = ExplainRequest(
            indices.toList(),
            request.paramAsBoolean("local", false),
            request.paramAsTime("master_timeout", MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT),
            SearchParams(size, from, sortField, sortOrder, queryString)
        )

        return RestChannelConsumer { channel ->
            client.execute(ExplainAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
