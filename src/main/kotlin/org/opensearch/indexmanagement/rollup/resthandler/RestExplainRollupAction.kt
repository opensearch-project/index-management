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

package org.opensearch.indexmanagement.rollup.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ROLLUP_JOBS_BASE_URI
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import org.opensearch.indexmanagement.rollup.action.explain.ExplainRollupRequest
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.action.RestToXContentListener

class RestExplainRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return listOf(Route(GET, "$ROLLUP_JOBS_BASE_URI/{rollupID}/_explain"))
    }

    override fun getName(): String = "opendistro_explain_rollup_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val rollupIDs: List<String> = Strings.splitStringByCommaToArray(request.param("rollupID")).toList()
        if (rollupIDs.isEmpty()) {
            throw IllegalArgumentException("Missing rollupID")
        }
        val explainRequest = ExplainRollupRequest(rollupIDs)
        return RestChannelConsumer { channel ->
            client.execute(ExplainRollupAction.INSTANCE, explainRequest, RestToXContentListener(channel))
        }
    }
}
