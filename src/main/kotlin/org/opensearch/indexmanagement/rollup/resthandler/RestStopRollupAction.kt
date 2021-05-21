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
import org.opensearch.indexmanagement.rollup.action.stop.StopRollupAction
import org.opensearch.indexmanagement.rollup.action.stop.StopRollupRequest
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ROLLUP_JOBS_BASE_URI
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestStopRollupAction : BaseRestHandler() {

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST,"$ROLLUP_JOBS_BASE_URI/{rollupID}/_stop",
                POST, "$LEGACY_ROLLUP_JOBS_BASE_URI/{rollupID}/_stop"
            )
        )
    }

    override fun getName(): String {
        return "opendistro_stop_rollup_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val id = request.param("rollupID", Rollup.NO_ID)
        if (Rollup.NO_ID == id) {
            throw IllegalArgumentException("Missing rollup ID")
        }

        val stopRequest = StopRollupRequest(id)
        return RestChannelConsumer { channel ->
            client.execute(StopRollupAction.INSTANCE, stopRequest, RestToXContentListener(channel))
        }
    }
}
