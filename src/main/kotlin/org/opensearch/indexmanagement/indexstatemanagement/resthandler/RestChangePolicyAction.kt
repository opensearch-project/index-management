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

package org.opensearch.indexmanagement.indexstatemanagement.resthandler

import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.ISM_BASE_URI
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyRequest
import org.apache.logging.log4j.LogManager
import org.opensearch.client.node.NodeClient
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.IndexManagementPlugin.Companion.LEGACY_ISM_BASE_URI
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.POST
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestChangePolicyAction : BaseRestHandler() {

    private val log = LogManager.getLogger(javaClass)

    override fun routes(): List<Route> {
        return emptyList()
    }

    override fun replacedRoutes(): List<ReplacedRoute> {
        return listOf(
            ReplacedRoute(
                POST, CHANGE_POLICY_BASE_URI,
                POST, LEGACY_CHANGE_POLICY_BASE_URI
            ),
            ReplacedRoute(
                POST,"$CHANGE_POLICY_BASE_URI/{index}",
                POST, "$LEGACY_CHANGE_POLICY_BASE_URI/{index}"
            )
        )
    }

    override fun getName(): String = "change_policy_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indices: Array<String>? = Strings.splitStringByCommaToArray(request.param("index"))
        if (indices == null || indices.isEmpty()) {
            throw IllegalArgumentException("Missing index")
        }

        val xcp = request.contentParser()
        ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
        val changePolicy = ChangePolicy.parse(xcp)

        val changePolicyRequest = ChangePolicyRequest(indices.toList(), changePolicy)

        return RestChannelConsumer { channel ->
            client.execute(ChangePolicyAction.INSTANCE, changePolicyRequest, RestToXContentListener(channel))
        }
    }

    companion object {
        const val CHANGE_POLICY_BASE_URI = "$ISM_BASE_URI/change_policy"
        const val LEGACY_CHANGE_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/change_policy"
        const val INDEX_NOT_MANAGED = "This index is not being managed"
        const val INDEX_IN_TRANSITION = "Cannot change policy while transitioning to new state"
    }
}
