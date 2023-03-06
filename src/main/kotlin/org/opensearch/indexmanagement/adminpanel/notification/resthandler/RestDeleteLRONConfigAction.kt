/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.resthandler

import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.adminpanel.notification.action.delete.DeleteLRONConfigAction
import org.opensearch.indexmanagement.adminpanel.notification.action.delete.DeleteLRONConfigRequest
import org.opensearch.indexmanagement.util.REFRESH
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import java.io.IOException

class RestDeleteLRONConfigAction : BaseRestHandler() {
    override fun routes(): List<RestHandler.Route> {
        return listOf(
            RestHandler.Route(RestRequest.Method.DELETE, "${IndexManagementPlugin.LRON_BASE_URI}/{id}")
        )
    }

    override fun getName(): String {
        return "delete_lron_config_action"
    }

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val docID = request.param("id")
        val refreshPolicy = RefreshPolicy.parse(request.param(REFRESH, RefreshPolicy.IMMEDIATE.value))
        val deleteLRONConfigRequest = DeleteLRONConfigRequest(docID, refreshPolicy)

        return RestChannelConsumer { channel ->
            client.execute(DeleteLRONConfigAction.INSTANCE, deleteLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
