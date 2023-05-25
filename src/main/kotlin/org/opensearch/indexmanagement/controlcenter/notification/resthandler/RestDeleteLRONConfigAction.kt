/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.resthandler

import org.opensearch.client.node.NodeClient
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigAction
import org.opensearch.indexmanagement.controlcenter.notification.action.delete.DeleteLRONConfigRequest
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
        val docId = request.param("id")
        val deleteLRONConfigRequest = DeleteLRONConfigRequest(docId)

        return RestChannelConsumer { channel ->
            client.execute(DeleteLRONConfigAction.INSTANCE, deleteLRONConfigRequest, RestToXContentListener(channel))
        }
    }
}
