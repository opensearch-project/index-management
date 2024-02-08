/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification

import org.opensearch.ExceptionsHelper
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.util.IndexUtils

class ControlCenterIndices(
    private val client: IndicesAdminClient,
    private val clusterService: ClusterService,
) {
    fun checkAndUpdateControlCenterIndex(actionListener: ActionListener<AcknowledgedResponse>) {
        if (!controlCenterIndexExists()) {
            val indexRequest =
                CreateIndexRequest(IndexManagementPlugin.CONTROL_CENTER_INDEX)
                    .mapping(controlCenterMappings)
                    .settings(Settings.builder().put(INDEX_HIDDEN, true).build())
            client.create(
                indexRequest,
                object : ActionListener<CreateIndexResponse> {
                    override fun onFailure(e: Exception) {
                        if (ExceptionsHelper.unwrapCause(e) is ResourceAlreadyExistsException) {
                            /*
                             * if two request create the control center index at the same time, may raise this exception
                             * but we don't take it as error
                             */
                            actionListener.onResponse(
                                CreateIndexResponse(
                                    true,
                                    true,
                                    IndexManagementPlugin.CONTROL_CENTER_INDEX,
                                ),
                            )
                        } else {
                            actionListener.onFailure(e)
                        }
                    }

                    override fun onResponse(response: CreateIndexResponse) {
                        actionListener.onResponse(response)
                    }
                },
            )
        } else {
            IndexUtils.checkAndUpdateIndexMapping(
                IndexManagementPlugin.CONTROL_CENTER_INDEX,
                IndexUtils.getSchemaVersion(controlCenterMappings),
                controlCenterMappings,
                clusterService.state(),
                client,
                actionListener,
            )
        }
    }

    private fun controlCenterIndexExists(): Boolean = clusterService.state().routingTable.hasIndex(IndexManagementPlugin.CONTROL_CENTER_INDEX)

    companion object {
        val controlCenterMappings =
            ControlCenterIndices::class.java.classLoader
                .getResource("mappings/opensearch-control-center.json")!!.readText()
    }
}
