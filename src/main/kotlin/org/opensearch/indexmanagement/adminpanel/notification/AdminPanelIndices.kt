/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification

import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.ActionListener
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.IndicesAdminClient
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.IndexManagementPlugin
import org.opensearch.indexmanagement.indexstatemanagement.util.INDEX_HIDDEN
import org.opensearch.indexmanagement.util.IndexUtils

class AdminPanelIndices(
    private val client: IndicesAdminClient,
    private val clusterService: ClusterService,
) {

    fun checkAndUpdateAdminPanelIndex(actionListener: ActionListener<AcknowledgedResponse>) {
        if (!adminPanelIndexExists()) {
            val indexRequest = CreateIndexRequest(IndexManagementPlugin.ADMIN_PANEL_INDEX)
                .mapping(adminPanelMappings)
                .settings(Settings.builder().put(INDEX_HIDDEN, true).build())
            client.create(
                indexRequest,
                object : ActionListener<CreateIndexResponse> {
                    override fun onFailure(e: Exception) {
                        if (e is ResourceAlreadyExistsException) {
                            /* if two request create the admin panel index at the same time, may raise this exception */
                            /* but we don't take it as error */
                            actionListener.onResponse(
                                CreateIndexResponse(
                                    true,
                                    true,
                                    IndexManagementPlugin.ADMIN_PANEL_INDEX
                                )
                            )
                        } else actionListener.onFailure(e)
                    }

                    override fun onResponse(response: CreateIndexResponse) {
                        actionListener.onResponse(response)
                    }
                }
            )
        } else {
            IndexUtils.checkAndUpdateIndexMapping(
                IndexManagementPlugin.ADMIN_PANEL_INDEX,
                IndexUtils.getSchemaVersion(adminPanelMappings),
                adminPanelMappings,
                clusterService.state(),
                client,
                actionListener
            )
        }
    }

    private fun adminPanelIndexExists(): Boolean = clusterService.state().routingTable.hasIndex(IndexManagementPlugin.ADMIN_PANEL_INDEX)

    companion object {
        val adminPanelMappings = AdminPanelIndices::class.java.classLoader
            .getResource("mappings/opensearch-admin-panel.json")!!.readText()
    }
}
