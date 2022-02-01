/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement

import org.opensearch.client.Client
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.spi.indexstatemanagement.NewClusterEventHandler

/**
 * Notifies the NewClusterEventHandlers from all extensions whenever a ClusterChangedEvent of the `isNewCluster` type occurs,
 * enabling extensions to react to `new cluster` typed events.
 */
class NewClusterEventListener(
    val client: Client,
    val clusterService: ClusterService,
    private val newClusterEventHandlers: List<NewClusterEventHandler>
) : ClusterStateListener {

    init {
        clusterService.addListener(this)
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        if (event.isNewCluster) {
            newClusterEventHandlers.forEach { eventHandler ->
                eventHandler.processEvent(client, clusterService, event)
            }
        }
    }
}
