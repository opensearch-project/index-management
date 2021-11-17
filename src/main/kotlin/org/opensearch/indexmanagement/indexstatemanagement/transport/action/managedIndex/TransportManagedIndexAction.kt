/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex

import org.opensearch.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService

/**
 * This is a non operational transport action that is used by ISM to check if the user has required index permissions to manage index
 */
class TransportManagedIndexAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
) : HandledTransportAction<ManagedIndexRequest, AcknowledgedResponse>(
    ManagedIndexAction.NAME, transportService, actionFilters, ::ManagedIndexRequest
) {

    override fun doExecute(task: Task, request: ManagedIndexRequest, listener: ActionListener<AcknowledgedResponse>) {
        // Do nothing
        return listener.onResponse(AcknowledgedResponse(true))
    }
}
