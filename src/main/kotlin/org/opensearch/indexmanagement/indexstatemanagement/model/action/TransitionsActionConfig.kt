/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.indexmanagement.indexstatemanagement.action.Action
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.Transition
import org.opensearch.script.ScriptService

data class TransitionsActionConfig(
    val transitions: List<Transition>
) : ActionConfig(ActionType.TRANSITION, -1) {

    override fun toAction(
        clusterService: ClusterService,
        scriptService: ScriptService,
        client: Client,
        settings: Settings,
        managedIndexMetaData: ManagedIndexMetaData
    ): Action = TransitionsAction(clusterService, client, managedIndexMetaData, this)
}
