/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ReadOnlyActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep

class ReadOnlyAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ReadOnlyActionConfig
) : Action(ActionType.READ_ONLY, config, managedIndexMetaData) {

    private val setReadOnlyStep = SetReadOnlyStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(setReadOnlyStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return setReadOnlyStep
    }
}
