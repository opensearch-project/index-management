/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.OpenActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep

class OpenAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: OpenActionConfig
) : Action(ActionType.OPEN, config, managedIndexMetaData) {

    private val attemptOpenStep = AttemptOpenStep(clusterService, client, config, managedIndexMetaData)

    private val steps = listOf(attemptOpenStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return attemptOpenStep
    }
}
