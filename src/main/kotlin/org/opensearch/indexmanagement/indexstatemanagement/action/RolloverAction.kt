/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RolloverActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.rollover.AttemptRolloverStep

class RolloverAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: RolloverActionConfig
) : Action(ActionType.ROLLOVER, config, managedIndexMetaData) {

    private val attemptRolloverStep = AttemptRolloverStep(clusterService, client, config, managedIndexMetaData)
    private val steps = listOf(attemptRolloverStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(): Step {
        return attemptRolloverStep
    }
}
