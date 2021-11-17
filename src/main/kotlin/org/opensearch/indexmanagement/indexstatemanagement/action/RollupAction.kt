/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep

class RollupAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: RollupActionConfig
) : Action(ActionConfig.ActionType.ROLLUP, config, managedIndexMetaData) {

    private val attemptCreateRollupJobStep = AttemptCreateRollupJobStep(clusterService, client, config.ismRollup, managedIndexMetaData)
    private val waitForRollupCompletionStep = WaitForRollupCompletionStep(clusterService, client, managedIndexMetaData)

    override fun getSteps(): List<Step> = listOf(attemptCreateRollupJobStep, waitForRollupCompletionStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptCreateRollupJobStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptCreateRollupJobStep.name -> waitForRollupCompletionStep
                else -> attemptCreateRollupJobStep
            }
        }

        return when (stepMetaData.name) {
            AttemptCreateRollupJobStep.name -> attemptCreateRollupJobStep
            else -> waitForRollupCompletionStep
        }
    }
}
