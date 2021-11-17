/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ForceMergeActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptCallForceMergeStep
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptSetReadOnlyStep
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.WaitForForceMergeStep

class ForceMergeAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ForceMergeActionConfig
) : Action(ActionType.FORCE_MERGE, config, managedIndexMetaData) {

    private val attemptSetReadOnlyStep = AttemptSetReadOnlyStep(clusterService, client, config, managedIndexMetaData)
    private val attemptCallForceMergeStep = AttemptCallForceMergeStep(clusterService, client, config, managedIndexMetaData)
    private val waitForForceMergeStep = WaitForForceMergeStep(clusterService, client, config, managedIndexMetaData)

    // Using a LinkedHashMap here to maintain order of steps for getSteps() while providing a convenient way to
    // get the current Step object using the current step's name in getStepToExecute()
    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptSetReadOnlyStep.name to attemptSetReadOnlyStep,
        AttemptCallForceMergeStep.name to attemptCallForceMergeStep,
        WaitForForceMergeStep.name to waitForForceMergeStep
    )

    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    @Suppress("ReturnCount")
    override fun getStepToExecute(): Step {
        // If stepMetaData is null, return the first step in ForceMergeAction
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptSetReadOnlyStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action (assumed to be from the previous action in the policy), return
        // the first step in ForceMergeAction
        if (!stepNameToStep.containsKey(currentStep)) return attemptSetReadOnlyStep

        val currentStepStatus = stepMetaData.stepStatus

        // If the current step has completed, return the next step
        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            return when (currentStep) {
                AttemptSetReadOnlyStep.name -> attemptCallForceMergeStep
                AttemptCallForceMergeStep.name -> waitForForceMergeStep
                // Shouldn't hit this case but including it so that the when expression is exhaustive
                else -> stepNameToStep[currentStep]!!
            }
        }

        // If the current step has not completed, return it
        return stepNameToStep[currentStep]!!
    }
}
