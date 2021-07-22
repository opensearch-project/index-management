/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptCheckConfigStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep

class ShrinkAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ShrinkActionConfig
) : Action(ActionType.SHRINK, config, managedIndexMetaData) {

    private val attemptCheckConfigStep = AttemptCheckConfigStep(clusterService, client, config, managedIndexMetaData)
    private val attemptMoveShardsStep = AttemptMoveShardsStep(clusterService, client, config, managedIndexMetaData)
    private val waitForMoveShardsStep = WaitForMoveShardsStep(clusterService, client, config, managedIndexMetaData)
    private val attemptShrinkStep = AttemptShrinkStep(clusterService, client, config, managedIndexMetaData)
    private val waitForShrinkStep = WaitForShrinkStep(clusterService, client, config, managedIndexMetaData)

    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptCheckConfigStep.name to attemptCheckConfigStep,
        AttemptMoveShardsStep.name to attemptMoveShardsStep,
        WaitForMoveShardsStep.name to waitForMoveShardsStep,
        AttemptShrinkStep.name to attemptShrinkStep,
        WaitForShrinkStep.name to waitForShrinkStep
    )
    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    override fun getStepToExecute(): Step {
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptCheckConfigStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action, assume it is from another action.
        if (!stepNameToStep.containsKey(currentStep)) return attemptCheckConfigStep

        val currentStepStatus = stepMetaData.stepStatus

        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            return when (currentStep) {
                AttemptCheckConfigStep.name -> attemptMoveShardsStep
                AttemptMoveShardsStep.name -> waitForMoveShardsStep
                WaitForMoveShardsStep.name -> attemptShrinkStep
                AttemptShrinkStep.name -> waitForShrinkStep
                else -> stepNameToStep[currentStep]!!
            }
        }
        // step not completed
        return stepNameToStep[currentStep]!!
    }
}
