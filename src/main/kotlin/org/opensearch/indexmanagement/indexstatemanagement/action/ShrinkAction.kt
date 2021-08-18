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
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import org.opensearch.jobscheduler.spi.JobExecutionContext

class ShrinkAction(
    clusterService: ClusterService,
    client: Client,
    managedIndexMetaData: ManagedIndexMetaData,
    config: ShrinkActionConfig,
    context: JobExecutionContext
) : Action(ActionType.SHRINK, config, managedIndexMetaData) {
    private val logger = org.apache.logging.log4j.LogManager.getLogger(javaClass)
    private val attemptMoveShardsStep = AttemptMoveShardsStep(clusterService, client, config, managedIndexMetaData, context)
    private val waitForMoveShardsStep = WaitForMoveShardsStep(clusterService, client, config, managedIndexMetaData, context)
    private val attemptShrinkStep = AttemptShrinkStep(clusterService, client, config, managedIndexMetaData, context)
    private val waitForShrinkStep = WaitForShrinkStep(clusterService, client, config, managedIndexMetaData, context)

    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptMoveShardsStep.name to attemptMoveShardsStep,
        WaitForMoveShardsStep.name to waitForMoveShardsStep,
        AttemptShrinkStep.name to attemptShrinkStep,
        WaitForShrinkStep.name to waitForShrinkStep
    )
    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    @SuppressWarnings("ReturnCount")
    override fun getStepToExecute(): Step {

        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptMoveShardsStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action, assume it is from another action.
        if (!stepNameToStep.containsKey(currentStep)) return attemptMoveShardsStep

        val currentStepStatus = stepMetaData.stepStatus
        logger.info("Current step status $currentStepStatus")
        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            logger.info("")
            return when (currentStep) {
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
