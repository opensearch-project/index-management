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

/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig.ActionType
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ShrinkActionConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.Step
import org.opensearch.indexmanagement.indexstatemanagement.step.shrink.*

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
            when (currentStep) {
                AttemptCheckConfigStep.name -> return attemptMoveShardsStep
                AttemptMoveShardsStep.name -> return waitForMoveShardsStep
                WaitForMoveShardsStep.name -> return attemptShrinkStep
                AttemptShrinkStep.name -> return waitForShrinkStep
                else -> stepNameToStep[currentStep]!!
            }
        }
        // step not completed
        return stepNameToStep[currentStep]!!
    }
}
