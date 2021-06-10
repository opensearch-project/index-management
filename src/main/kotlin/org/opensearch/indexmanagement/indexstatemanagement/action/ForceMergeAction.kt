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
