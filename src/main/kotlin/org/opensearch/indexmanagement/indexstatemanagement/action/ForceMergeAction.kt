/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptCallForceMergeStep
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.AttemptSetReadOnlyStep
import org.opensearch.indexmanagement.indexstatemanagement.step.forcemerge.WaitForForceMergeStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ForceMergeAction(
    val maxNumSegments: Int,
    index: Int
) : Action(name, index) {

    init {
        require(maxNumSegments > 0) { "Force merge {$MAX_NUM_SEGMENTS_FIELD} must be greater than 0" }
    }

    private val attemptSetReadOnlyStep = AttemptSetReadOnlyStep(this)
    private val attemptCallForceMergeStep = AttemptCallForceMergeStep(this)
    private val waitForForceMergeStep = WaitForForceMergeStep(this)

    // Using a LinkedHashMap here to maintain order of steps for getSteps() while providing a convenient way to
    // get the current Step object using the current step's name in getStepToExecute()
    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptSetReadOnlyStep.name to attemptSetReadOnlyStep,
        AttemptCallForceMergeStep.name to attemptCallForceMergeStep,
        WaitForForceMergeStep.name to waitForForceMergeStep
    )

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        val managedIndexMetaData = context.metadata
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

    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(MAX_NUM_SEGMENTS_FIELD, maxNumSegments)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(maxNumSegments)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "force_merge"
        const val MAX_NUM_SEGMENTS_FIELD = "max_num_segments"
    }
}
