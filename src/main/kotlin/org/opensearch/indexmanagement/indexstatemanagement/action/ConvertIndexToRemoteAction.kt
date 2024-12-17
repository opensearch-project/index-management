/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.AttemptRestoreStep
import org.opensearch.indexmanagement.indexstatemanagement.step.restore.WaitForRestoreStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ConvertIndexToRemoteAction(
    val repository: String,
    index: Int,
) : Action(name, index) {

    companion object {
        const val name = "convert_index_to_remote"
        const val REPOSITORY_FIELD = "repository"

        @JvmStatic
        fun fromStreamInput(si: StreamInput): ConvertIndexToRemoteAction {
            val repository = si.readString()
            val index = si.readInt()
            return ConvertIndexToRemoteAction(repository, index)
        }
    }

    private val attemptRestoreStep = AttemptRestoreStep(this)
    private val waitForRestoreStep = WaitForRestoreStep()

    private val steps = listOf(attemptRestoreStep, waitForRestoreStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        // If stepMetaData is null, return the first step (attemptRestoreStep)
        val stepMetaData = context.metadata.stepMetaData ?: return attemptRestoreStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptRestoreStep.name -> waitForRestoreStep
                else -> attemptRestoreStep // Default to the first step
            }
        }

        // If the current step is not completed, continue executing it
        return when (stepMetaData.name) {
            AttemptRestoreStep.name -> attemptRestoreStep
            else -> waitForRestoreStep
        }
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(REPOSITORY_FIELD, repository)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(repository)
        out.writeInt(actionIndex)
    }
}
