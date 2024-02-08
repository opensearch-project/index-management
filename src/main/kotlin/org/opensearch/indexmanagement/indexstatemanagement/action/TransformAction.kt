/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.AttemptCreateTransformJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.transform.WaitForTransformCompletionStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.transform.model.ISMTransform

class TransformAction(
    val ismTransform: ISMTransform,
    index: Int,
) : Action(name, index) {
    companion object {
        const val name = "transform"
        const val ISM_TRANSFORM_FIELD = "ism_transform"
    }

    private val attemptCreateTransformJobStep = AttemptCreateTransformJobStep(this)
    private val waitForTransformCompletionStep = WaitForTransformCompletionStep()
    private val steps = listOf(attemptCreateTransformJobStep, waitForTransformCompletionStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        // if stepMetaData is null, return first step
        val stepMetaData = context.metadata.stepMetaData ?: return attemptCreateTransformJobStep

        // if the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptCreateTransformJobStep.name -> waitForTransformCompletionStep
                else -> attemptCreateTransformJobStep
            }
        }

        return when (stepMetaData.name) {
            AttemptCreateTransformJobStep.name -> attemptCreateTransformJobStep
            else -> waitForTransformCompletionStep
        }
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(ISM_TRANSFORM_FIELD, ismTransform)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        ismTransform.writeTo(out)
        out.writeInt(actionIndex)
    }
}
