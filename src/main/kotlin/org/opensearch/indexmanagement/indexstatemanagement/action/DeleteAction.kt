/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.delete.AttemptDeleteStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class DeleteAction(
    index: Int,
    val deleteSnapshot: Boolean = false,
) : Action(name, index) {
    companion object {
        const val name = "delete"
        const val DELETE_SNAPSHOT_FIELD = "delete_snapshot"
    }

    private val attemptDeleteStep = AttemptDeleteStep(this)
    private val steps = listOf(attemptDeleteStep)

    override fun getStepToExecute(context: StepContext): Step = attemptDeleteStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        if (deleteSnapshot) {
            builder.field(DELETE_SNAPSHOT_FIELD, deleteSnapshot)
        }
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(actionIndex)
        out.writeBoolean(deleteSnapshot)
    }
}
