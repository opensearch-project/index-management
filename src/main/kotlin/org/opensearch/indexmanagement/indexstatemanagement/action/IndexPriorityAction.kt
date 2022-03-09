/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.indexpriority.AttemptSetIndexPriorityStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class IndexPriorityAction(
    val indexPriority: Int,
    index: Int
) : Action(name, index) {

    init {
        require(indexPriority >= 0) { "IndexPriorityAction index_priority value must be a non-negative number" }
    }

    private val attemptSetIndexPriorityStep = AttemptSetIndexPriorityStep(this)
    private val steps = listOf(attemptSetIndexPriorityStep)

    override fun getStepToExecute(context: StepContext): Step = attemptSetIndexPriorityStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(INDEX_PRIORITY_FIELD, indexPriority)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(indexPriority)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "index_priority"
        const val INDEX_PRIORITY_FIELD = "priority"
    }
}
