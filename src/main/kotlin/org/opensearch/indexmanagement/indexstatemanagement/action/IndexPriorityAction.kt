/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

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

    companion object {
        const val name = "index_priority"
        const val INDEX_PRIORITY_FIELD = "priority"
    }
}
