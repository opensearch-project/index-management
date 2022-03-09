/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.close.AttemptCloseStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class CloseAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "close"
    }
    private val attemptCloseStep = AttemptCloseStep()

    private val steps = listOf(attemptCloseStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptCloseStep
    }

    override fun getSteps(): List<Step> = steps
}
