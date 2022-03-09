/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReadWriteAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "read_write"
    }

    private val setReadWriteStep = SetReadWriteStep()
    private val steps = listOf(setReadWriteStep)

    override fun getStepToExecute(context: StepContext): Step {
        return setReadWriteStep
    }

    override fun getSteps(): List<Step> = steps
}
