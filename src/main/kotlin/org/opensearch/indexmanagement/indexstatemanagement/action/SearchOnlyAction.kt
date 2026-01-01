/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.searchonly.AttemptSearchOnlyStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class SearchOnlyAction(
    index: Int,
) : Action(name, index) {
    companion object {
        const val name = "search_only"
    }

    private val attemptSearchOnlyStep = AttemptSearchOnlyStep()
    private val steps = listOf(attemptSearchOnlyStep)

    override fun getStepToExecute(context: StepContext): Step = attemptSearchOnlyStep

    override fun getSteps(): List<Step> = steps
}
