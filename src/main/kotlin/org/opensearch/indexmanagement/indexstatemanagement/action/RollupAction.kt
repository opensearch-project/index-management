/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.rollup.model.ISMRollup
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RollupAction(
    val ismRollup: ISMRollup,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "rollup"
    }

    override fun getStepToExecute(context: StepContext): Step {
        TODO("Not yet implemented")
    }

    override fun getSteps(): List<Step> {
        TODO("Not yet implemented")
    }
}
