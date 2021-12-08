/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.model.destination.Destination
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.script.Script

class NotificationAction(
    val destination: Destination,
    val messageTemplate: Script,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "notification"
    }

    override fun getStepToExecute(context: StepContext): Step {
        TODO("Not yet implemented")
    }

    override fun getSteps(): List<Step> {
        TODO("Not yet implemented")
    }
}
