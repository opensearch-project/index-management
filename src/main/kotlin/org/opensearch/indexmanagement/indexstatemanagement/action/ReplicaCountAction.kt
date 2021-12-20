/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptReplicaCountStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReplicaCountAction(
    val numOfReplicas: Int,
    index: Int
) : Action(name, index) {

    companion object {
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"
        const val name = "replica_count"
    }

    private val attemptReplicaCountStep = AttemptReplicaCountStep()
    private val steps = listOf(attemptReplicaCountStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptReplicaCountStep
    }

    override fun getSteps(): List<Step> = steps
}
