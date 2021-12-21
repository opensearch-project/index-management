/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptReplicaCountStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReplicaCountAction(
    val numOfReplicas: Int,
    index: Int
) : Action(name, index) {

    init {
        require(numOfReplicas >= 0) { "ReplicaCountAction number_of_replicas value must be a non-negative number" }
    }

    private val attemptReplicaCountStep = AttemptReplicaCountStep(this)
    private val steps = listOf(attemptReplicaCountStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptReplicaCountStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(NUMBER_OF_REPLICAS_FIELD, numOfReplicas)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(numOfReplicas)
        out.writeInt(actionIndex)
    }

    companion object {
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"
        const val name = "replica_count"
    }
}
