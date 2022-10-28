/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.alias.AttemptAliasStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class AliasAction(
    val actions: List<IndicesAliasesRequest.AliasActions>,
    index: Int
) : Action(name, index) {

    init {
        require(actions.isNotEmpty()) { "At least one alias action needs to be specified." }
        require(!actions.any { it.actionType() == IndicesAliasesRequest.AliasActions.Type.REMOVE_INDEX }) { "Remove_index is not allowed here." }
        require(actions.all { it.indices().isNullOrEmpty() }) { "Alias actions are only allowed on managed indices." }
    }

    private val attemptAliasStep = AttemptAliasStep(this)

    private val steps = listOf(attemptAliasStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptAliasStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(ACTIONS, actions)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeList(actions)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "alias"
        const val ACTIONS = "actions"
    }
}
