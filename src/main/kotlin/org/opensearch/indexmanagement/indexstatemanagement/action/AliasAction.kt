/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.step.alias.AttemptAliasActionsStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class AliasAction(
    val actions: List<IndicesAliasesRequest.AliasActions>,
    index: Int,
) : Action(name, index) {

    /**
     * Allowing the alias action to be only applicable on the managed index for ADD and REMOVE actions only.
     * https://github.com/opensearch-project/OpenSearch/blob/4d045a164e12a382881140e32f9285a3224fecc7/server/src/main/java/org/opensearch/action/admin/indices/alias/IndicesAliasesRequest.java#L105
     */
    init {
        require(actions.isNotEmpty()) { "At least one alias action needs to be specified." }
        val allowedActionTypes = listOf(IndicesAliasesRequest.AliasActions.Type.ADD, IndicesAliasesRequest.AliasActions.Type.REMOVE)
        require(actions.all { it.actionType() in allowedActionTypes }) { "Only ADD and REMOVE actions are allowed." }
        require(
            actions.all { it.indices().isNullOrEmpty() },
        ) { "Alias action can only work on its applied index so don't accept index/indices parameter." }
        require(
            actions.all { it.aliases().isNotEmpty() },
        ) { "At least one alias needs to be specified." }
    }

    private val attemptAliasActionsStep = AttemptAliasActionsStep(this)

    private val steps = listOf(attemptAliasActionsStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptAliasActionsStep
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
