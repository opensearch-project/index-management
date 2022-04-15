/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import org.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import org.opensearch.indexmanagement.indexstatemanagement.action.DeleteAction
import org.opensearch.indexmanagement.indexstatemanagement.action.TransitionsAction
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import java.io.IOException

data class State(
    val name: String,
    val actions: List<Action>,
    val transitions: List<Transition>
) : ToXContentObject, Writeable {

    init {
        require(name.isNotBlank()) { "State must contain a valid name" }
        var hasDelete = false
        actions.forEach { action ->
            // dont allow actions after delete as they will never happen
            require(!hasDelete) { "State=$name must not contain an action after a delete action" }
            hasDelete = action.type == DeleteAction.name || action.deleteIndexMetadataAfterFinish()
        }

        // dont allow transitions if state contains delete
        if (hasDelete) require(transitions.isEmpty()) { "State=$name cannot contain transitions if using delete action" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(NAME_FIELD, name)
            .startArray(ACTIONS_FIELD)
            .also { actions.forEach { action -> action.toXContent(it, params) } }
            .endArray()
            .field(TRANSITIONS_FIELD, transitions.toTypedArray())
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readList { ISMActionsParser.instance.fromStreamInput(it) },
        sin.readList(::Transition)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeList(actions)
        out.writeList(transitions)
    }

    fun getActionToExecute(
        managedIndexMetaData: ManagedIndexMetaData,
        indexMetadataProvider: IndexMetadataProvider
    ): Action? {
        var actionConfig: Action?
        val actionMetaData = managedIndexMetaData.actionMetaData
        // If we are transitioning to this state get the first action in the state
        // If the action/actionIndex are null it means we just initialized and should get the first action from the state
        if (managedIndexMetaData.transitionTo != null || actionMetaData == null) {
            actionConfig = this.actions.firstOrNull() ?: TransitionsAction(this.transitions, indexMetadataProvider)
        } else if (actionMetaData.name == TransitionsAction.name) {
            // If the current action is transition and we do not have a transitionTo set then we should be in Transition
            actionConfig = TransitionsAction(this.transitions, indexMetadataProvider)
        } else {
            // Get the current actionConfig that is in the ManagedIndexMetaData
            actionConfig = this.actions.filterIndexed { index, config ->
                index == actionMetaData.index && config.type == actionMetaData.name
            }.firstOrNull()
            if (actionConfig == null) return null

            val stepMetaData = managedIndexMetaData.stepMetaData
            // TODO: Refactor so we can get isLastStep from somewhere besides an instantiated Action class so we can simplify this to a when block
            // If stepCompleted is true and this is the last step of the action then we should get the next action
            if (stepMetaData != null && stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
                val action = actionConfig
                if (action.isLastStep(stepMetaData.name)) {
                    actionConfig = this.actions.getOrNull(actionMetaData.index + 1) ?: TransitionsAction(this.transitions, indexMetadataProvider)
                }
            }
        }

        return actionConfig
    }

    companion object {
        const val NAME_FIELD = "name"
        const val ACTIONS_FIELD = "actions"
        const val TRANSITIONS_FIELD = "transitions"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): State {
            var name: String? = null
            val actions: MutableList<Action> = mutableListOf()
            val transitions: MutableList<Transition> = mutableListOf()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    ACTIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            actions.add(ISMActionsParser.instance.parse(xcp, actions.size))
                        }
                    }
                    TRANSITIONS_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            transitions.add(Transition.parse(xcp))
                        }
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in State.")
                }
            }

            return State(
                name = requireNotNull(name) { "State name is null" },
                actions = actions.toList(),
                transitions = transitions.toList()
            )
        }
    }
}
