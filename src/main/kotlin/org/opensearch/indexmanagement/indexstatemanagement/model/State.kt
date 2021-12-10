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
import org.opensearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import java.io.IOException

data class State(
    val name: String,
    val actions: List<ActionConfig>,
    val transitions: List<Transition>
) : ToXContentObject, Writeable {

    init {
        require(name.isNotBlank()) { "State must contain a valid name" }
        var hasDelete = false
        actions.forEach { actionConfig ->
            // dont allow actions after delete as they will never happen
            require(!hasDelete) { "State=$name must not contain an action after a delete action" }
            hasDelete = actionConfig.type == ActionConfig.ActionType.DELETE
        }

        // dont allow transitions if state contains delete
        if (hasDelete) require(transitions.isEmpty()) { "State=$name cannot contain transitions if using delete action" }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(ACTIONS_FIELD, actions.toTypedArray())
            .field(TRANSITIONS_FIELD, transitions.toTypedArray())
            .endObject()
        return builder
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(),
        sin.readList { ActionConfig.fromStreamInput(it) },
        sin.readList(::Transition)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(name)
        out.writeList(actions)
        out.writeList(transitions)
    }

    companion object {
        const val NAME_FIELD = "name"
        const val ACTIONS_FIELD = "actions"
        const val TRANSITIONS_FIELD = "transitions"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): State {
            var name: String? = null
            val actions: MutableList<ActionConfig> = mutableListOf()
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
                            actions.add(ActionConfig.parse(xcp, actions.size))
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
