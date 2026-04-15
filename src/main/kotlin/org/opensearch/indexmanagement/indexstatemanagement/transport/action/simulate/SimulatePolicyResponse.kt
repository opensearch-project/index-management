/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import java.io.IOException

class SimulatePolicyResponse :
    ActionResponse,
    ToXContentObject {
    val simulateResults: List<IndexSimulateResult>

    constructor(simulateResults: List<IndexSimulateResult>) : super() {
        this.simulateResults = simulateResults
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        simulateResults = sin.readList(::IndexSimulateResult),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeList(simulateResults)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.startArray(SIMULATE_RESULTS_FIELD)
        simulateResults.forEach { it.toXContent(builder, params) }
        builder.endArray()
        return builder.endObject()
    }

    companion object {
        const val SIMULATE_RESULTS_FIELD = "simulate_results"
    }
}

/**
 * Holds the simulation result for a single index.
 *
 * @param indexName         the index being simulated
 * @param indexUUID         the UUID of the index (null when the index was not found)
 * @param policyId          the policy used for the simulation
 * @param isManaged         whether the index is currently managed by this policy
 * @param currentState      the state the index is currently in (or would start in)
 * @param currentAction     the action that would execute next in the current state
 * @param transitionEvaluation per-transition condition evaluations for the current state
 * @param nextState         the state the index would transition to (first condition met), null if none
 * @param error             index-level error message (e.g. index not found), null on success
 */
data class IndexSimulateResult(
    val indexName: String,
    val indexUUID: String?,
    val policyId: String,
    val isManaged: Boolean,
    val currentState: String?,
    val currentAction: String?,
    val transitionEvaluation: List<TransitionSimulateResult>?,
    val nextState: String?,
    val error: String?,
) : ToXContentObject,
    Writeable {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indexName = sin.readString(),
        indexUUID = sin.readOptionalString(),
        policyId = sin.readString(),
        isManaged = sin.readBoolean(),
        currentState = sin.readOptionalString(),
        currentAction = sin.readOptionalString(),
        transitionEvaluation = sin.readOptionalList(::TransitionSimulateResult),
        nextState = sin.readOptionalString(),
        error = sin.readOptionalString(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(indexName)
        out.writeOptionalString(indexUUID)
        out.writeString(policyId)
        out.writeBoolean(isManaged)
        out.writeOptionalString(currentState)
        out.writeOptionalString(currentAction)
        if (transitionEvaluation != null) {
            out.writeBoolean(true)
            out.writeList(transitionEvaluation)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalString(nextState)
        out.writeOptionalString(error)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(INDEX_NAME_FIELD, indexName)
        builder.field(POLICY_ID_FIELD, policyId)
        builder.field(IS_MANAGED_FIELD, isManaged)
        if (error != null) {
            builder.field(ERROR_FIELD, error)
        } else {
            builder.field(CURRENT_STATE_FIELD, currentState)
            builder.field(CURRENT_ACTION_FIELD, currentAction)
            if (transitionEvaluation != null) {
                builder.startArray(TRANSITION_EVALUATION_FIELD)
                transitionEvaluation.forEach { it.toXContent(builder, params) }
                builder.endArray()
            }
            builder.field(NEXT_STATE_FIELD, nextState)
        }
        return builder.endObject()
    }

    companion object {
        const val INDEX_NAME_FIELD = "index_name"
        const val POLICY_ID_FIELD = "policy_id"
        const val IS_MANAGED_FIELD = "is_managed"
        const val CURRENT_STATE_FIELD = "current_state"
        const val CURRENT_ACTION_FIELD = "current_action"
        const val TRANSITION_EVALUATION_FIELD = "transition_evaluation"
        const val NEXT_STATE_FIELD = "next_state"
        const val ERROR_FIELD = "error"
    }
}

/**
 * Holds the condition evaluation result for a single transition within a state.
 *
 * @param stateName     the target state this transition would move to
 * @param conditionMet  whether the transition condition is currently satisfied
 * @param conditionType the kind of condition being evaluated (e.g. "min_index_age"), or "unconditional"
 * @param currentValue  human-readable current value of the metric being checked
 * @param requiredValue human-readable threshold required by the condition
 */
data class TransitionSimulateResult(
    val stateName: String,
    val conditionMet: Boolean,
    val conditionType: String,
    val currentValue: String?,
    val requiredValue: String?,
) : ToXContentObject,
    Writeable {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        stateName = sin.readString(),
        conditionMet = sin.readBoolean(),
        conditionType = sin.readString(),
        currentValue = sin.readOptionalString(),
        requiredValue = sin.readOptionalString(),
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(stateName)
        out.writeBoolean(conditionMet)
        out.writeString(conditionType)
        out.writeOptionalString(currentValue)
        out.writeOptionalString(requiredValue)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder =
        builder.startObject()
            .field(STATE_NAME_FIELD, stateName)
            .field(CONDITION_MET_FIELD, conditionMet)
            .field(CONDITION_TYPE_FIELD, conditionType)
            .also { if (currentValue != null) it.field(CURRENT_VALUE_FIELD, currentValue) }
            .also { if (requiredValue != null) it.field(REQUIRED_VALUE_FIELD, requiredValue) }
            .endObject()

    companion object {
        const val STATE_NAME_FIELD = "state_name"
        const val CONDITION_MET_FIELD = "condition_met"
        const val CONDITION_TYPE_FIELD = "condition_type"
        const val CURRENT_VALUE_FIELD = "current_value"
        const val REQUIRED_VALUE_FIELD = "required_value"
    }
}

// Helper to read an optional list from StreamInput
private fun <T> StreamInput.readOptionalList(reader: Writeable.Reader<T>): List<T>? = if (readBoolean()) readList(reader) else null
