/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

data class ExplainFilter(
    val filterPolicyID: String?,
    val filterState: String?,
    val filterAction: String?
) : Writeable {

    constructor() : this(
        filterPolicyID = null,
        filterState = null,
        filterAction = null
    )

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        filterPolicyID = sin.readString(),
        filterState = sin.readString(),
        filterAction = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(filterPolicyID)
        out.writeString(filterState)
        out.writeString(filterAction)
    }

    fun isValid(managedIndexConfig: ManagedIndexConfig): Boolean {
        var valid = true

        if (filterPolicyID != null && filterPolicyID != managedIndexConfig.policyID) {
            valid = false
        }

        val policy = managedIndexConfig.policy

        if (filterState != null && policy != null && !policy.states.any { it.name == filterState }) {
            valid = false
        }

        if (filterAction != null && policy != null && !policy.states.any { state -> state.actions.any { it.type == filterAction } }) {
            valid = false
        }

        return valid
    }

    companion object {
        const val FILTER_FIELD = "filter"
        const val POLICY_ID_FIELD = "policy_id"
        const val STATE_FIELD = "state"
        const val ACTION_FIELD = "action_type"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ExplainFilter {
            var policyID: String? = null
            var state: String? = null
            var action: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FILTER_FIELD -> {
                        ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_OBJECT) {
                            val filter = xcp.currentName()
                            xcp.nextToken()

                            when (filter) {
                                POLICY_ID_FIELD -> policyID = xcp.text()
                                STATE_FIELD -> state = xcp.text()
                                ACTION_FIELD -> action = xcp.text()
                            }
                        }
                    }
                }
            }

            return ExplainFilter(policyID, state, action)
        }
    }
}
