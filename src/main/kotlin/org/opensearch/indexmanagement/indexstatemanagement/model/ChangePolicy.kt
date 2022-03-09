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
import org.opensearch.commons.authuser.User
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.opensearchapi.optionalUserField
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StateMetaData
import java.io.IOException

/**
 * The ChangePolicy data class is used for the eventual changing of policies in a managed index.
 * Whether it be completely different policies, or the same policy, but a new version. If a ChangePolicy
 * exists on a [ManagedIndexConfig] then when we are in the transition part of a state we will always check
 * if a ChangePolicy exists and attempt to change the policy. If the change happens, the ChangePolicy on
 * [ManagedIndexConfig] will be reset to null.
 *
 * The list of [StateFilter] are only used in the ChangePolicy API call where we will use them to filter out
 * the [ManagedIndexConfig]s to apply the ChangePolicy to.
 */
data class ChangePolicy(
    val policyID: String,
    val state: String?,
    val include: List<StateFilter>,
    val isSafe: Boolean,
    val user: User? = null
) : Writeable, ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        state = sin.readOptionalString(),
        include = sin.readList(::StateFilter),
        isSafe = sin.readBoolean(),
        user = if (sin.readBoolean()) {
            User(sin)
        } else null
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
            .field(ManagedIndexConfig.POLICY_ID_FIELD, policyID)
            .field(StateMetaData.STATE, state)
            .field(IS_SAFE_FIELD, isSafe)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(USER_FIELD, user)
        return builder.endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
        out.writeOptionalString(state)
        out.writeList(include)
        out.writeBoolean(isSafe)
        out.writeBoolean(user != null)
        user?.writeTo(out)
    }

    companion object {
        const val POLICY_ID_FIELD = "policy_id"
        const val STATE_FIELD = "state"
        const val INCLUDE_FIELD = "include"
        const val IS_SAFE_FIELD = "is_safe"
        const val USER_FIELD = "user"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ChangePolicy {
            var policyID: String? = null
            var state: String? = null
            var isSafe: Boolean = false
            var user: User? = null
            val include = mutableListOf<StateFilter>()

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    POLICY_ID_FIELD -> policyID = xcp.text()
                    STATE_FIELD -> state = xcp.textOrNull()
                    INCLUDE_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            include.add(StateFilter.parse(xcp))
                        }
                    }
                    IS_SAFE_FIELD -> isSafe = xcp.booleanValue()
                    USER_FIELD -> {
                        user = if (xcp.currentToken() == Token.VALUE_NULL) null else User.parse(xcp)
                    }
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ChangePolicy.")
                }
            }

            return ChangePolicy(
                requireNotNull(policyID) { "ChangePolicy policy id is null" },
                state,
                include.toList(),
                isSafe,
                user
            )
        }
    }
}
