/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.commons.authuser.User
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.opensearchapi.instant
import org.opensearch.indexmanagement.opensearchapi.optionalISMTemplateField
import org.opensearch.indexmanagement.opensearchapi.optionalTimeField
import org.opensearch.indexmanagement.opensearchapi.optionalUserField
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.util.IndexUtils
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException
import java.time.Instant

data class Policy(
    val id: String = NO_ID,
    val seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    val primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    val description: String,
    val schemaVersion: Long,
    val lastUpdatedTime: Instant,
    val errorNotification: ErrorNotification?,
    val defaultState: String,
    val states: List<State>,
    val ismTemplate: List<ISMTemplate>? = null,
    val user: User? = null,
) : ToXContentObject, Writeable {

    init {
        val distinctStateNames = states.map { it.name }.distinct()
        states.forEach { state ->
            state.transitions.forEach { transition ->
                require(distinctStateNames.contains(transition.stateName)) {
                    "Policy contains a transition in state=${state.name} pointing to a nonexistent state=${transition.stateName}"
                }
            }
        }
        require(distinctStateNames.size == states.size) { "Policy cannot have duplicate state names" }
        require(states.isNotEmpty()) { "Policy must contain at least one State" }
        requireNotNull(states.find { it.name == defaultState }) { "Policy must have a valid default state" }
    }

    fun toXContent(builder: XContentBuilder): XContentBuilder {
        return toXContent(builder, ToXContent.EMPTY_PARAMS)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.startObject(POLICY_TYPE)
        builder.field(POLICY_ID_FIELD, id)
            .field(DESCRIPTION_FIELD, description)
            .optionalTimeField(LAST_UPDATED_TIME_FIELD, lastUpdatedTime)
            .field(SCHEMA_VERSION_FIELD, schemaVersion)
            .field(ERROR_NOTIFICATION_FIELD, errorNotification)
            .field(DEFAULT_STATE_FIELD, defaultState)
            .startArray(STATES_FIELD)
            .also { states.forEach { state -> state.toXContent(it, params) } }
            .endArray()
            .optionalISMTemplateField(ISM_TEMPLATE, ismTemplate)
        if (params.paramAsBoolean(WITH_USER, true)) builder.optionalUserField(USER_FIELD, user)
        if (params.paramAsBoolean(WITH_TYPE, true)) builder.endObject()
        return builder.endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        description = sin.readString(),
        schemaVersion = sin.readLong(),
        lastUpdatedTime = sin.readInstant(),
        errorNotification = sin.readOptionalWriteable(::ErrorNotification),
        defaultState = sin.readString(),
        states = sin.readList(::State),
        ismTemplate = if (sin.readBoolean()) {
            sin.readList(::ISMTemplate)
        } else {
            null
        },
        user = if (sin.readBoolean()) {
            User(sin)
        } else {
            null
        },
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeString(description)
        out.writeLong(schemaVersion)
        out.writeInstant(lastUpdatedTime)
        out.writeOptionalWriteable(errorNotification)
        out.writeString(defaultState)
        out.writeList(states)
        if (ismTemplate != null) {
            out.writeBoolean(true)
            out.writeList(ismTemplate)
        } else {
            out.writeBoolean(false)
        }
        out.writeBoolean(user != null)
        user?.writeTo(out)
    }

    /**
     * Disallowed actions are ones that are not specified in the [ManagedIndexSettings.ALLOW_LIST] setting.
     */
    fun getDisallowedActions(allowList: List<String>): List<String> {
        val allowListSet = allowList.toSet()
        val disallowedActions = mutableListOf<String>()
        this.states.forEach { state ->
            state.actions.forEach { actionConfig ->
                if (!allowListSet.contains(actionConfig.type)) {
                    disallowedActions.add(actionConfig.type)
                }
            }
        }
        return disallowedActions.distinct()
    }

    fun getStateToExecute(managedIndexMetaData: ManagedIndexMetaData): State? {
        if (managedIndexMetaData.transitionTo != null) {
            return this.states.find { it.name == managedIndexMetaData.transitionTo }
        }
        return this.states.find {
            val stateMetaData = managedIndexMetaData.stateMetaData
            stateMetaData != null && it.name == stateMetaData.name
        }
    }

    companion object {
        const val POLICY_TYPE = "policy"
        const val POLICY_ID_FIELD = "policy_id"
        const val DESCRIPTION_FIELD = "description"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val SCHEMA_VERSION_FIELD = "schema_version"
        const val ERROR_NOTIFICATION_FIELD = "error_notification"
        const val DEFAULT_STATE_FIELD = "default_state"
        const val STATES_FIELD = "states"
        const val ISM_TEMPLATE = "ism_template"
        const val USER_FIELD = "user"

        @Suppress("ComplexMethod", "LongMethod", "NestedBlockDepth")
        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(
            xcp: XContentParser,
            id: String = NO_ID,
            seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        ): Policy {
            var description: String? = null
            var defaultState: String? = null
            var errorNotification: ErrorNotification? = null
            var lastUpdatedTime: Instant? = null
            var schemaVersion: Long = IndexUtils.DEFAULT_SCHEMA_VERSION
            val states: MutableList<State> = mutableListOf()
            var ismTemplates: List<ISMTemplate>? = null
            var user: User? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SCHEMA_VERSION_FIELD -> schemaVersion = xcp.longValue()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    POLICY_ID_FIELD -> { /* do nothing as this is an internal field */ }
                    DESCRIPTION_FIELD -> description = xcp.text()
                    ERROR_NOTIFICATION_FIELD -> errorNotification = if (xcp.currentToken() == Token.VALUE_NULL) null else ErrorNotification.parse(xcp)
                    DEFAULT_STATE_FIELD -> defaultState = xcp.text()
                    STATES_FIELD -> {
                        ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                        while (xcp.nextToken() != Token.END_ARRAY) {
                            states.add(State.parse(xcp))
                        }
                    }
                    ISM_TEMPLATE -> {
                        if (xcp.currentToken() != Token.VALUE_NULL) {
                            ismTemplates = mutableListOf()
                            when (xcp.currentToken()) {
                                Token.START_ARRAY -> {
                                    while (xcp.nextToken() != Token.END_ARRAY) {
                                        ismTemplates.add(ISMTemplate.parse(xcp))
                                    }
                                }
                                Token.START_OBJECT -> {
                                    ismTemplates.add(ISMTemplate.parse(xcp))
                                }
                                else -> ensureExpectedToken(Token.START_ARRAY, xcp.currentToken(), xcp)
                            }
                        }
                    }
                    USER_FIELD -> user = if (xcp.currentToken() == Token.VALUE_NULL) null else User.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in Policy.")
                }
            }

            return Policy(
                id = id,
                seqNo = seqNo,
                primaryTerm = primaryTerm,
                description = requireNotNull(description) { "$DESCRIPTION_FIELD is null" },
                schemaVersion = schemaVersion,
                lastUpdatedTime = lastUpdatedTime ?: Instant.now(),
                errorNotification = errorNotification,
                defaultState = requireNotNull(defaultState) { "$DEFAULT_STATE_FIELD is null" },
                states = states.toList(),
                ismTemplate = ismTemplates,
                user = user,
            )
        }
    }
}
