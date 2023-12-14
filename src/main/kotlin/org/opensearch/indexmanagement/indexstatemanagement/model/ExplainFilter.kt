/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.indexstatemanagement.util.MANAGED_INDEX_POLICY_ID_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import java.io.IOException

data class ExplainFilter(
    val policyID: String? = null,
    val state: String? = null,
    val actionType: String? = null,
    val failed: Boolean? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readOptionalString(),
        state = sin.readOptionalString(),
        actionType = sin.readOptionalString(),
        failed = sin.readOptionalBoolean()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.startObject(FILTER_FIELD)

        if (policyID != null) builder.field(POLICY_ID_FIELD, policyID)
        if (state != null) builder.field(STATE_FIELD, state)
        if (actionType != null) builder.field(ACTION_FIELD, actionType)
        if (failed != null) builder.field(FAILED_FIELD, failed)

        builder.endObject()
        return builder.endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(policyID)
        out.writeOptionalString(state)
        out.writeOptionalString(actionType)
        out.writeOptionalBoolean(failed)
    }

    fun byMetaData(metaData: ManagedIndexMetaData): Boolean {
        var isValid = true

        val stateMetaData = metaData.stateMetaData
        if (state != null && (stateMetaData == null || stateMetaData.name != state)) {
            isValid = false
        }

        val actionMetaData = metaData.actionMetaData
        if (actionType != null && (actionMetaData == null || actionMetaData.name != actionType)) {
            isValid = false
        }

        val retryInfoMetaData = metaData.policyRetryInfo
        val actionFailedNotValid = actionMetaData == null || actionMetaData.failed != failed
        val retryFailedNotValid = retryInfoMetaData == null || retryInfoMetaData.failed != failed
        if (failed != null && actionFailedNotValid && retryFailedNotValid) {
            isValid = false
        }

        return isValid
    }

    companion object {
        const val FILTER_FIELD = "filter"
        const val POLICY_ID_FIELD = "policy_id"
        const val STATE_FIELD = "state"
        const val ACTION_FIELD = "action_type"
        const val FAILED_FIELD = "failed"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): ExplainFilter {
            var policyID: String? = null
            var state: String? = null
            var actionType: String? = null
            var failed: Boolean? = null

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
                                ACTION_FIELD -> actionType = xcp.text()
                                FAILED_FIELD -> failed = xcp.booleanValue()
                            }
                        }
                    }
                }
            }

            return ExplainFilter(policyID, state, actionType, failed)
        }
    }
}

fun BoolQueryBuilder.filterByPolicyID(explainFilter: ExplainFilter?): BoolQueryBuilder {
    if (explainFilter?.policyID != null) {
        this.filter(QueryBuilders.termsQuery(MANAGED_INDEX_POLICY_ID_FIELD, explainFilter.policyID))
    }

    return this
}
