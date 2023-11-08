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
import org.opensearch.index.query.BoolQueryBuilder
import org.opensearch.index.query.Operator
import org.opensearch.index.query.QueryBuilders
import org.opensearch.indexmanagement.indexstatemanagement.util.MANAGED_INDEX_POLICY_ID_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import java.io.IOException

data class ExplainFilter(
    val policyID: String?,
    val state: String?,
    val actionType: String?,
    val failed: Boolean?
) : Writeable {

    constructor() : this(
        policyID = null,
        state = null,
        actionType = null,
        failed = null
    )

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readOptionalString(),
        state = sin.readOptionalString(),
        actionType = sin.readOptionalString(),
        failed = sin.readOptionalBoolean()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(policyID)
        out.writeOptionalString(state)
        out.writeOptionalString(actionType)
        out.writeOptionalBoolean(failed)
    }

    fun convertToBoolQueryBuilder(): BoolQueryBuilder {
        val boolQuery = QueryBuilders.boolQuery()

        if (policyID != null) {
            boolQuery.must(
                QueryBuilders
                    .queryStringQuery(policyID)
                    .field(MANAGED_INDEX_POLICY_ID_FIELD)
                    .defaultOperator(Operator.AND)
            )
        }

        return boolQuery
    }

    fun validMetaData(metaData: ManagedIndexMetaData): Boolean {
        var ok = true

        val stateMetaData = metaData.stateMetaData
        if (state != null && (stateMetaData == null || stateMetaData.name != state)) {
            ok = false
        }

        val actionMetaData = metaData.actionMetaData
        if (actionType != null && (actionMetaData == null || actionMetaData.name != actionType)) {
            ok = false
        }

        if (failed != null && (actionMetaData == null || actionMetaData.failed != failed)) {
            ok = false
        }

        return ok
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
