/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.apache.logging.log4j.LogManager
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

private val log = LogManager.getLogger(ExplainFilter::class.java)

data class ExplainFilter(
    val policyID: String?,
    val state: String?,
    val actionType: String?,
    val failed: Boolean?
) : Writeable {

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

    fun byMetaData(metaData: ManagedIndexMetaData): Boolean {
        var isValid = true

        log.info(metaData)

        val stateMetaData = metaData.stateMetaData
        if (state != null && (stateMetaData == null || stateMetaData.name != state)) {
            isValid = false
        }

        val actionMetaData = metaData.actionMetaData
        if (actionType != null && (actionMetaData == null || actionMetaData.name != actionType)) {
            isValid = false
        }

        val retryInfoMetaData = metaData.policyRetryInfo
        if (failed != null && (actionMetaData == null || actionMetaData.failed != failed) &&
            (retryInfoMetaData == null || retryInfoMetaData.failed != failed)
        ) {
            isValid = false
        }

        log.info("IS VALID: $isValid")

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
        log.info("FILTER ON POLICY ID")

        return this.filter(QueryBuilders.termsQuery(MANAGED_INDEX_POLICY_ID_FIELD, explainFilter.policyID))
    }

    return this
}
