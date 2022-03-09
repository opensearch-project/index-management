/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import java.io.IOException

class GetPoliciesResponse : ActionResponse, ToXContentObject {

    val policies: List<Policy>
    val totalPolicies: Int

    constructor(
        policies: List<Policy>,
        totalPolicies: Int
    ) : super() {
        this.policies = policies
        this.totalPolicies = totalPolicies
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policies = sin.readList(::Policy),
        totalPolicies = sin.readInt()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(policies)
        out.writeInt(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val policyParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false", Action.EXCLUDE_CUSTOM_FIELD_PARAM to "true"))
        return builder.startObject()
            .startArray("policies")
            .apply {
                for (policy in policies) {
                    this.startObject()
                        .field(_ID, policy.id)
                        .field(_SEQ_NO, policy.seqNo)
                        .field(_PRIMARY_TERM, policy.primaryTerm)
                        .field(Policy.POLICY_TYPE, policy, policyParams)
                        .endObject()
                }
            }
            .endArray()
            .field("total_policies", totalPolicies)
            .endObject()
    }
}
