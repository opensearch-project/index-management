/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO

// totalPolicies may differ from the length of the policies field if the size parameter is introduced
class GetSMPoliciesResponse(
    val policies: List<SMPolicy>,
    val totalPolicies: Long,
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        policies = sin.readList(::SMPolicy),
        totalPolicies = sin.readLong(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(policies)
        out.writeLong(totalPolicies)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startArray("policies")
            .apply {
                for (policy in policies) {
                    this.startObject()
                        .field(_ID, policy.id)
                        .field(_SEQ_NO, policy.seqNo)
                        .field(_PRIMARY_TERM, policy.primaryTerm)
                        .field(SMPolicy.SM_TYPE, policy, XCONTENT_WITHOUT_TYPE_AND_USER)
                        .endObject()
                }
            }
            .endArray()
            .field("total_policies", totalPolicies)
            .endObject()
    }
}
