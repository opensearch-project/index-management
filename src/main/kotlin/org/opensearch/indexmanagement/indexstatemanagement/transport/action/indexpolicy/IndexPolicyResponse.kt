/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action.Companion.EXCLUDE_CUSTOM_FIELD_PARAM
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.indexmanagement.util._VERSION
import org.opensearch.rest.RestStatus
import java.io.IOException

class IndexPolicyResponse : ActionResponse, ToXContentObject {

    val id: String
    val version: Long
    val primaryTerm: Long
    val seqNo: Long
    val policy: Policy
    val status: RestStatus

    constructor(
        id: String,
        version: Long,
        primaryTerm: Long,
        seqNo: Long,
        policy: Policy,
        status: RestStatus
    ) : super() {
        this.id = id
        this.version = version
        this.primaryTerm = primaryTerm
        this.seqNo = seqNo
        this.policy = policy
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        primaryTerm = sin.readLong(),
        seqNo = sin.readLong(),
        policy = Policy(sin),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(primaryTerm)
        out.writeLong(seqNo)
        policy.writeTo(out)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val policyParams = ToXContent.MapParams(mapOf(WITH_USER to "false", EXCLUDE_CUSTOM_FIELD_PARAM to "true"))
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_PRIMARY_TERM, primaryTerm)
            .field(_SEQ_NO, seqNo)
            .field(Policy.POLICY_TYPE, policy, policyParams)
            .endObject()
    }
}
