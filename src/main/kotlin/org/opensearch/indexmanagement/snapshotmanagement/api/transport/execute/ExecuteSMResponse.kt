/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import org.opensearch.rest.RestStatus

class ExecuteSMResponse(
    val policy: SMPolicy,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        policy = SMPolicy(sin),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        policy.writeTo(out)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(SMPolicy.SM_TYPE, policy, XCONTENT_WITHOUT_TYPE_AND_USER)
            .endObject()
    }
}
