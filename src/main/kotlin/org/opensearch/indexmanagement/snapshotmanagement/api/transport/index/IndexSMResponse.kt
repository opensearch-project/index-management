/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.index

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy

class IndexSMResponse(val policy: SMPolicy) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        policy = SMPolicy(sin)
    )

    override fun writeTo(out: StreamOutput) {
        policy.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(policy.policyName)
            .field(SMPolicy.SNAPSHOT_CONFIG_FIELD, policy.snapshotConfig)
            .endObject()
            .endObject()
    }
}
