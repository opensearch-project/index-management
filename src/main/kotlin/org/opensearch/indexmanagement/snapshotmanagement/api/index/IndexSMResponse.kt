/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.index

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy

class IndexSMResponse(val smPolicy: SMPolicy) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        smPolicy = SMPolicy(sin)
    )

    override fun writeTo(out: StreamOutput) {
        smPolicy.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(smPolicy.policyName)
            .field(SMPolicy.SNAPSHOT_CONFIG_FIELD, smPolicy.snapshotConfig)
            .endObject()
            .endObject()
    }
}
