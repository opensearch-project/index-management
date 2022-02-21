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
import org.opensearch.indexmanagement.snapshotmanagement.model.SM

class IndexSMResponse(val sm: SM) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        sm = SM(sin)
    )

    override fun writeTo(out: StreamOutput) {
        sm.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(sm.policyName)
            .field(SM.CREATE_SCHEDULE_FIELD, sm.createSchedule)
            .field(SM.SNAPSHOT_CONFIG_FIELD, sm.snapshotConfig)
            .endObject()
            .endObject()
    }
}
