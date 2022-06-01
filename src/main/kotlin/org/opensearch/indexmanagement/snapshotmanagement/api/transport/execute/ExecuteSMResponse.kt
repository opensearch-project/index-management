/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.execute

import org.opensearch.action.ActionResponse
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus

class ExecuteSMResponse(val createSnapshotResponse: CreateSnapshotResponse, val status: RestStatus) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        createSnapshotResponse = CreateSnapshotResponse(sin),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        createSnapshotResponse.writeTo(out)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject().field(SNAPSHOT_RESPONSE_FIELD, createSnapshotResponse).endObject()
    }

    companion object {
        const val SNAPSHOT_RESPONSE_FIELD = "created_snapshot"
    }
}
