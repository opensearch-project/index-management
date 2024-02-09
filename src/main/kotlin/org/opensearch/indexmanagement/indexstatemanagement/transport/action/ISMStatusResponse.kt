/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.util.FailedIndex
import org.opensearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.opensearch.indexmanagement.indexstatemanagement.util.buildInvalidIndexResponse
import java.io.IOException

open class ISMStatusResponse : ActionResponse, ToXContentObject {

    val updated: Int
    val failedIndices: List<FailedIndex>

    constructor(
        updated: Int,
        failedIndices: List<FailedIndex>,
    ) : super() {
        this.updated = updated
        this.failedIndices = failedIndices
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        updated = sin.readInt(),
        failedIndices = sin.readList(::FailedIndex),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeInt(updated)
        out.writeCollection(failedIndices)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(UPDATED_INDICES, updated)
        buildInvalidIndexResponse(builder, failedIndices)
        return builder.endObject()
    }
}
