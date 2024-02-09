/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.preview

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class PreviewTransformResponse(
    val documents: List<Map<String, Any>>,
    val status: RestStatus,
) : ActionResponse(), ToXContentObject {

    constructor(sin: StreamInput) : this(
        documents = sin.let {
            val documentList = mutableListOf<Map<String, Any>>()
            val size = it.readVInt()
            repeat(size) { _ ->
                documentList.add(sin.readMap()!!)
            }
            documentList.toList()
        },
        status = sin.readEnum(RestStatus::class.java),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeVInt(documents.size)
        for (document in documents) {
            out.writeMap(document)
        }
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("documents", documents)
            .endObject()
    }
}
