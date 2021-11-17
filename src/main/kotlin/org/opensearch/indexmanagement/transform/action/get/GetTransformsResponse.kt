/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.get

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE_AND_USER
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.Transform.Companion.TRANSFORM_TYPE
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.rest.RestStatus
import java.io.IOException

class GetTransformsResponse(
    val transforms: List<Transform>,
    val totalTransforms: Int,
    val status: RestStatus
) : ActionResponse(), ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        transforms = sin.readList(::Transform),
        totalTransforms = sin.readInt(),
        status = sin.readEnum(RestStatus::class.java)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeCollection(transforms)
        out.writeInt(totalTransforms)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("total_transforms", totalTransforms)
            .startArray("transforms")
            .apply {
                for (transform in transforms) {
                    this.startObject()
                        .field(_ID, transform.id)
                        .field(_SEQ_NO, transform.seqNo)
                        .field(_PRIMARY_TERM, transform.primaryTerm)
                        .field(TRANSFORM_TYPE, transform, XCONTENT_WITHOUT_TYPE_AND_USER)
                        .endObject()
                }
            }
            .endArray()
            .endObject()
    }
}
