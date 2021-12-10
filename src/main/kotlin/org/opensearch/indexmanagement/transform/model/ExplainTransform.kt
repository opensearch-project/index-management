/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import java.io.IOException

data class ExplainTransform(
    val metadataID: String? = null,
    val metadata: TransformMetadata? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadataID = sin.readOptionalString(),
        metadata = if (sin.readBoolean()) TransformMetadata(sin) else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(metadataID)
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(Transform.METADATA_ID_FIELD, metadataID)
            .field(TransformMetadata.TRANSFORM_METADATA_TYPE, metadata, XCONTENT_WITHOUT_TYPE)
            .endObject()
    }
}
