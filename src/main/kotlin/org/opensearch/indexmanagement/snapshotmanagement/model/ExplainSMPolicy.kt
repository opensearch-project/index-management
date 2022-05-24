/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

data class ExplainSMPolicy(
    val policyName: String? = null,
    val metadata: SMMetadata? = null,
    val enabled: Boolean? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyName = sin.readOptionalString(),
        metadata = if (sin.readBoolean()) SMMetadata(sin) else null,
        enabled = sin.readOptionalBoolean()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(policyName)
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
        out.writeOptionalBoolean(enabled)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(SMPolicy.NAME_FIELD, policyName)
            .field(SMMetadata.SM_METADATA_TYPE, metadata)
            .field(SMPolicy.ENABLED_FIELD, enabled)
            .endObject()
    }
}
