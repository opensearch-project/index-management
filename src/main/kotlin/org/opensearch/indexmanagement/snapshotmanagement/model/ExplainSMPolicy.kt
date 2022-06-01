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
import org.opensearch.indexmanagement.opensearchapi.optionalField
import java.io.IOException

data class ExplainSMPolicy(
    val metadata: SMMetadata? = null,
    val enabled: Boolean? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadata = if (sin.readBoolean()) SMMetadata(sin) else null,
        enabled = sin.readOptionalBoolean()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
        out.writeOptionalBoolean(enabled)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        metadata?.let {
            builder.field(SMMetadata.POLICY_SEQ_NO_FIELD, it.policySeqNo)
                .field(SMMetadata.POLICY_PRIMARY_TERM_FIELD, it.policyPrimaryTerm)
                .field(SMMetadata.CURRENT_STATE_FIELD, it.currentState.toString())
                .field(SMMetadata.CREATION_FIELD, it.creation)
                .field(SMMetadata.DELETION_FIELD, it.deletion)
                .optionalField(SMMetadata.INFO_FIELD, it.info)
        }
        return builder.field(SMPolicy.ENABLED_FIELD, enabled)
            .endObject()
    }
}
