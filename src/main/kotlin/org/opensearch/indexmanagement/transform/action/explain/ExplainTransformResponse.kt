/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.explain

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.transform.model.ExplainTransform
import java.io.IOException

class ExplainTransformResponse(val idsToExplain: Map<String, ExplainTransform?>) : ActionResponse(), ToXContentObject {

    internal fun getIdsToExplain(): Map<String, ExplainTransform?> {
        return this.idsToExplain
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        idsToExplain = sin.let {
            val idsToExplain = mutableMapOf<String, ExplainTransform?>()
            val size = it.readVInt()
            for (i in 0 until size) {
                idsToExplain[it.readString()] = if (sin.readBoolean()) ExplainTransform(it) else null
            }
            idsToExplain.toMap()
        }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeVInt(idsToExplain.size)
        idsToExplain.entries.forEach { (id, metadata) ->
            out.writeString(id)
            out.writeBoolean(metadata != null)
            metadata?.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        idsToExplain.entries.forEach { (id, explain) ->
            builder.field(id, explain)
        }
        return builder.endObject()
    }
}
