/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.explain

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.rollup.model.ExplainRollup
import java.io.IOException

class ExplainRollupResponse : ActionResponse, ToXContentObject {
    val idsToExplain: Map<String, ExplainRollup?>

    constructor(idsToExplain: Map<String, ExplainRollup?>) : super() {
        this.idsToExplain = idsToExplain
    }

    internal fun getIdsToExplain(): Map<String, ExplainRollup?> {
        return this.idsToExplain
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        idsToExplain = sin.let {
            val idsToExplain = mutableMapOf<String, ExplainRollup?>()
            val size = it.readVInt()
            repeat(size) { _ ->
                idsToExplain[it.readString()] = if (sin.readBoolean()) ExplainRollup(it) else null
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
