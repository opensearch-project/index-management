/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.indexmanagement.opensearchapi.readOptionalValue
import org.opensearch.indexmanagement.snapshotmanagement.model.ExplainSMPolicy
import org.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import java.io.IOException

class ExplainSMPolicyResponse : ActionResponse, ToXContentObject {
    val policiesToExplain: Map<String, ExplainSMPolicy?>

    constructor(policiesToExplain: Map<String, ExplainSMPolicy?>) : super() {
        this.policiesToExplain = policiesToExplain
    }

    internal fun getIdsToExplain(): Map<String, ExplainSMPolicy?> {
        return this.policiesToExplain
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policiesToExplain = sin.let {
            val policiesToExplain = mutableMapOf<String, ExplainSMPolicy?>()
            val size = it.readVInt()
            repeat(size) { _ ->
                policiesToExplain[it.readString()] = sin.readOptionalValue(::ExplainSMPolicy)
            }
            policiesToExplain.toMap()
        }
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeVInt(policiesToExplain.size)
        policiesToExplain.entries.forEach { (name, explain) ->
            out.writeString(name)
            out.writeBoolean(explain != null)
            explain?.writeTo(out)
        }
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startArray(SM_POLICIES_FIELD)
            .also {
                policiesToExplain.entries.forEach { (name, explain) ->
                    it.startObject().apply {
                        this.field(SMPolicy.NAME_FIELD, name)
                        explain?.toXContent(this, params)
                    }.endObject()
                }
            }
            .endArray()
            .endObject()
    }

    companion object {
        const val SM_POLICIES_FIELD = "policies"
    }
}
