/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.get

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.controlcenter.notification.LRONConfigResponse
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig.Companion.LRON_CONFIG_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import java.io.IOException

class GetLRONConfigsResponse(
    val lronConfigResponses: List<LRONConfigResponse>,
    val totalNumber: Int,
    val timedOut: Boolean
) : ActionResponse(), ToXContentObject {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lronConfigResponses = sin.readList(::LRONConfigResponse),
        totalNumber = sin.readInt(),
        timedOut = sin.readBoolean()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(lronConfigResponses)
        out.writeInt(totalNumber)
        out.writeBoolean(timedOut)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val lronConfigParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false"))

        return builder.startObject()
            .startArray(LRON_CONFIG_FIELD + "s")
            .also { lronConfigResponses.forEach { lronConfigResponse -> lronConfigResponse.toXContent(it, lronConfigParams) } }
            .endArray()
            .field("total_number", totalNumber)
            .field("timed_out", timedOut)
            .endObject()
    }
}
