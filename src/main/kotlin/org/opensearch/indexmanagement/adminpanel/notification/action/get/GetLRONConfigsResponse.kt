/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.get

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig.Companion.LRON_CONFIG_FIELD
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import java.io.IOException

class GetLRONConfigsResponse(
    val lronConfigs: List<LRONConfig>,
    val totalNumber: Int,
    val timedOut: Boolean = false
) : ActionResponse(), ToXContentObject {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lronConfigs = sin.readList(::LRONConfig), totalNumber = sin.readInt(), timedOut = sin.readBoolean()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeList(lronConfigs)
        out.writeInt(totalNumber)
        out.writeBoolean(timedOut)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        val lronConfigParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false"))

        return builder.startObject().startArray(LRON_CONFIG_FIELD + "s")
            .also { lronConfigs.forEach { lronConfig -> lronConfig.toXContent(it, lronConfigParams) } }.endArray()
            .field("total_number", totalNumber).field("timed_out", timedOut).endObject()
    }
}
