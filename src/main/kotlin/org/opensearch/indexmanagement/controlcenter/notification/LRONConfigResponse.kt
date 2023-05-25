/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import org.opensearch.indexmanagement.controlcenter.notification.util.WITH_PRIORITY
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.util._ID
import java.io.IOException

class LRONConfigResponse(
    val id: String,
    val lronConfig: LRONConfig
) : ActionResponse(), ToXContentObject {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        lronConfig = LRONConfig(sin)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        lronConfig.writeTo(out)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)

        /* drop user info in rest layer. only keep user info in transport layer */
        val lronConfigParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false", WITH_PRIORITY to "false"))
        builder.field(LRONConfig.LRON_CONFIG_FIELD, lronConfig, lronConfigParams)

        return builder.endObject()
    }
}
