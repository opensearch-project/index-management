/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.get

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.model.LRONConfig
import org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.util.LRON_CONFIG_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import org.opensearch.indexmanagement.indexstatemanagement.util.WITH_USER
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.indexmanagement.util._VERSION
import java.io.IOException

class GetLRONConfigResponse : ActionResponse, ToXContentObject {
    val id: String
    val version: Long
    val seqNo: Long
    val primaryTerm: Long
    val configType: String
    val lronConfig: LRONConfig?

    constructor(
        id: String,
        version: Long,
        primaryTerm: Long,
        seqNo: Long,
        configType: String,
        lronConfig: LRONConfig?
    ) : super() {
        this.id = id
        this.version = version
        this.primaryTerm = primaryTerm
        this.seqNo = seqNo
        this.configType = configType
        this.lronConfig = lronConfig
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        primaryTerm = sin.readLong(),
        seqNo = sin.readLong(),
        configType = sin.readString(),
        lronConfig = sin.readOptionalWriteable(::LRONConfig)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(primaryTerm)
        out.writeLong(seqNo)
        out.writeString(configType)
        out.writeOptionalWriteable(lronConfig)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_PRIMARY_TERM, primaryTerm)
            .field(_SEQ_NO, seqNo)
            .field(LRON_CONFIG_TYPE, configType)

        /* drop user info in rest layer. only keep user info in transport layer */
        if (lronConfig != null) {
            val lronConfigParams = ToXContent.MapParams(mapOf(WITH_TYPE to "false", WITH_USER to "false"))
            builder.field(LRONConfig.LRON_CONFIG_FIELD, lronConfig, lronConfigParams)
        }
        return builder.endObject()
    }
}
