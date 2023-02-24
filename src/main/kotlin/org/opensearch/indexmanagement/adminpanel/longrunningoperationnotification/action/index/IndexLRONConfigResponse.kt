/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.longrunningoperationnotification.action.index

import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.indexmanagement.util._VERSION
import org.opensearch.rest.RestStatus
import java.io.IOException

class IndexLRONConfigResponse : ActionResponse, ToXContentObject {

    val id: String
    val version: Long
    val primaryTerm: Long
    val seqNo: Long
    val status: RestStatus

    constructor(
        id: String,
        version: Long,
        primaryTerm: Long,
        seqNo: Long,
        status: RestStatus
    ) : super() {
        this.id = id
        this.version = version
        this.primaryTerm = primaryTerm
        this.seqNo = seqNo
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        id = sin.readString(),
        version = sin.readLong(),
        primaryTerm = sin.readLong(),
        seqNo = sin.readLong(),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(id)
        out.writeLong(version)
        out.writeLong(primaryTerm)
        out.writeLong(seqNo)
        out.writeEnum(status)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(_ID, id)
            .field(_VERSION, version)
            .field(_PRIMARY_TERM, primaryTerm)
            .field(_SEQ_NO, seqNo)
            .endObject()
    }
}
