/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.adminpanel.notification.action.index

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.adminpanel.notification.model.LRONConfig
import org.opensearch.indexmanagement.util.NO_ID
import java.io.IOException

class IndexLRONConfigRequest : ActionRequest {
    val taskID: String
    val lronConfig: LRONConfig
    val seqNo: Long
    val primaryTerm: Long
    val refreshPolicy: WriteRequest.RefreshPolicy

    constructor(
        taskID: String = NO_ID,
        lronConfig: LRONConfig,
        seqNo: Long,
        primaryTerm: Long,
        refreshPolicy: WriteRequest.RefreshPolicy
    ) : super() {
        this.taskID = taskID
        this.lronConfig = lronConfig
        this.seqNo = seqNo
        this.primaryTerm = primaryTerm
        this.refreshPolicy = refreshPolicy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        taskID = sin.readString(),
        lronConfig = LRONConfig(sin),
        seqNo = sin.readLong(),
        primaryTerm = sin.readLong(),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(taskID)
        lronConfig.writeTo(out)
        out.writeLong(seqNo)
        out.writeLong(primaryTerm)
        out.writeEnum(refreshPolicy)
    }
}
