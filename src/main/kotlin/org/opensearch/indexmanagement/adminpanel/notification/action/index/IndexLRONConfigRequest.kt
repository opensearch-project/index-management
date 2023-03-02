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
import java.io.IOException

class IndexLRONConfigRequest : ActionRequest {
    val lronConfig: LRONConfig
    val refreshPolicy: WriteRequest.RefreshPolicy
    val isUpdate: Boolean

    constructor(
        lronConfig: LRONConfig,
        refreshPolicy: WriteRequest.RefreshPolicy,
        isUpdate: Boolean = false
    ) : super() {
        this.lronConfig = lronConfig
        this.refreshPolicy = refreshPolicy
        this.isUpdate = isUpdate
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lronConfig = LRONConfig(sin),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java),
        isUpdate = sin.readBoolean()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        lronConfig.writeTo(out)
        out.writeEnum(refreshPolicy)
        out.writeBoolean(isUpdate)
    }
}
