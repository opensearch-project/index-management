/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.index

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.controlcenter.notification.model.LRONConfig
import java.io.IOException

class IndexLRONConfigRequest(
    val lronConfig: LRONConfig,
    val isUpdate: Boolean
) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        lronConfig = LRONConfig(sin),
        isUpdate = sin.readBoolean()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        lronConfig.writeTo(out)
        out.writeBoolean(isUpdate)
    }
}
