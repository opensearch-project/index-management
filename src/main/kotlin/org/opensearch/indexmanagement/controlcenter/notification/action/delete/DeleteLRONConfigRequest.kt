/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.controlcenter.notification.action.delete

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.controlcenter.notification.util.LRON_DOC_ID_PREFIX
import java.io.IOException

class DeleteLRONConfigRequest(
    val docId: String,
) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        docId = sin.readString(),
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (!(docId.startsWith(LRON_DOC_ID_PREFIX))) {
            validationException =
                ValidateActions.addValidationError(
                    "Invalid LRONConfig ID",
                    validationException,
                )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(docId)
    }
}
