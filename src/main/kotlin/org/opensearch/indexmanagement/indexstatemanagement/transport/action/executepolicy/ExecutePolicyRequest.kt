/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executepolicy

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ExecutePolicyRequest(
    val indices: List<String>
) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList()
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (indices.isEmpty()) {
            validationException = ValidateActions.addValidationError("Missing indices", validationException)
        }
        return validationException
    }
    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
    }


}