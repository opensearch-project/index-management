/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.executemanagedindex

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class ExecuteManagedIndexRequest(
    val index: String,
) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        index = sin.readString(),
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (index.isBlank()) {
            validationException = ValidateActions.addValidationError("Missing index", validationException)
        } else if (index.contains(",") || index.contains("*")) {
            validationException = ValidateActions.addValidationError(
                "Only a single index name is supported, wildcards and comma separated lists are not", validationException,
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(index)
    }
}
