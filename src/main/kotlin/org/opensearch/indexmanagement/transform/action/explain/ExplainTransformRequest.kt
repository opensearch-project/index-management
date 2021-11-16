/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.explain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainTransformRequest(val transformIDs: List<String>) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(transformIDs = sin.readStringArray().toList())

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (transformIDs.isEmpty()) {
            validationException = addValidationError("Missing transformID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(transformIDs.toTypedArray())
    }
}
