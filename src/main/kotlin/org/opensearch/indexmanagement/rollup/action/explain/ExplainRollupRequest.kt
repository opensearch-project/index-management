/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.explain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainRollupRequest : ActionRequest {

    val rollupIDs: List<String>

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(rollupIDs = sin.readStringArray().toList())

    constructor(rollupIDs: List<String>) {
        this.rollupIDs = rollupIDs
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (rollupIDs.isEmpty()) {
            validationException = addValidationError("Missing rollupID", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(rollupIDs.toTypedArray())
    }
}
