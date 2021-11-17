/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.TimeValue
import java.io.IOException

class RetryFailedManagedIndexRequest : ActionRequest {

    val indices: List<String>
    val startState: String?
    val masterTimeout: TimeValue

    constructor(
        indices: List<String>,
        startState: String?,
        masterTimeout: TimeValue
    ) : super() {
        this.indices = indices
        this.startState = startState
        this.masterTimeout = masterTimeout
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        startState = sin.readOptionalString(),
        masterTimeout = sin.readTimeValue()
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
        out.writeOptionalString(startState)
        out.writeTimeValue(masterTimeout)
    }
}
