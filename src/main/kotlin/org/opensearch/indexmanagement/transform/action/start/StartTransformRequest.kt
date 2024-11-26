/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.start

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

class StartTransformRequest : ActionRequest {

    val id: String
        get() = field

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin) {
        this.id = sin.readString()
    }

    constructor(id: String) {
        this.id = id
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (this.id.isEmpty()) {
            validationException = addValidationError("id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(id)
    }
}
