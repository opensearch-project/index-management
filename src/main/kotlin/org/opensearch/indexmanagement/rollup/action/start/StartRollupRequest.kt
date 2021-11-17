/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.start

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.action.update.UpdateRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class StartRollupRequest : UpdateRequest {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : super(sin)

    constructor(id: String) {
        super.id(id)
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (super.id().isEmpty()) {
            validationException = addValidationError("id is missing", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
    }
}
