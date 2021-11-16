/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import java.io.IOException

class ChangePolicyRequest(
    val indices: List<String>,
    val changePolicy: ChangePolicy
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        changePolicy = ChangePolicy(sin)
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
        changePolicy.writeTo(out)
    }
}
