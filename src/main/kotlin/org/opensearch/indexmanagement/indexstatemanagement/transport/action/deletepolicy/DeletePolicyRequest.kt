/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.action.support.WriteRequest
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class DeletePolicyRequest(val policyID: String, val refreshPolicy: WriteRequest.RefreshPolicy) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        refreshPolicy = sin.readEnum(WriteRequest.RefreshPolicy::class.java)
    )

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (policyID.isBlank()) {
            validationException = ValidateActions.addValidationError(
                "Missing policy ID",
                validationException
            )
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
        out.writeEnum(refreshPolicy)
    }
}
