/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions.addValidationError
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import java.io.IOException

class ExplainSMPolicyRequest : ActionRequest {

    val smPolicyNames: List<String>

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(smPolicyNames = sin.readStringArray().toList())

    constructor(smPolicyNames: List<String>) {
        this.smPolicyNames = smPolicyNames
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = null
        if (smPolicyNames.isEmpty()) {
            validationException = addValidationError("Missing sm policy Names", validationException)
        }
        return validationException
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(smPolicyNames.toTypedArray())
    }
}
