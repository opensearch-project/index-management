/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.simulate

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.indexstatemanagement.model.Policy
import java.io.IOException

class SimulatePolicyRequest : ActionRequest {
    val indices: List<String>

    // Exactly one of policyId or policy must be provided
    val policyId: String?
    val policy: Policy?

    constructor(
        indices: List<String>,
        policyId: String?,
        policy: Policy?,
    ) : super() {
        this.indices = indices
        this.policyId = policyId
        this.policy = policy
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        policyId = sin.readOptionalString(),
        policy = sin.readOptionalWriteable(::Policy),
    )

    override fun validate(): ActionRequestValidationException? {
        var exception: ActionRequestValidationException? = null
        if (indices.isEmpty()) {
            exception = ValidateActions.addValidationError("indices cannot be empty", exception)
        }
        if (policyId == null && policy == null) {
            exception = ValidateActions.addValidationError("either policy_id or policy must be provided", exception)
        }
        if (policyId != null && policy != null) {
            exception = ValidateActions.addValidationError("only one of policy_id or policy can be provided, not both", exception)
        }
        return exception
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeOptionalString(policyId)
        out.writeOptionalWriteable(policy)
    }
}
