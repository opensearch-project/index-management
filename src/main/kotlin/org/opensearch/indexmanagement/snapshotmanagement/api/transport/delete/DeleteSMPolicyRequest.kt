/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.delete

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput

class DeleteSMPolicyRequest(
    val policyName: String
) : ActionRequest() {
    override fun validate(): ActionRequestValidationException? {
        return null
    }

    constructor(sin: StreamInput) : this(
        policyName = sin.readString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(policyName)
    }
}
