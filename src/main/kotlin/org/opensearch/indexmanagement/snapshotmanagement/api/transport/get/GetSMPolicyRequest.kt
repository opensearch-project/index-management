/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class GetSMPolicyRequest(
    val policyID: String,
) : ActionRequest() {
    override fun validate(): ActionRequestValidationException? = null

    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(policyID)
    }
}
