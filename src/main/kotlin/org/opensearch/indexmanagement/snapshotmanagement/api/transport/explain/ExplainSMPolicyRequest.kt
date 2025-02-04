/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.explain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class ExplainSMPolicyRequest(
    val policyNames: Array<String>,
) : ActionRequest() {
    override fun validate(): ActionRequestValidationException? = null

    constructor(sin: StreamInput) : this(policyNames = sin.readStringArray())

    override fun writeTo(out: StreamOutput) {
        out.writeStringArray(policyNames)
    }
}
