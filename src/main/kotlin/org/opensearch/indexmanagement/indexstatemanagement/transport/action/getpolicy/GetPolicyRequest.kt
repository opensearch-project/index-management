/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetPolicyRequest : ActionRequest {

    val policyID: String
    val version: Long
    val fetchSrcContext: FetchSourceContext

    constructor(
        policyID: String,
        version: Long,
        fetchSrcContext: FetchSourceContext
    ) : super() {
        this.policyID = policyID
        this.version = version
        this.fetchSrcContext = fetchSrcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        policyID = sin.readString(),
        version = sin.readLong(),
        fetchSrcContext = FetchSourceContext(sin)
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
        out.writeLong(version)
        fetchSrcContext.writeTo(out)
    }
}
