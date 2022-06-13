/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.snapshotmanagement.api.transport.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import java.io.IOException

class GetSMPoliciesRequest(val searchParams: SearchParams) : ActionRequest() {
    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        searchParams = SearchParams(sin)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        searchParams.writeTo(out)
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }
}
