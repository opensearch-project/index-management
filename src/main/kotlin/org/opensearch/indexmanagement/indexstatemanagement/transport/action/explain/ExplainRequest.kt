/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.transport.action.explain

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.unit.TimeValue
import org.opensearch.indexmanagement.indexstatemanagement.model.SearchParams
import java.io.IOException

class ExplainRequest : ActionRequest {

    val indices: List<String>
    val local: Boolean
    val masterTimeout: TimeValue
    val searchParams: SearchParams

    constructor(
        indices: List<String>,
        local: Boolean,
        masterTimeout: TimeValue,
        searchParams: SearchParams
    ) : super() {
        this.indices = indices
        this.local = local
        this.masterTimeout = masterTimeout
        this.searchParams = searchParams
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        indices = sin.readStringList(),
        local = sin.readBoolean(),
        masterTimeout = sin.readTimeValue(),
        searchParams = SearchParams(sin)
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeStringCollection(indices)
        out.writeBoolean(local)
        out.writeTimeValue(masterTimeout)
        searchParams.writeTo(out)
    }
}
