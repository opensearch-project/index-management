/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.common.model.rest.SearchParams
import java.io.IOException

class GetLRONConfigsRequest : ActionRequest {

    val searchParams: SearchParams
    val docIds: Array<String>

    constructor(
        searchParams: SearchParams,
        ids: Array<String>
    ) : super() {
        this.searchParams = searchParams
        this.docIds = ids
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        searchParams = SearchParams(sin),
        ids = sin.readStringArray()
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        searchParams.writeTo(out)
        out.writeStringArray(docIds)
    }
}
