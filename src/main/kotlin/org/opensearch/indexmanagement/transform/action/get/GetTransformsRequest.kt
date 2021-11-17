/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform.action.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.transform.model.Transform
import java.io.IOException

class GetTransformsRequest(
    val searchString: String = DEFAULT_SEARCH_STRING,
    val from: Int = DEFAULT_FROM,
    val size: Int = DEFAULT_SIZE,
    val sortField: String = DEFAULT_SORT_FIELD,
    val sortDirection: String = DEFAULT_SORT_DIRECTION
) : ActionRequest() {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        searchString = sin.readString(),
        from = sin.readInt(),
        size = sin.readInt(),
        sortField = sin.readString(),
        sortDirection = sin.readString()
    )

    override fun validate(): ActionRequestValidationException? = null

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(searchString)
        out.writeInt(from)
        out.writeInt(size)
        out.writeString(sortField)
        out.writeString(sortDirection)
    }

    companion object {
        const val DEFAULT_SEARCH_STRING = ""
        const val DEFAULT_FROM = 0
        const val DEFAULT_SIZE = 20
        const val DEFAULT_SORT_FIELD = "${Transform.TRANSFORM_TYPE}.${Transform.TRANSFORM_ID_FIELD}.keyword"
        const val DEFAULT_SORT_DIRECTION = "asc"
    }
}
