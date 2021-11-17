/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.action.get

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.indexmanagement.rollup.model.Rollup
import java.io.IOException

class GetRollupsRequest : ActionRequest {

    val searchString: String
    val from: Int
    val size: Int
    val sortField: String
    val sortDirection: String

    constructor(
        searchString: String = DEFAULT_SEARCH_STRING,
        from: Int = DEFAULT_FROM,
        size: Int = DEFAULT_SIZE,
        sortField: String = DEFAULT_SORT_FIELD,
        sortDirection: String = DEFAULT_SORT_DIRECTION
    ) : super() {
        this.searchString = searchString
        this.from = from
        this.size = size
        this.sortField = sortField
        this.sortDirection = sortDirection
    }

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
        const val DEFAULT_SORT_FIELD = "${Rollup.ROLLUP_TYPE}.${Rollup.ROLLUP_ID_FIELD}.keyword"
        const val DEFAULT_SORT_DIRECTION = "asc"
    }
}
