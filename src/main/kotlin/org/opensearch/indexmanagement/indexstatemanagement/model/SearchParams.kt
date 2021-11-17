/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model

import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import java.io.IOException

data class SearchParams(
    val size: Int,
    val from: Int,
    val sortField: String,
    val sortOrder: String,
    val queryString: String
) : Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        size = sin.readInt(),
        from = sin.readInt(),
        sortField = sin.readString(),
        sortOrder = sin.readString(),
        queryString = sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeInt(size)
        out.writeInt(from)
        out.writeString(sortField)
        out.writeString(sortOrder)
        out.writeString(queryString)
    }
}
