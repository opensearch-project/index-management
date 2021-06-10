/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
