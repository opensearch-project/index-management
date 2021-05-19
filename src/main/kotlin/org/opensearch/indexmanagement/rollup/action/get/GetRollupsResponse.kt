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

import org.opensearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import org.opensearch.indexmanagement.rollup.model.Rollup
import org.opensearch.indexmanagement.rollup.model.Rollup.Companion.ROLLUP_TYPE
import org.opensearch.indexmanagement.util._ID
import org.opensearch.indexmanagement.util._PRIMARY_TERM
import org.opensearch.indexmanagement.util._SEQ_NO
import org.opensearch.action.ActionResponse
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.rest.RestStatus
import java.io.IOException

class GetRollupsResponse : ActionResponse, ToXContentObject {
    val rollups: List<Rollup>
    val totalRollups: Int
    val status: RestStatus

    constructor(
        rollups: List<Rollup>,
        totalRollups: Int,
        status: RestStatus
    ) : super() {
        this.rollups = rollups
        this.totalRollups = totalRollups
        this.status = status
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        rollups = sin.readList(::Rollup),
        totalRollups = sin.readInt(),
        status = sin.readEnum(RestStatus::class.java)
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeCollection(rollups)
        out.writeInt(totalRollups)
        out.writeEnum(status)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field("total_rollups", totalRollups)
            .startArray("rollups")
            .apply {
                for (rollup in rollups) {
                    this.startObject()
                        .field(_ID, rollup.id)
                        .field(_SEQ_NO, rollup.seqNo)
                        .field(_PRIMARY_TERM, rollup.primaryTerm)
                        .field(ROLLUP_TYPE, rollup, XCONTENT_WITHOUT_TYPE)
                        .endObject()
                }
            }
            .endArray()
            .endObject()
    }
}
